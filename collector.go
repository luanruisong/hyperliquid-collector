package collector

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"time"
)

type Stream string

const (
	Fills        Stream = "node_fills_streaming"
	MiscEvents   Stream = "misc_events_streaming"
	TWAPStatuses Stream = "node_twap_statuses_streaming"
)

// Config 定义采集器的数据来源、读取进度和文件保留策略。
type Config struct {
	// DataDir 是 Hyperliquid 节点的数据根目录，必填且必须已存在。
	// 采集器在其下查找三个 *_streaming 数据流目录。
	DataDir string

	// CheckpointPath 是保存读取进度的文件路径，其父目录必须已存在且可写。
	// 留空时使用 DataDir 下的 .hyperliquid-collector.json。
	CheckpointPath string

	// MaxRecordBytes 是单条 JSONL 记录的字节上限，包含行末换行。零值为 8 MiB。
	MaxRecordBytes int

	// Retention 是源文件的保留时长。零值禁用清理。
	// 仅删除已完整采集、且所属小时结束时间和文件修改时间均超过保留时长的文件。
	// 每个数据流最新小时的文件始终保留。
	Retention time.Duration
}

type Record struct {
	Stream      Stream
	Path        string
	Offset      int64
	BlockNumber uint64
	BlockTime   time.Time
	LocalTime   time.Time
	Events      []json.RawMessage
}

type Handler func(context.Context, Record) error

type Collector struct {
	config Config
}

func New(config Config) (*Collector, error) {
	if config.DataDir == "" {
		return nil, errors.New("collector: DataDir is required")
	}
	dir, err := filepath.Abs(config.DataDir)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("collector: %s is not a directory", dir)
	}
	config.DataDir = dir
	if config.CheckpointPath == "" {
		config.CheckpointPath = filepath.Join(dir, ".hyperliquid-collector.json")
	}
	config.CheckpointPath, err = filepath.Abs(config.CheckpointPath)
	if err != nil {
		return nil, err
	}
	if config.Retention < 0 || config.MaxRecordBytes < 0 {
		return nil, errors.New("collector: retention and record size cannot be negative")
	}
	if config.MaxRecordBytes == 0 {
		config.MaxRecordBytes = 8 << 20
	}
	return &Collector{config: config}, nil
}

func (collector *Collector) Run(ctx context.Context, handle Handler) error {
	if handle == nil {
		return errors.New("collector: handler is required")
	}
	config := collector.config
	lock, err := os.OpenFile(config.CheckpointPath+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("collector: lock checkpoint: %w", err)
	}
	root, err := os.OpenRoot(config.DataDir)
	if err != nil {
		return err
	}
	defer root.Close()
	progress, err := loadCheckpoint(config.CheckpointPath, config.DataDir)
	if err != nil {
		return err
	}

	return collector.watch(ctx, root, progress, handle)
}

func (collector *Collector) reconcile(ctx context.Context, root *os.Root, progress *checkpoint, handle Handler, streams map[Stream]bool, watch func(string) error) error {
	var collectionErr error
	var candidates []hourFile
	changed := false
	for _, stream := range supportedStreams {
		if !streams[stream] {
			continue
		}
		files, err := hourlyFiles(root, stream, watch)
		if err != nil {
			collectionErr = err
			break
		}
		for index, file := range files {
			if err := watch(file.path); err != nil {
				collectionErr = err
				break
			}
			offset := progress.Offsets[file.path]
			next, info, err := collector.read(ctx, root, stream, file.path, offset, handle)
			if next != offset {
				progress.Offsets[file.path] = next
				changed = true
			}
			if err != nil {
				collectionErr = err
				break
			}
			if info != nil && next == info.Size() && index < len(files)-1 && file.hour.Before(files[len(files)-1].hour) {
				file.info = info
				candidates = append(candidates, file)
			}
		}
		if collectionErr != nil {
			break
		}
	}
	if changed {
		if err := progress.save(collector.config.CheckpointPath); err != nil {
			return errors.Join(collectionErr, err)
		}
	}
	if collectionErr != nil {
		return collectionErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if collector.config.Retention > 0 {
		return collector.clean(root, progress, candidates)
	}
	return nil
}

func (collector *Collector) read(ctx context.Context, root *os.Root, stream Stream, path string, offset int64, handle Handler) (int64, os.FileInfo, error) {
	file, err := root.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return offset, nil, nil
	}
	if err != nil {
		return offset, nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return offset, nil, err
	}
	if !info.Mode().IsRegular() || info.Size() < offset {
		return offset, nil, fmt.Errorf("collector: %s is not a regular append-only file at byte %d", path, offset)
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return offset, nil, err
	}
	if info.Size() == offset {
		return offset, info, nil
	}

	scanner := bufio.NewScanner(io.LimitReader(file, info.Size()-offset))
	scanner.Buffer(make([]byte, 0, min(64<<10, collector.config.MaxRecordBytes)), collector.config.MaxRecordBytes)
	scanner.Split(func(data []byte, _ bool) (int, []byte, error) {
		if index := slices.Index(data, '\n'); index >= 0 {
			return index + 1, data[:index], nil
		}
		return 0, nil, nil
	})
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return offset, nil, err
		}
		record, err := decode(stream, path, offset, scanner.Bytes())
		if err == nil {
			err = handle(ctx, record)
		}
		if err != nil {
			return offset, nil, fmt.Errorf("collector: %s at byte %d: %w", path, offset, err)
		}
		offset += int64(len(scanner.Bytes())) + 1
	}
	if err := scanner.Err(); err != nil {
		return offset, nil, fmt.Errorf("collector: %s at byte %d: %w", path, offset, err)
	}
	return offset, info, nil
}

func decode(stream Stream, path string, offset int64, line []byte) (Record, error) {
	var payload struct {
		BlockNumber uint64            `json:"block_number"`
		BlockTime   string            `json:"block_time"`
		LocalTime   string            `json:"local_time"`
		Events      []json.RawMessage `json:"events"`
	}
	if err := json.Unmarshal(line, &payload); err != nil {
		return Record{}, err
	}
	if payload.BlockNumber == 0 || payload.Events == nil {
		return Record{}, errors.New("missing block_number or events")
	}
	blockTime, err := parseTime(payload.BlockTime)
	if err != nil {
		return Record{}, fmt.Errorf("block_time: %w", err)
	}
	localTime, err := parseTime(payload.LocalTime)
	if err != nil {
		return Record{}, fmt.Errorf("local_time: %w", err)
	}
	return Record{stream, path, offset, payload.BlockNumber, blockTime, localTime, payload.Events}, nil
}

func parseTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err == nil {
		return parsed.UTC(), nil
	}
	return time.Parse("2006-01-02T15:04:05", value)
}
