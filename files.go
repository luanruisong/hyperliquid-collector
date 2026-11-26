package collector

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"
)

type hourFile struct {
	path string
	hour time.Time
	info os.FileInfo
}

func hourlyFiles(root *os.Root, stream Stream, watch func(string) error) ([]hourFile, error) {
	hourly := filepath.Join(string(stream), "hourly")
	for _, dir := range []string{string(stream), hourly} {
		info, err := root.Lstat(dir)
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("collector: %s is not a real directory", dir)
		}
		if err := watch(dir); err != nil {
			return nil, err
		}
	}
	days, err := fs.ReadDir(root.FS(), hourly)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var files []hourFile
	for _, day := range days {
		date, err := time.Parse("20060102", day.Name())
		if err != nil || !day.IsDir() {
			continue
		}
		dir := filepath.Join(hourly, day.Name())
		if err := watch(dir); err != nil {
			return nil, err
		}
		hours, err := fs.ReadDir(root.FS(), dir)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, hour := range hours {
			number, err := strconv.Atoi(hour.Name())
			if err != nil || number < 0 || number > 23 ||
				(hour.Name() != strconv.Itoa(number) && hour.Name() != fmt.Sprintf("%02d", number)) ||
				!hour.Type().IsRegular() {
				continue
			}
			files = append(files, hourFile{
				path: filepath.Join(dir, hour.Name()),
				hour: date.Add(time.Duration(number) * time.Hour),
			})
		}
	}
	slices.SortFunc(files, func(left, right hourFile) int {
		if order := left.hour.Compare(right.hour); order != 0 {
			return order
		}
		if left.path < right.path {
			return -1
		}
		if left.path > right.path {
			return 1
		}
		return 0
	})
	return files, nil
}

type checkpoint struct {
	DataDir string           `json:"data_dir"`
	Offsets map[string]int64 `json:"offsets"`
}

func loadCheckpoint(path, dataDir string) (*checkpoint, error) {
	progress := &checkpoint{dataDir, make(map[string]int64)}
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return progress, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, progress); err != nil {
		return nil, fmt.Errorf("collector: decode checkpoint: %w", err)
	}
	if progress.DataDir != dataDir || progress.Offsets == nil {
		return nil, errors.New("collector: checkpoint does not match data directory")
	}
	for _, offset := range progress.Offsets {
		if offset < 0 {
			return nil, errors.New("collector: invalid checkpoint offset")
		}
	}
	return progress, nil
}

func (progress *checkpoint) save(path string) error {
	dir := filepath.Dir(path)
	temp, err := os.CreateTemp(dir, ".hyperliquid-checkpoint-*")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	writeErr := json.NewEncoder(temp).Encode(progress)
	if writeErr == nil {
		writeErr = temp.Sync()
	}
	if err := errors.Join(writeErr, temp.Close()); err != nil {
		return err
	}
	if err := os.Rename(temp.Name(), path); err != nil {
		return err
	}
	parent, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer parent.Close()
	return parent.Sync()
}

func (collector *Collector) clean(root *os.Root, progress *checkpoint, files []hourFile) error {
	now := time.Now().UTC()
	cutoff := now.Add(-collector.config.Retention)
	changed := false
	var cleanupErr error
	for _, file := range files {
		if file.hour.Add(time.Hour).After(cutoff) || file.info.ModTime().After(cutoff) {
			continue
		}
		info, err := root.Lstat(file.path)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			cleanupErr = err
			break
		}
		if !info.Mode().IsRegular() || !os.SameFile(file.info, info) ||
			info.Size() != file.info.Size() || !info.ModTime().Equal(file.info.ModTime()) {
			continue
		}
		if err := root.Remove(file.path); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			cleanupErr = err
			break
		}
		delete(progress.Offsets, file.path)
		changed = true
	}
	if changed {
		return errors.Join(cleanupErr, progress.save(collector.config.CheckpointPath))
	}
	return cleanupErr
}
