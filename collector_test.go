package collector_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/luanruisong/hyperliquid-collector"
)

func TestCollectsAllStreams(t *testing.T) {
	dir := t.TempDir()
	streams := map[collector.Stream]string{
		collector.Fills:        `["0x123",{"coin":"BTC","px":"123.45"}]`,
		collector.MiscEvents:   `{"inner":{"CDeposit":{"amount":"10"}}}`,
		collector.TWAPStatuses: `{"twap_id":7,"status":"finished"}`,
	}
	for stream, event := range streams {
		writeHour(t, dir, string(stream), "20261125", "9", line(123, event))
	}
	sdk := newSDK(t, collector.Config{DataDir: dir})
	ctx, cancel := runContext(t)
	received := make(map[collector.Stream]string)
	err := sdk.Run(ctx, func(_ context.Context, record collector.Record) error {
		wantPath := filepath.Join(string(record.Stream), "hourly", "20261125", "9")
		wantTime := time.Date(2026, 11, 25, 12, 0, 0, 123456789, time.UTC)
		if record.BlockNumber != 123 || record.Path != wantPath || record.Offset != 0 ||
			record.BlockTime != wantTime || record.LocalTime.Sub(record.BlockTime) != time.Second ||
			len(record.Events) != 1 {
			t.Fatalf("unexpected record: %+v", record)
		}
		received[record.Stream] = string(record.Events[0])
		if len(received) == len(streams) {
			cancel()
		}
		return nil
	})
	requireError(t, err, context.Canceled)
	if !reflect.DeepEqual(received, streams) {
		t.Fatalf("events = %v, want %v", received, streams)
	}
}

func TestResumesAtRejectedRecord(t *testing.T) {
	dir := t.TempDir()
	first, second := line(100, "{}"), line(100, "[]")
	writeHour(t, dir, string(collector.Fills), "20261125", "9", first+second)
	config := collector.Config{DataDir: dir, CheckpointPath: filepath.Join(t.TempDir(), "progress.json")}
	ctx, _ := runContext(t)
	rejected := errors.New("sink rejected record")
	var offsets []int64
	err := newSDK(t, config).Run(ctx, func(_ context.Context, record collector.Record) error {
		offsets = append(offsets, record.Offset)
		if len(offsets) == 2 {
			return rejected
		}
		return nil
	})
	requireError(t, err, rejected)
	if want := []int64{0, int64(len(first))}; !reflect.DeepEqual(offsets, want) {
		t.Fatalf("offsets = %v, want %v", offsets, want)
	}

	ctx, cancel := runContext(t)
	err = newSDK(t, config).Run(ctx, func(_ context.Context, record collector.Record) error {
		if record.Offset != int64(len(first)) || string(record.Events[0]) != "[]" {
			t.Fatalf("resumed at wrong record: %+v", record)
		}
		cancel()
		return nil
	})
	requireError(t, err, context.Canceled)
}

func TestReadsCompletedLineAndNewHour(t *testing.T) {
	dir := t.TempDir()
	stream := string(collector.Fills)
	first := line(100, "{}")
	old := writeHour(t, dir, stream, "20261125", "9", first+strings.TrimSuffix(line(101, "{}"), "\n"))
	ctx, cancel := runContext(t)
	var blocks []uint64
	err := newSDK(t, collector.Config{DataDir: dir}).Run(ctx, func(_ context.Context, record collector.Record) error {
		blocks = append(blocks, record.BlockNumber)
		switch record.BlockNumber {
		case 100:
			writeHour(t, dir, stream, "20261125", "10", line(102, "{}"))
		case 102:
			file, err := os.OpenFile(old, os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				t.Fatal(err)
			}
			_, err = file.WriteString("\n")
			if err := errors.Join(err, file.Close()); err != nil {
				t.Fatal(err)
			}
		case 101:
			if record.Offset != int64(len(first)) {
				t.Fatalf("partial record offset = %d", record.Offset)
			}
			cancel()
		}
		return nil
	})
	requireError(t, err, context.Canceled)
	if want := []uint64{100, 102, 101}; !reflect.DeepEqual(blocks, want) {
		t.Fatalf("blocks = %v, want %v", blocks, want)
	}
}

func TestWatchesNewStreamAndDateDirectories(t *testing.T) {
	dir := t.TempDir()
	writeHour(t, dir, string(collector.Fills), "20261125", "23", line(100, "{}"))
	ctx, cancel := runContext(t)
	var received []string
	err := newSDK(t, collector.Config{DataDir: dir}).Run(ctx, func(_ context.Context, record collector.Record) error {
		received = append(received, record.Path)
		switch record.BlockNumber {
		case 100:
			writeHour(t, dir, string(collector.MiscEvents), "20261125", "23", line(101, "{}"))
		case 101:
			writeHour(t, dir, string(collector.MiscEvents), "20261126", "0", line(102, "{}"))
		case 102:
			cancel()
		}
		return nil
	})
	requireError(t, err, context.Canceled)
	want := []string{
		"node_fills_streaming/hourly/20261125/23",
		"misc_events_streaming/hourly/20261125/23",
		"misc_events_streaming/hourly/20261126/0",
	}
	if !reflect.DeepEqual(received, want) {
		t.Fatalf("paths = %v, want %v", received, want)
	}
}

func TestRetention(t *testing.T) {
	for _, test := range []struct {
		name      string
		retention time.Duration
	}{
		{name: "disabled by default"},
		{name: "enabled", retention: 24 * time.Hour},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			stream := string(collector.Fills)
			old := writeHour(t, dir, stream, "20200101", "8", line(100, "{}"))
			partial := writeHour(t, dir, stream, "20200101", "9", strings.TrimSuffix(line(101, "{}"), "\n"))
			latest := writeHour(t, dir, stream, "20200101", "11", line(103, "{}"))
			recentWrite := writeHour(t, dir, stream, "20200101", "10", line(102, "{}"))
			unrelated := writeHour(t, dir, "replica_cmds", "20200101", "8", line(200, "{}"))
			for _, path := range []string{old, partial, latest, unrelated} {
				expired := time.Now().Add(-72 * time.Hour)
				if err := os.Chtimes(path, expired, expired); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := runContext(t)
			err := newSDK(t, collector.Config{DataDir: dir, Retention: test.retention}).Run(ctx, func(_ context.Context, record collector.Record) error {
				switch record.BlockNumber {
				case 103:
					writeHour(t, dir, stream, "20200101", "12", line(104, "{}"))
				case 104:
					cancel()
				}
				return nil
			})
			requireError(t, err, context.Canceled)
			_, err = os.Stat(old)
			if test.retention > 0 {
				requireError(t, err, os.ErrNotExist)
			} else if err != nil {
				t.Fatalf("disabled retention deleted a file: %v", err)
			}
			for _, path := range []string{partial, latest, recentWrite, unrelated} {
				if _, err := os.Stat(path); err != nil {
					t.Errorf("protected file %s: %v", path, err)
				}
			}
		})
	}
}

func TestMalformedRecordIsNotSkipped(t *testing.T) {
	dir := t.TempDir()
	first := line(100, "{}")
	writeHour(t, dir, string(collector.Fills), "20261125", "9", first+"not json\n"+line(101, "{}"))
	ctx, _ := runContext(t)
	var blocks []uint64
	err := newSDK(t, collector.Config{DataDir: dir}).Run(ctx, func(_ context.Context, record collector.Record) error {
		blocks = append(blocks, record.BlockNumber)
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("at byte %d", len(first))) {
		t.Fatalf("error = %v, want malformed record offset", err)
	}
	if !reflect.DeepEqual(blocks, []uint64{100}) {
		t.Fatalf("blocks = %v, want only the record before the malformed line", blocks)
	}
}

func newSDK(t *testing.T, config collector.Config) *collector.Collector {
	t.Helper()
	sdk, err := collector.New(config)
	if err != nil {
		t.Fatal(err)
	}
	return sdk
}

func runContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return ctx, cancel
}

func writeHour(t *testing.T, dir, stream, day, hour, content string) string {
	t.Helper()
	path := filepath.Join(dir, stream, "hourly", day, hour)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func line(block uint64, event string) string {
	return fmt.Sprintf(`{"block_number":%d,"block_time":"2026-11-25T12:00:00.123456789","local_time":"2026-11-25T12:00:01.123456789Z","events":[%s]}`+"\n", block, event)
}

func requireError(t *testing.T, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}
