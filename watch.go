package collector

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/fsnotify/fsnotify"
)

var supportedStreams = [...]Stream{Fills, MiscEvents, TWAPStatuses}

func (collector *Collector) watch(ctx context.Context, root *os.Root, progress *checkpoint, handle Handler) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	dir := collector.config.DataDir
	if err := watcher.Add(dir); err != nil {
		return err
	}
	watch := func(path string) error {
		err := watcher.Add(filepath.Join(dir, path))
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	all := map[Stream]bool{Fills: true, MiscEvents: true, TWAPStatuses: true}
	if err := collector.reconcile(ctx, root, progress, handle, all, watch); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err, ok := <-watcher.Errors:
			if !ok {
				return errors.New("collector: filesystem watcher closed")
			}
			if !errors.Is(err, fsnotify.ErrEventOverflow) {
				return fmt.Errorf("collector: filesystem watcher: %w", err)
			}
			if err := collector.reconcile(ctx, root, progress, handle, all, watch); err != nil {
				return err
			}
		case event, ok := <-watcher.Events:
			if !ok {
				return errors.New("collector: filesystem watcher closed")
			}
			dirty := make(map[Stream]bool)
			if err := markDirty(dir, event, dirty); err != nil {
				return err
			}
			if len(dirty) > 0 {
				if err := collector.reconcile(ctx, root, progress, handle, dirty, watch); err != nil {
					return err
				}
			}
		}
	}
}

func markDirty(dir string, event fsnotify.Event, dirty map[Stream]bool) error {
	relevant := fsnotify.Create | fsnotify.Write | fsnotify.Rename | fsnotify.Remove
	if event.Op&relevant == 0 {
		return nil
	}
	if event.Name == dir && (event.Has(fsnotify.Remove) || event.Has(fsnotify.Rename)) {
		return fmt.Errorf("collector: watched data directory removed or renamed: %s", dir)
	}
	relative, err := filepath.Rel(dir, event.Name)
	if err != nil {
		return err
	}
	stream, _, _ := strings.Cut(relative, string(os.PathSeparator))
	switch Stream(stream) {
	case Fills, MiscEvents, TWAPStatuses:
		dirty[Stream(stream)] = true
	}
	return nil
}
