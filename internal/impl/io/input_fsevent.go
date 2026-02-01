package io

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	fsEventInputFieldPaths           = "paths"
	fsEventInputFieldIsRecursive     = "is_recursive"
	fsEventInputFieldWatchNewSubdirs = "watch_new_subdirs"
)

func fsEventInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Local").
		Summary(`Detects filesystem events. Emits empty messages with metadata describing the event`).
		Description(`
### Metadata

This input adds the following metadata fields to each message:

`+"```text"+`
- fsevent_path
- fsevent_operation
- fsevent_mod_time_unix
- fsevent_mod_time (RFC3339)
`+"```"+`

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#bloblang-queries).`+`

|Operation|Cause|
|---------|-----|
|CREATE   |A new pathname was created.|
|WRITE    |The pathname was written to; this does *not* mean the write has finished, and a write can be followed by more writes.|
|REMOVE   |The path was removed; any watches on it will be removed. Some "remove" operations may trigger a RENAME if the file is actually moved (for example "remove to trash" is often a rename).|
|RENAME   |The path was renamed to something else. Any watches on it will be removed.|
|CHMOD    |File attributes were changed. It's generally not recommended to take action on this event, as it may get triggered very frequently by some software. For example, Spotlight indexing on macOS, anti-virus software, backup software, etc.|
`).
		Fields(
			service.NewStringListField(fsEventInputFieldPaths).
				Description("A list of paths to monitor for file changes."),
			service.NewBoolField(fsEventInputFieldIsRecursive).
				Description("If set, subdirs of configured paths will be watched too.").
				Default(false),
			service.NewBoolField(fsEventInputFieldWatchNewSubdirs).
				Description("If set, events from subdirs created after the input is started, will also be watched.").
				Default(false),
		)
}

func init() {
	err := service.RegisterInput("fsevent", fsEventInputSpec(),
		func(pConf *service.ParsedConfig, res *service.Resources) (service.Input, error) {
			return fsEventWatcherFromParsed(pConf, res)
		})
	if err != nil {
		panic(err)
	}
}

type fsEventWatcher struct {
	log             *service.Logger
	nm              *service.Resources
	watcher         *fsnotify.Watcher
	eventChan       chan watcherEventMsg
	cMut            sync.RWMutex
	paths           []string
	recursive       bool
	watchNewSubdirs bool
}

type watcherEventMsg struct {
	event         fsnotify.Event
	timestampUnix int64
}

func fsEventWatcherFromParsed(conf *service.ParsedConfig, nm *service.Resources) (*fsEventWatcher, error) {
	paths, err := conf.FieldStringList(fsEventInputFieldPaths)
	if err != nil {
		return nil, err
	}

	wn, err := conf.FieldBool(fsEventInputFieldWatchNewSubdirs)
	if err != nil {
		return nil, err
	}

	recursive, err := conf.FieldBool(fsEventInputFieldIsRecursive)
	if err != nil {
		return nil, err
	}

	return &fsEventWatcher{
		nm:              nm,
		log:             nm.Logger(),
		paths:           paths,
		recursive:       recursive,
		watchNewSubdirs: wn,
	}, nil
}

func (f *fsEventWatcher) Connect(ctx context.Context) error {
	f.cMut.Lock()
	defer f.cMut.Unlock()

	if f.watcher != nil {
		return nil
	}

	eventChan := make(chan watcherEventMsg)

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		f.log.Errorf("Failed to instantiate new filesystem watcher: %s", err)
		return err
	}

	for _, path := range f.paths {
		err = watcher.Add(path)
		if err != nil {
			f.log.Errorf("Failed to add path %v: %s", path, err)
			return err
		}
		if f.recursive {
			_ = filepath.WalkDir(path, func(path string, d fs.DirEntry, err error) error {
				if d.IsDir() {
					_ = watcher.Add(path)
				}
				return nil
			})
		}
	}

	go func() {
		defer close(eventChan)
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}

				// a create could be a new subdir. if enabled we will check
				if f.watchNewSubdirs && event.Has(fsnotify.Create) {
					st, err := os.Stat(event.Name)
					if err != nil {
						f.log.Warnf("Cannot check for new subpath: %s", err)
						continue
					}

					// if it is a file dont add it.
					if !st.IsDir() {
						continue
					}

					// adding the same path more than once is a noop,
					// so safe even though it is already in the watchlist
					if err := f.watcher.Add(event.Name); err != nil {
						f.log.Warnf("Failed to add path %v: %s", event.Name, err)
					}
				}

				msg := watcherEventMsg{
					event:         event,
					timestampUnix: time.Now().Unix(),
				}

				f.eventChan <- msg
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				f.log.Errorf("error:", err)
			}
		}
	}()

	f.watcher = watcher
	f.eventChan = eventChan
	return nil
}

func (f *fsEventWatcher) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	f.cMut.RLock()
	defer f.cMut.RUnlock()
	eventChan := f.eventChan

	if eventChan == nil {
		return nil, nil, service.ErrNotConnected
	}

	select {
	case msg, open := <-eventChan:
		if !open {
			f.eventChan = nil
			f.watcher = nil
			return nil, nil, service.ErrEndOfInput
		}

		message := service.NewMessage(nil)
		message.MetaSetMut("fsevent_operation", msg.event.Op.String())
		message.MetaSetMut("fsevent_path", msg.event.Name)
		message.MetaSetMut("fsevent_mod_time_unix", msg.timestampUnix)
		timestamp := time.Unix(msg.timestampUnix, 0).Format(time.RFC3339)
		message.MetaSetMut("fsevent_mod_time", timestamp)

		// check to see if the watchlist is empty.
		if f.watcher != nil && len(f.watcher.WatchList()) == 0 {
			f.log.Warn("Nothing being watched. Closing the input.")
			go f.Close(context.TODO())
		}

		return message, func(ctx context.Context, res error) error {
			return nil
		}, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (f *fsEventWatcher) Close(ctx context.Context) error {
	f.cMut.Lock()
	defer f.cMut.Unlock()

	var err error
	if f.watcher != nil {
		err = f.watcher.Close()
		f.watcher = nil
	}
	return err
}
