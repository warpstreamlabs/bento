package io

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

// TestFSEventReconnectsOnWatcherError is a regression test for the nightly
// "watcher stops picking up new files" stall observed on Windows when the
// watched paths are symlinks to SMB shares.
//
// When the SMB session is torn down (typically overnight) the underlying
// fsnotify watch reports an error such as:
//
//	GetQueuedCompletionPort: The specified network name is no longer available.
//
// Previously this error was only logged and the read loop kept running against
// a dead watch, so Read blocked forever and no new files were picked up until
// the process was manually restarted. The input must instead surface a
// disconnect (ErrNotConnected) so the framework rebuilds the watcher.
func TestFSEventReconnectsOnWatcherError(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	conf, err := fsEventInputSpec().ParseYAML(fmt.Sprintf("paths: [ %q ]\n", dir), nil)
	require.NoError(t, err)

	f, err := fsEventWatcherFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	require.NoError(t, f.Connect(ctx))

	// Simulate fsnotify surfacing a fatal watcher error (the SMB share going
	// away). Before the fix this was swallowed with a log line.
	select {
	case f.watcher.Errors <- errors.New("simulated: the specified network name is no longer available"):
	case <-time.After(5 * time.Second):
		t.Fatal("could not inject watcher error")
	}

	// Read must return ErrNotConnected so the framework reconnects, rather than
	// blocking forever on a dead watch.
	readErrCh := make(chan error, 1)
	go func() {
		_, _, rerr := f.Read(ctx)
		readErrCh <- rerr
	}()
	select {
	case rerr := <-readErrCh:
		require.ErrorIs(t, rerr, service.ErrNotConnected)
	case <-time.After(5 * time.Second):
		t.Fatal("Read blocked after watcher error instead of returning ErrNotConnected")
	}

	// Reconnecting must rebuild the watch and resume picking up new files.
	require.NoError(t, f.Connect(ctx))
	time.Sleep(500 * time.Millisecond)

	newFile := filepath.Join(dir, "after-reconnect.txt")
	require.NoError(t, os.WriteFile(newFile, []byte("hi"), 0o644))

	var gotPath string
	for i := 0; i < 10 && gotPath != newFile; i++ {
		readCtx, readCancel := context.WithTimeout(ctx, 3*time.Second)
		msg, _, rerr := f.Read(readCtx)
		readCancel()
		if rerr != nil {
			require.ErrorIs(t, rerr, context.DeadlineExceeded)
			continue
		}
		if p, ok := msg.MetaGet("fsevent_path"); ok {
			gotPath = p
		}
	}
	require.Equal(t, newFile, gotPath, "watcher did not pick up a new file after reconnect")

	require.NoError(t, f.Close(ctx))
}
