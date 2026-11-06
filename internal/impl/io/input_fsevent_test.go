package io_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component/input"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager/mock"

	_ "github.com/warpstreamlabs/bento/internal/impl/io"
)

// fseventInput creates the input and waits until it has connected. For fsevent
// a connected input has registered its paths with fsnotify, so any change made
// afterwards is delivered.
func fseventInput(t testing.TB, confPattern string, args ...any) input.Streamed {
	t.Helper()

	iConf, err := testutil.InputFromYAML(fmt.Sprintf(confPattern, args...))
	require.NoError(t, err)

	i, err := mock.NewManager().NewInput(iConf)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return i.ConnectionStatus().AllActive()
	}, time.Second*10, time.Millisecond*10, "input did not connect")

	return i
}

type fsEvent struct {
	path      string
	operation string
}

func (e fsEvent) is(path string, operations ...string) bool {
	return e.path == path && slices.Contains(operations, e.operation)
}

// nextEvent acks and returns the next event delivered by the input, failing
// the test once ctx expires.
func nextEvent(t *testing.T, ctx context.Context, i input.Streamed) fsEvent {
	t.Helper()

	select {
	case tran := <-i.TransactionChan():
		require.NoError(t, tran.Ack(ctx, nil))
		require.Equal(t, 1, tran.Payload.Len())

		part := tran.Payload.Get(0)
		e := fsEvent{
			path:      part.MetaGetStr("fsevent_path"),
			operation: part.MetaGetStr("fsevent_operation"),
		}
		return e
	case <-ctx.Done():
		require.FailNow(t, "timed out waiting for filesystem event")
		return fsEvent{}
	}
}

// awaitEvent reads events until one satisfies match, returning every event
// seen up to and including it.
func awaitEvent(t *testing.T, ctx context.Context, i input.Streamed, match func(fsEvent) bool) []fsEvent {
	t.Helper()

	var seen []fsEvent
	for {
		e := nextEvent(t, ctx, i)
		seen = append(seen, e)
		if match(e) {
			return seen
		}
	}
}

// awaitSubdirWatched waits for the CREATE event of a new subdirectory. The
// input registers the directory with fsnotify before it emits that event, so
// files created in the directory afterwards are watched.
func awaitSubdirWatched(t *testing.T, ctx context.Context, i input.Streamed, subdir string) {
	t.Helper()

	awaitEvent(t, ctx, i, func(e fsEvent) bool {
		return e.is(subdir, "CREATE")
	})
}

func TestFSEventBasic(t *testing.T) {
	dir := t.TempDir()
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	testFile := filepath.Join(dir, "test.txt")
	require.NoError(t, os.WriteFile(testFile, []byte("initial content"), 0o644))

	i := fseventInput(t, `
fsevent:
  paths: [ "%v" ]
`, testFile)

	require.NoError(t, os.WriteFile(testFile, []byte("modified content"), 0o644))

	select {
	case tran := <-i.TransactionChan():
		require.NoError(t, tran.Ack(ctx, nil))
		msg := tran.Payload
		assert.Equal(t, 1, msg.Len())

		// Check metadata fields
		part := msg.Get(0)
		assert.Equal(t, testFile, part.MetaGetStr("fsevent_path"))
		assert.NotEmpty(t, part.MetaGetStr("fsevent_operation"))
		assert.NotEmpty(t, part.MetaGetStr("fsevent_mod_time_unix"))
		assert.NotEmpty(t, part.MetaGetStr("fsevent_mod_time"))

		operation := part.MetaGetStr("fsevent_operation")
		assert.Contains(t, operation, "WRITE")

	case <-ctx.Done():
		t.Fatal("timed out waiting for filesystem event")
	}
}

func TestFSEventCreateFile(t *testing.T) {
	dir := t.TempDir()
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	i := fseventInput(t, `
fsevent:
  paths: [ "%v" ]
`, dir)

	newFile := filepath.Join(dir, "newfile.txt")
	require.NoError(t, os.WriteFile(newFile, []byte("new file content"), 0o644))

	e := nextEvent(t, ctx, i)
	assert.Equal(t, newFile, e.path)
	assert.Contains(t, e.operation, "CREATE")
}

func TestFSEventDeleteFile(t *testing.T) {
	dir := t.TempDir()
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	testFile := filepath.Join(dir, "test.txt")
	require.NoError(t, os.WriteFile(testFile, []byte("content"), 0o644))

	i := fseventInput(t, `
fsevent:
  paths: [ "%v" ]
`, testFile)

	require.NoError(t, os.Remove(testFile))

	e := nextEvent(t, ctx, i)
	assert.Equal(t, testFile, e.path)
	// The operation should be REMOVE or CHMOD (some filesystems send CHMOD before REMOVE)
	assert.Contains(t, e.operation, "CHMOD", "Expected CHMOD operation, got: %s", e.operation)
}

func TestFSEventMultipleDirs(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	// Watch both directories
	i := fseventInput(t, `
fsevent:
  paths: [ "%v", "%v" ]
`, dir1, dir2)

	file1 := filepath.Join(dir1, "file1.txt")
	file2 := filepath.Join(dir2, "file2.txt")

	isWriteOrCreate := func(e fsEvent) {
		t.Helper()
		assert.True(t, e.operation == "WRITE" || e.operation == "CREATE", "Expected WRITE or CREATE operation, got: %s", e.operation)
	}

	require.NoError(t, os.WriteFile(file1, []byte("content1"), 0o644))
	for _, e := range awaitEvent(t, ctx, i, func(e fsEvent) bool { return e.path == file1 }) {
		isWriteOrCreate(e)
	}

	require.NoError(t, os.WriteFile(file2, []byte("content2"), 0o644))
	for _, e := range awaitEvent(t, ctx, i, func(e fsEvent) bool { return e.path == file2 }) {
		isWriteOrCreate(e)
	}
}

func TestFSEventWatchNewSubdirs(t *testing.T) {
	dir := t.TempDir()
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	i := fseventInput(t, `
fsevent:
  paths: [ "%v" ]
  watch_new_subdirs: true
`, dir)

	subdir := filepath.Join(dir, "subdir")
	require.NoError(t, os.Mkdir(subdir, 0o755))
	awaitSubdirWatched(t, ctx, i, subdir)

	fileInSubdir := filepath.Join(subdir, "file.txt")
	require.NoError(t, os.WriteFile(fileInSubdir, nil, 0o644))
	awaitEvent(t, ctx, i, func(e fsEvent) bool { return e.is(fileInSubdir, "CREATE") })
}

func TestFSEventWatchNewSubdirsDisabled(t *testing.T) {
	dir := t.TempDir()
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	i := fseventInput(t, `
fsevent:
  paths: [ "%v" ]
  watch_new_subdirs: false
`, dir)

	subdir := filepath.Join(dir, "subdir")
	require.NoError(t, os.Mkdir(subdir, 0o755))
	awaitEvent(t, ctx, i, func(e fsEvent) bool { return e.is(subdir, "CREATE") })

	fileInSubdir := filepath.Join(subdir, "file.txt")
	require.NoError(t, os.WriteFile(fileInSubdir, []byte("content"), 0o644))

	// A change in the watched directory made after the subdirectory write acts
	// as a sentinel: if the subdirectory had been watched, its event would
	// have been delivered before this one.
	sentinel := filepath.Join(dir, "sentinel.txt")
	require.NoError(t, os.WriteFile(sentinel, []byte("content"), 0o644))
	for _, e := range awaitEvent(t, ctx, i, func(e fsEvent) bool { return e.path == sentinel }) {
		assert.NotEqual(t, fileInSubdir, e.path, "Should NOT have received event for file in subdirectory when watch_new_subdirs is disabled")
	}
}

func TestFSEventWatchNewSubdirsDeleteRecreate(t *testing.T) {
	dir := t.TempDir()
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	i := fseventInput(t, `
fsevent:
  paths: [ "%v" ]
  watch_new_subdirs: true
`, dir)

	subdir := filepath.Join(dir, "subdir")
	require.NoError(t, os.Mkdir(subdir, 0o755))
	awaitSubdirWatched(t, ctx, i, subdir)

	file1 := filepath.Join(subdir, "file1.txt")
	require.NoError(t, os.WriteFile(file1, []byte("content1"), 0o644))
	awaitEvent(t, ctx, i, func(e fsEvent) bool { return e.is(file1, "WRITE", "CREATE") })

	require.NoError(t, os.RemoveAll(subdir))
	awaitEvent(t, ctx, i, func(e fsEvent) bool { return e.is(subdir, "REMOVE") })

	// fsnotify can report the removal of a watched directory more than once,
	// and each report drops whatever watch is under that path, so recreating
	// the directory straight away can lose the new watch to a stale report.
	// Events are handled in order, so once a later change to the parent has
	// been delivered every report of the removal has been processed.
	settled := filepath.Join(dir, "settled.txt")
	require.NoError(t, os.WriteFile(settled, nil, 0o644))
	awaitEvent(t, ctx, i, func(e fsEvent) bool { return e.path == settled })

	require.NoError(t, os.Mkdir(subdir, 0o755))
	awaitSubdirWatched(t, ctx, i, subdir)

	file2 := filepath.Join(subdir, "file2.txt")
	require.NoError(t, os.WriteFile(file2, []byte("content2"), 0o644))
	awaitEvent(t, ctx, i, func(e fsEvent) bool { return e.is(file2, "WRITE", "CREATE") })
}

func TestFSEventWatchNewSubdirsNestedLogic(t *testing.T) {
	// This test specifically targets the nested logic in input_fsevent.go
	// that handles watching newly created subdirectories
	dir := t.TempDir()
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	i := fseventInput(t, `
fsevent:
  paths: [ "%v" ]
  watch_new_subdirs: true
`, dir)

	// Create a new subdirectory - this should trigger the nested logic
	subdir := filepath.Join(dir, "newsubdir")
	require.NoError(t, os.Mkdir(subdir, 0o755))
	awaitSubdirWatched(t, ctx, i, subdir)

	// Now create a file in the new subdirectory - this should work because
	// the nested logic should have added the subdirectory to the watcher
	fileInSubdir := filepath.Join(subdir, "testfile.txt")
	require.NoError(t, os.WriteFile(fileInSubdir, []byte("test content"), 0o644))
	awaitEvent(t, ctx, i, func(e fsEvent) bool { return e.is(fileInSubdir, "WRITE", "CREATE") })
}

func TestFSEventExtensionFilter(t *testing.T) {
	dir := t.TempDir()
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	i := fseventInput(t, `
fsevent:
  paths: [ "%v" ]
  extensions: [ ".txt" ]
`, dir)

	// This file should be filtered out
	ignoredFile := filepath.Join(dir, "ignored.log")
	require.NoError(t, os.WriteFile(ignoredFile, []byte("should be ignored"), 0o644))

	// This file should trigger an event, and any event for the ignored file
	// would have been delivered before it.
	matchedFile := filepath.Join(dir, "matched.txt")
	require.NoError(t, os.WriteFile(matchedFile, []byte("should be seen"), 0o644))

	for _, e := range awaitEvent(t, ctx, i, func(e fsEvent) bool { return e.path == matchedFile }) {
		assert.NotEqual(t, ignoredFile, e.path, "Should not receive events for filtered extensions")
	}
}
