package io_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component/input"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager/mock"

	_ "github.com/warpstreamlabs/bento/internal/impl/io"
)

func fseventInput(t testing.TB, confPattern string, args ...any) input.Streamed {
	iConf, err := testutil.InputFromYAML(fmt.Sprintf(confPattern, args...))
	require.NoError(t, err)

	i, err := mock.NewManager().NewInput(iConf)
	require.NoError(t, err)

	return i
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

	// Wait for the input to connect and start watching
	time.Sleep(time.Second)

	err := os.WriteFile(testFile, []byte("modified content"), 0o644)
	require.NoError(t, err)

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

	case <-time.After(time.Second * 5):
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

	// Wait for the input to connect and start watching
	time.Sleep(time.Second)

	newFile := filepath.Join(dir, "newfile.txt")
	err := os.WriteFile(newFile, []byte("new file content"), 0o644)
	require.NoError(t, err)

	select {
	case tran := <-i.TransactionChan():
		require.NoError(t, tran.Ack(ctx, nil))
		msg := tran.Payload
		assert.Equal(t, 1, msg.Len())

		part := msg.Get(0)
		assert.Equal(t, newFile, part.MetaGetStr("fsevent_path"))

		operation := part.MetaGetStr("fsevent_operation")
		assert.Contains(t, operation, "CREATE")

	case <-time.After(time.Second * 5):
		t.Fatal("timed out waiting for create event")
	}
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

	// Wait for the input to connect and start watching
	time.Sleep(time.Second)

	err := os.Remove(testFile)
	require.NoError(t, err)

	select {
	case tran := <-i.TransactionChan():
		require.NoError(t, tran.Ack(ctx, nil))
		msg := tran.Payload
		assert.Equal(t, 1, msg.Len())

		part := msg.Get(0)
		assert.Equal(t, testFile, part.MetaGetStr("fsevent_path"))

		// The operation should be REMOVE or CHMOD (some filesystems send CHMOD before REMOVE)
		operation := part.MetaGetStr("fsevent_operation")
		assert.Contains(t, operation, "CHMOD", "Expected CHMOD operation, got: %s", operation)

	case <-time.After(time.Second * 5):
		t.Fatal("timed out waiting for delete event")
	}
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

	// Give the watcher a moment to start up
	time.Sleep(time.Second)

	file1 := filepath.Join(dir1, "file1.txt")
	file2 := filepath.Join(dir2, "file2.txt")

	require.NoError(t, os.WriteFile(file1, []byte("content1"), 0o644))

	// Wait for event from file1
	var dir1Event bool
	for j := 0; j < 10; j++ {
		select {
		case tran := <-i.TransactionChan():
			require.NoError(t, tran.Ack(ctx, nil))
			msg := tran.Payload
			assert.Equal(t, 1, msg.Len())

			part := msg.Get(0)
			eventPath := part.MetaGetStr("fsevent_path")
			operation := part.MetaGetStr("fsevent_operation")
			assert.True(t, operation == "WRITE" || operation == "CREATE", "Expected WRITE or CREATE operation, got: %s", operation)

			if eventPath == file1 {
				dir1Event = true
				break
			}

		case <-time.After(time.Second * 1):
			continue
		}
	}

	require.NoError(t, os.WriteFile(file2, []byte("content2"), 0o644))

	// Wait for event from file2
	var dir2Event bool
	for j := 0; j < 10; j++ {
		select {
		case tran := <-i.TransactionChan():
			require.NoError(t, tran.Ack(ctx, nil))
			msg := tran.Payload
			assert.Equal(t, 1, msg.Len())

			part := msg.Get(0)
			eventPath := part.MetaGetStr("fsevent_path")
			operation := part.MetaGetStr("fsevent_operation")
			assert.True(t, operation == "WRITE" || operation == "CREATE", "Expected WRITE or CREATE operation, got: %s", operation)

			if eventPath == file2 {
				dir2Event = true
				break
			}

		case <-time.After(time.Second * 1):
			continue
		}
	}

	assert.True(t, dir1Event, "Should have received event from dir1")
	assert.True(t, dir2Event, "Should have received event from dir2")
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

	// Wait for the input to connect and start watching
	time.Sleep(time.Second)

	subdir := filepath.Join(dir, "subdir")
	require.NoError(t, os.Mkdir(subdir, 0o755))

	// Wait a bit for the CREATE event to be processed
	time.Sleep(time.Second)

	fileInSubdir := filepath.Join(subdir, "file.txt")
	require.NoError(t, os.WriteFile(fileInSubdir, []byte("content"), 0o644))

	// We should receive events for both the subdir creation and the file creation
	var subdirCreated, fileCreated bool
	for j := 0; j < 10; j++ {
		select {
		case tran := <-i.TransactionChan():
			require.NoError(t, tran.Ack(ctx, nil))
			msg := tran.Payload
			assert.Equal(t, 1, msg.Len())

			part := msg.Get(0)
			eventPath := part.MetaGetStr("fsevent_path")
			operation := part.MetaGetStr("fsevent_operation")

			if eventPath == subdir && operation == "CREATE" {
				subdirCreated = true
			} else if eventPath == fileInSubdir && (operation == "WRITE" || operation == "CREATE") {
				fileCreated = true
			}

			if subdirCreated && fileCreated {
				break
			}

		case <-time.After(time.Second * 1):
			continue
		}
	}

	assert.True(t, subdirCreated, "Should have received CREATE event for subdirectory")
	assert.True(t, fileCreated, "Should have received event for file in subdirectory")
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

	// Wait for the input to connect and start watching
	time.Sleep(time.Second)

	subdir := filepath.Join(dir, "subdir")
	require.NoError(t, os.Mkdir(subdir, 0o755))

	// Wait a bit for the CREATE event to be processed
	time.Sleep(time.Second)

	fileInSubdir := filepath.Join(subdir, "file.txt")
	require.NoError(t, os.WriteFile(fileInSubdir, []byte("content"), 0o644))

	var subdirCreated, fileCreated bool
	for j := 0; j < 10; j++ {
		select {
		case tran := <-i.TransactionChan():
			require.NoError(t, tran.Ack(ctx, nil))
			msg := tran.Payload
			assert.Equal(t, 1, msg.Len())

			part := msg.Get(0)
			eventPath := part.MetaGetStr("fsevent_path")
			operation := part.MetaGetStr("fsevent_operation")

			if eventPath == subdir && operation == "CREATE" {
				subdirCreated = true
			} else if eventPath == fileInSubdir && (operation == "WRITE" || operation == "CREATE") {
				fileCreated = true
			}

			if fileCreated {
				break
			}

		case <-time.After(time.Second * 1):
			continue
		}
	}

	assert.True(t, subdirCreated, "Should have received CREATE event for subdirectory")
	assert.False(t, fileCreated, "Should NOT have received event for file in subdirectory when watch_new_subdirs is disabled")
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

	// Wait for the input to connect and start watching
	time.Sleep(time.Second)

	subdir := filepath.Join(dir, "subdir")
	require.NoError(t, os.Mkdir(subdir, 0o755))

	// Wait a bit for the CREATE event to be processed
	time.Sleep(time.Second)

	file1 := filepath.Join(subdir, "file1.txt")
	require.NoError(t, os.WriteFile(file1, []byte("content1"), 0o644))

	var subdirCreated, file1Created bool
	for j := 0; j < 10; j++ {
		select {
		case tran := <-i.TransactionChan():
			require.NoError(t, tran.Ack(ctx, nil))
			msg := tran.Payload
			assert.Equal(t, 1, msg.Len())

			part := msg.Get(0)
			eventPath := part.MetaGetStr("fsevent_path")
			operation := part.MetaGetStr("fsevent_operation")

			if eventPath == subdir && operation == "CREATE" {
				subdirCreated = true
			} else if eventPath == file1 && (operation == "WRITE" || operation == "CREATE") {
				file1Created = true
			}

			if subdirCreated && file1Created {
				break
			}

		case <-time.After(time.Second * 1):
			continue
		}
	}

	assert.True(t, subdirCreated, "Should have received CREATE event for subdirectory")
	assert.True(t, file1Created, "Should have received event for file in subdirectory")

	require.NoError(t, os.RemoveAll(subdir))

	var subdirDeleted bool
	for j := 0; j < 10; j++ {
		select {
		case tran := <-i.TransactionChan():
			require.NoError(t, tran.Ack(ctx, nil))
			msg := tran.Payload
			assert.Equal(t, 1, msg.Len())

			part := msg.Get(0)
			eventPath := part.MetaGetStr("fsevent_path")
			operation := part.MetaGetStr("fsevent_operation")

			if eventPath == subdir && (operation == "REMOVE" || operation == "CHMOD") {
				subdirDeleted = true
			}

			if subdirDeleted {
				break
			}

		case <-time.After(time.Second * 1):
			continue
		}
	}

	assert.True(t, subdirDeleted, "Should have received DELETE event for subdirectory")

	time.Sleep(time.Second) // Give it a moment
	require.NoError(t, os.Mkdir(subdir, 0o755))

	time.Sleep(time.Second) // Give it a moment
	file2 := filepath.Join(subdir, "file2.txt")
	require.NoError(t, os.WriteFile(file2, []byte("content2"), 0o644))

	var subdirRecreated, file2Created bool
	for j := 0; j < 10; j++ {
		select {
		case tran := <-i.TransactionChan():
			require.NoError(t, tran.Ack(ctx, nil))
			msg := tran.Payload
			assert.Equal(t, 1, msg.Len())

			part := msg.Get(0)
			eventPath := part.MetaGetStr("fsevent_path")
			operation := part.MetaGetStr("fsevent_operation")

			if eventPath == subdir && operation == "CREATE" {
				subdirRecreated = true
			} else if eventPath == file2 && (operation == "WRITE" || operation == "CREATE") {
				file2Created = true
			}

			if subdirRecreated && file2Created {
				break
			}

		case <-time.After(time.Second * 1):
			continue
		}
	}

	assert.True(t, subdirRecreated, "Should have received CREATE event for recreated subdirectory")
	assert.True(t, file2Created, "Should have received event for file in recreated subdirectory")
}

func TestFSEventWatchNewSubdirsNestedLogic(t *testing.T) {
	// This test specifically targets the nested logic at line 158 in input_fsevent.go
	// that handles watching newly created subdirectories
	dir := t.TempDir()
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	i := fseventInput(t, `
fsevent:
  paths: [ "%v" ]
  watch_new_subdirs: true
`, dir)

	// Wait for the input to connect and start watching
	time.Sleep(time.Second)

	// Create a new subdirectory - this should trigger the nested logic
	subdir := filepath.Join(dir, "newsubdir")
	require.NoError(t, os.Mkdir(subdir, 0o755))

	// Wait for the CREATE event to be processed
	time.Sleep(time.Second)

	// Now create a file in the new subdirectory - this should work because
	// the nested logic should have added the subdirectory to the watcher
	fileInSubdir := filepath.Join(subdir, "testfile.txt")
	require.NoError(t, os.WriteFile(fileInSubdir, []byte("test content"), 0o644))

	// We should receive events for both the subdir creation and the file creation
	var subdirCreated, fileCreated bool
	for j := 0; j < 10; j++ {
		select {
		case tran := <-i.TransactionChan():
			require.NoError(t, tran.Ack(ctx, nil))
			msg := tran.Payload
			assert.Equal(t, 1, msg.Len())

			part := msg.Get(0)
			eventPath := part.MetaGetStr("fsevent_path")
			operation := part.MetaGetStr("fsevent_operation")

			if eventPath == subdir && operation == "CREATE" {
				subdirCreated = true
			} else if eventPath == fileInSubdir && (operation == "WRITE" || operation == "CREATE") {
				fileCreated = true
			}

			if subdirCreated && fileCreated {
				break
			}

		case <-time.After(time.Second * 1):
			continue
		}
	}

	assert.True(t, subdirCreated, "Should have received CREATE event for new subdirectory")
	assert.True(t, fileCreated, "Should have received event for file in new subdirectory (nested logic working)")
}
