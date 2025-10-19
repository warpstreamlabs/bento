package io_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/internal/impl/io"
)

// stripNewline removes trailing newline from line bytes for comparison
// The custom scanner keeps delimiters, so we need to strip them for testing
func stripNewline(b []byte) string {
	return string(bytes.TrimSuffix(b, []byte("\n")))
}

func TestStreamingFileInput_BasicReading(t *testing.T) {
	tmpDir := t.TempDir()
	stateDir := filepath.Join(tmpDir, "state")
	filePath := filepath.Join(tmpDir, "test.log")

	// Create test file
	testData := "line1\nline2\nline3\n"
	require.NoError(t, os.WriteFile(filePath, []byte(testData), 0644))

	cfg := io.StreamingFileInputConfig{
		Path:               filePath,
		StateDir:           stateDir,
		CheckpointInterval: 1,
		MaxBufferSize:      10,
		IdleTimeout:        50 * time.Millisecond,
		ReadTimeout:        100 * time.Millisecond,
	}

	input, err := io.NewStreamingFileInput(cfg, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, input.Connect(ctx))
	defer input.Close(ctx)

	// Give read loop time to start
	time.Sleep(100 * time.Millisecond)

	// Read lines
	msg1, _, err := input.Read(ctx)
	require.NoError(t, err)
	b1, err := msg1.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "line1", stripNewline(b1))

	msg2, _, err := input.Read(ctx)
	require.NoError(t, err)
	b2, err := msg2.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "line2", stripNewline(b2))

	msg3, _, err := input.Read(ctx)
	require.NoError(t, err)
	b3, err := msg3.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "line3", stripNewline(b3))
}

func TestStreamingFileInput_PositionPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	stateDir := filepath.Join(tmpDir, "state")
	filePath := filepath.Join(tmpDir, "test.log")

	// Create test file with multiple lines
	testData := "line1\nline2\nline3\nline4\nline5\n"
	require.NoError(t, os.WriteFile(filePath, []byte(testData), 0644))

	cfg := io.StreamingFileInputConfig{
		Path:               filePath,
		StateDir:           stateDir,
		CheckpointInterval: 2, // Save position every 2 lines
		MaxBufferSize:      10,
		IdleTimeout:        50 * time.Millisecond,
		ReadTimeout:        100 * time.Millisecond,
	}

	// First reader - read first 2 lines
	input1, err := io.NewStreamingFileInput(cfg, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, input1.Connect(ctx))
	time.Sleep(100 * time.Millisecond)

	// Read and ack first 2 lines
	_, ack1, err := input1.Read(ctx)
	require.NoError(t, err)
	require.NoError(t, ack1(ctx, nil))

	_, ack2, err := input1.Read(ctx)
	require.NoError(t, err)
	require.NoError(t, ack2(ctx, nil))

	// Give time for checkpoint
	time.Sleep(200 * time.Millisecond)

	require.NoError(t, input1.Close(ctx))

	// Second reader - should resume from position
	input2, err := io.NewStreamingFileInput(cfg, nil)
	require.NoError(t, err)

	require.NoError(t, input2.Connect(ctx))
	defer input2.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Should read line3 (next line after checkpoint)
	msg3, _, err := input2.Read(ctx)
	require.NoError(t, err)
	b3, err := msg3.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "line3", stripNewline(b3))
}

func TestStreamingFileInput_FileRotation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("File rotation test requires Unix-like system")
	}

	tmpDir := t.TempDir()
	stateDir := filepath.Join(tmpDir, "state")
	filePath := filepath.Join(tmpDir, "test.log")

	// Create initial file
	testData := "line1\nline2\n"
	require.NoError(t, os.WriteFile(filePath, []byte(testData), 0644))

	cfg := io.StreamingFileInputConfig{
		Path:               filePath,
		StateDir:           stateDir,
		CheckpointInterval: 1,
		MaxBufferSize:      10,
		IdleTimeout:        50 * time.Millisecond,
		ReadTimeout:        100 * time.Millisecond,
	}

	input, err := io.NewStreamingFileInput(cfg, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, input.Connect(ctx))
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Read first line
	msg1, ack1, err := input.Read(ctx)
	require.NoError(t, err)
	b1, err := msg1.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "line1", stripNewline(b1))
	require.NoError(t, ack1(ctx, nil))

	// Simulate file rotation by replacing the file
	newData := "line3\nline4\n"
	require.NoError(t, os.WriteFile(filePath, []byte(newData), 0644))

	// Give time for rotation detection
	time.Sleep(200 * time.Millisecond)

	// Should read from new file
	msg3, _, err := input.Read(ctx)
	require.NoError(t, err)
	b3, err := msg3.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "line3", stripNewline(b3))
}

func TestStreamingFileInput_ConcurrentReads(t *testing.T) {
	tmpDir := t.TempDir()
	stateDir := filepath.Join(tmpDir, "state")
	filePath := filepath.Join(tmpDir, "test.log")

	// Create test file with many lines
	var testData string
	for i := 1; i <= 100; i++ {
		testData += fmt.Sprintf("line%d\n", i)
	}
	require.NoError(t, os.WriteFile(filePath, []byte(testData), 0644))

	cfg := io.StreamingFileInputConfig{
		Path:               filePath,
		StateDir:           stateDir,
		CheckpointInterval: 10,
		MaxBufferSize:      50,
		IdleTimeout:        50 * time.Millisecond,
	}

	input, err := io.NewStreamingFileInput(cfg, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, input.Connect(ctx))
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Read all lines concurrently
	var wg sync.WaitGroup
	var mu sync.Mutex
	readLines := make(map[string]bool)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				msg, ack, err := input.Read(ctx)
				if err != nil {
					return
				}
				b, _ := msg.AsBytes()
				line := stripNewline(b)
				mu.Lock()
				readLines[line] = true
				mu.Unlock()
				ack(ctx, nil)
			}
		}()
	}

	wg.Wait()

	// Verify we read all lines
	assert.Equal(t, 100, len(readLines))
	for i := 1; i <= 100; i++ {
		assert.True(t, readLines[fmt.Sprintf("line%d", i)])
	}
}

func TestStreamingFileInput_StateFileFormat(t *testing.T) {
	tmpDir := t.TempDir()
	stateDir := filepath.Join(tmpDir, "state")
	filePath := filepath.Join(tmpDir, "test.log")

	testData := "line1\nline2\n"
	require.NoError(t, os.WriteFile(filePath, []byte(testData), 0644))

	cfg := io.StreamingFileInputConfig{
		Path:               filePath,
		StateDir:           stateDir,
		CheckpointInterval: 1,
		MaxBufferSize:      10,
		IdleTimeout:        50 * time.Millisecond,
	}

	input, err := io.NewStreamingFileInput(cfg, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, input.Connect(ctx))
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Read and ack a line
	_, ack, err := input.Read(ctx)
	require.NoError(t, err)
	require.NoError(t, ack(ctx, nil))

	time.Sleep(200 * time.Millisecond)
	require.NoError(t, input.Close(ctx))

	// Check state file exists and is valid JSON
	stateFiles, err := filepath.Glob(filepath.Join(stateDir, "pos_*.json"))
	require.NoError(t, err)
	require.Len(t, stateFiles, 1)

	data, err := os.ReadFile(stateFiles[0])
	require.NoError(t, err)

	var pos io.FilePosition
	require.NoError(t, json.Unmarshal(data, &pos))
	assert.Equal(t, filePath, pos.FilePath)
	assert.Greater(t, pos.RawOffset, int64(0))
}
