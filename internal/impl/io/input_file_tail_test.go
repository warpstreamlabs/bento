package io

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component/input"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
)

// tailCollector consumes and acks the messages of a file_tail input, records
// their contents and positions, and wakes the test on each arrival. The
// consumer never blocks on the test, so the input can be shut down cleanly
// however many messages it produced.
type tailCollector struct {
	mut       sync.Mutex
	msgs      []string
	positions []string
	arrived   chan struct{}
	done      chan struct{}
}

func collectTail(t *testing.T, s input.Streamed) *tailCollector {
	t.Helper()

	c := &tailCollector{
		arrived: make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
	go func() {
		defer close(c.done)
		for msg := range s.TransactionChan() {
			part := msg.Payload.Get(0)

			c.mut.Lock()
			c.msgs = append(c.msgs, string(part.AsBytes()))
			c.positions = append(c.positions, part.MetaGetStr("file_tail_position"))
			c.mut.Unlock()

			assert.NoError(t, msg.Ack(context.Background(), nil))

			select {
			case c.arrived <- struct{}{}:
			default:
			}
		}
	}()
	return c
}

func (c *tailCollector) snapshot() (msgs, positions []string) {
	c.mut.Lock()
	defer c.mut.Unlock()
	return slices.Clone(c.msgs), slices.Clone(c.positions)
}

// requireMessages waits until at least len(want) messages have arrived and
// then requires them to be exactly want.
func (c *tailCollector) requireMessages(t *testing.T, want ...string) {
	t.Helper()

	timeout := time.After(time.Second * 5)
	for {
		msgs, _ := c.snapshot()
		if len(msgs) >= len(want) {
			require.Equal(t, want, msgs)
			return
		}
		select {
		case <-c.arrived:
		case <-timeout:
			require.Equal(t, want, msgs, "timed out waiting for messages")
		}
	}
}

// stop shuts the input down and waits for the consumer to drain it.
func (c *tailCollector) stop(t *testing.T, s input.Streamed) {
	t.Helper()

	s.TriggerStopConsuming()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	require.NoError(t, s.WaitForClose(ctx))
	<-c.done
}

func TestFileTail_Basic(t *testing.T) {
	fullPath := createFile(t, "Hello Alice")

	inputConf, err := testutil.InputFromYAML(fmt.Sprintf(`
file_tail:
  path: %v
`, fullPath))
	require.NoError(t, err)

	s, err := mock.NewManager().NewInput(inputConf)
	require.NoError(t, err)

	c := collectTail(t, s)
	c.requireMessages(t, "Hello Alice")

	appendLine(t, fullPath, "Hello Bob")
	c.requireMessages(t, "Hello Alice", "Hello Bob")

	c.stop(t, s)

	_, positions := c.snapshot()
	assert.Equal(t, []string{"12", "22"}, positions)
}

func TestFileTail_StartPositionEnd(t *testing.T) {
	fullPath := createFile(t, "Hello Alice")

	inputConf, err := testutil.InputFromYAML(fmt.Sprintf(`
file_tail:
  path: %v
  start_position: end
`, fullPath))
	require.NoError(t, err)

	s, err := mock.NewManager().NewInput(inputConf)
	require.NoError(t, err)

	c := collectTail(t, s)

	appendLine(t, fullPath, "Hello Bob")
	c.requireMessages(t, "Hello Bob")

	c.stop(t, s)

	_, positions := c.snapshot()
	assert.Equal(t, []string{"22"}, positions)
}

func TestFileTail_FileRotation(t *testing.T) {
	fullPath := createFile(t, "Hello Alice")

	inputConf, err := testutil.InputFromYAML(fmt.Sprintf(`
file_tail:
  path: %v
`, fullPath))
	require.NoError(t, err)

	s, err := mock.NewManager().NewInput(inputConf)
	require.NoError(t, err)

	c := collectTail(t, s)
	c.requireMessages(t, "Hello Alice")

	err = os.Rename(fullPath, filepath.Join(filepath.Dir(fullPath), "log1.txt"))
	require.NoError(t, err)

	// create new file with same path as before - with init data
	err = os.WriteFile(fullPath, []byte("Hello Bob"), 0o644)
	require.NoError(t, err)

	c.requireMessages(t, "Hello Alice", "Hello Bob")

	c.stop(t, s)
}

func TestFileTail_FileTruncation(t *testing.T) {
	fullPath := createFile(t, "Hello Alice")

	inputConf, err := testutil.InputFromYAML(fmt.Sprintf(`
file_tail:
  path: %v
`, fullPath))
	require.NoError(t, err)

	s, err := mock.NewManager().NewInput(inputConf)
	require.NoError(t, err)

	c := collectTail(t, s)
	c.requireMessages(t, "Hello Alice")

	err = os.Truncate(fullPath, 0)
	require.NoError(t, err)

	appendLine(t, fullPath, "Hello Bob")
	c.requireMessages(t, "Hello Alice", "Hello Bob")

	c.stop(t, s)
}

func TestFileTail_Shutdown(t *testing.T) {
	fullPath := createFile(t, "Hello World")

	inputConf, err := testutil.InputFromYAML(fmt.Sprintf(`
file_tail:
  path: %v
`, fullPath))
	require.NoError(t, err)

	s, err := mock.NewManager().NewInput(inputConf)
	require.NoError(t, err)

	i := 0
	go func() {
		for msg := range s.TransactionChan() {
			_ = msg.Payload.Get(0).AsBytes()

			err := msg.Ack(context.Background(), nil)
			require.NoError(t, err)
			i++
		}
	}()

	// append a new lines to 'log.txt'
	f, err := os.OpenFile(fullPath, os.O_APPEND|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	defer f.Close()

	terminate := make(chan struct{})
	defer func() {
		terminate <- struct{}{}
	}()

	go func() {
		for {
			select {
			case <-terminate:
				return
			default:
				_, err = f.WriteString("Hello World\n")
			}
		}
	}()

	time.Sleep(time.Second * 2)

	s.TriggerStopConsuming()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	assert.NoError(t, s.WaitForClose(ctx))
}

func TestFileTail_ErrorHandling(t *testing.T) {
	fullPath := createFile(t, "Hello Alice")

	tail, err := newTail(fullPath, ifs.OS())
	require.NoError(t, err)

	ctx := context.Background()
	go tail.watch(ctx)

	<-tail.lineChan

	err = os.Remove(fullPath)
	require.NoError(t, err)

	tErr := <-tail.errChan

	assert.Contains(t, tErr.Error(), "no such file or directory")
}

func createFile(t *testing.T, content string) (fullPath string) {
	t.Helper()

	tmpDir := t.TempDir()

	fullPath = filepath.Join(tmpDir, "log.txt")

	err := os.WriteFile(fullPath, []byte(content+"\n"), 0o644)
	require.NoError(t, err)

	return fullPath
}

func appendLine(t *testing.T, fullPath string, content string) {
	f, err := os.OpenFile(fullPath, os.O_APPEND|os.O_WRONLY, 0o644)
	require.NoError(t, err)

	_, err = f.WriteString(content + "\n")
	require.NoError(t, err)

	defer f.Close()
}
