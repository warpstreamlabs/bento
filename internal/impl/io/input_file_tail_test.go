package io

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
)

func TestFileTail_Basic(t *testing.T) {
	fullPath := createFile(t, "Hello Alice")

	inputConf, err := testutil.InputFromYAML(fmt.Sprintf(`
file_tail:
  path: %v
`, fullPath))
	require.NoError(t, err)

	s, err := mock.NewManager().NewInput(inputConf)
	require.NoError(t, err)

	var receivedMsgs []string
	msgArrivedChan := make(chan struct{})

	var receivedPositions []string

	go func() {
		for msg := range s.TransactionChan() {
			bytes := msg.Payload.Get(0).AsBytes()
			position := msg.Payload.Get(0).MetaGetStr("file_tail_position")

			receivedPositions = append(receivedPositions, position)

			receivedMsgs = append(receivedMsgs, string(bytes))

			err := msg.Ack(context.Background(), nil)
			require.NoError(t, err)
			msgArrivedChan <- struct{}{}
		}
	}()

	<-msgArrivedChan

	appendLine(t, fullPath, "Hello Bob")

	<-msgArrivedChan

	s.TriggerStopConsuming()

	err = s.WaitForClose(context.Background())
	require.NoError(t, err)

	expectedMsgs := []string{"Hello Alice", "Hello Bob"}
	expectedPositions := []string{"12", "22"}

	assert.Equal(t, expectedPositions, receivedPositions)
	assert.Equal(t, expectedMsgs, receivedMsgs)
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

	var receivedMsgs []string
	msgArrivedChan := make(chan struct{})

	var receivedPositions []string

	go func() {
		for msg := range s.TransactionChan() {
			bytes := msg.Payload.Get(0).AsBytes()
			position := msg.Payload.Get(0).MetaGetStr("file_tail_position")

			receivedPositions = append(receivedPositions, position)

			receivedMsgs = append(receivedMsgs, string(bytes))

			err := msg.Ack(context.Background(), nil)
			require.NoError(t, err)
			msgArrivedChan <- struct{}{}
		}
	}()

	appendLine(t, fullPath, "Hello Bob")

	<-msgArrivedChan

	s.TriggerStopConsuming()

	err = s.WaitForClose(context.Background())
	require.NoError(t, err)

	expectedMsgs := []string{"Hello Bob"}
	expectedPositions := []string{"22"}

	assert.Equal(t, expectedPositions, receivedPositions)
	assert.Equal(t, expectedMsgs, receivedMsgs)
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

	var receivedMsgs []string
	msgArrivedChan := make(chan struct{})

	go func() {
		for msg := range s.TransactionChan() {
			bytes := msg.Payload.Get(0).AsBytes()

			receivedMsgs = append(receivedMsgs, string(bytes))

			err := msg.Ack(context.Background(), nil)
			require.NoError(t, err)
			msgArrivedChan <- struct{}{}
		}
	}()

	<-msgArrivedChan

	err = os.Rename(fullPath, filepath.Join(filepath.Dir(fullPath), "log1.txt"))
	require.NoError(t, err)

	// create new file with same path as before - with init data
	err = os.WriteFile(fullPath, []byte("Hello Bob"), 0o644)
	require.NoError(t, err)

	<-msgArrivedChan

	s.TriggerStopConsuming()

	expectedMsgs := []string{"Hello Alice", "Hello Bob"}

	assert.Equal(t, expectedMsgs, receivedMsgs)
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

	var receivedMsgs []string
	msgArrivedChan := make(chan struct{})

	go func() {
		for msg := range s.TransactionChan() {
			bytes := msg.Payload.Get(0).AsBytes()

			receivedMsgs = append(receivedMsgs, string(bytes))

			err := msg.Ack(context.Background(), nil)
			require.NoError(t, err)
			msgArrivedChan <- struct{}{}
		}
	}()

	<-msgArrivedChan

	err = os.Truncate(fullPath, 0)
	require.NoError(t, err)

	appendLine(t, fullPath, "Hello Bob")

	<-msgArrivedChan

	s.TriggerStopConsuming()

	expectedMsgs := []string{"Hello Alice", "Hello Bob"}

	assert.Equal(t, expectedMsgs, receivedMsgs)
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
