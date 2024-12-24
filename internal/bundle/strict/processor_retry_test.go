package strict

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
)

//------------------------------------------------------------------------------

type mockProcRetry struct {
	retries int
}

func (m *mockProcRetry) ProcessBatch(ctx context.Context, batch message.Batch) ([]message.Batch, error) {
	m.retries++
	batch.Get(0).ErrorSet(errors.New("sad"))
	return []message.Batch{batch}, nil
}

func (m *mockProcRetry) Close(ctx context.Context) error {
	// Do nothing as our processor doesn't require resource cleanup.
	return nil
}

//------------------------------------------------------------------------------

func TestProcessorWrapWithRetry(t *testing.T) {
	tCtx := context.Background()
	mgr := mock.NewManager()

	mock := &mockProcRetry{}
	retryProc := wrapWithRetry(mock, mgr, 3)

	msg := message.QuickBatch([][]byte{[]byte("this should be retried")})

	start := time.Now()
	msgs, err := retryProc.ProcessBatch(tCtx, msg)
	require.Error(t, err, "Expected error on first attempt")
	require.Empty(t, msgs)

	msgs, err = retryProc.ProcessBatch(tCtx, msg)
	require.Error(t, err, "Expected error on second attempt")
	require.Empty(t, msgs)

	msgs, err = retryProc.ProcessBatch(tCtx, msg)
	require.Error(t, err, "Expected error on third attempt")
	require.Empty(t, msgs)

	elapsedTime := time.Since(start)
	expectedMinimum := 200 * time.Millisecond // [200ms, 600ms]
	require.GreaterOrEqualf(t, elapsedTime, expectedMinimum, "Total wait duration should be at least %v, got %v", expectedMinimum, elapsedTime)

	msgs, err = retryProc.ProcessBatch(tCtx, msg)
	require.NoError(t, err, "Expected no error as maxiumum retries exceeded")
	require.Len(t, msgs, 1)

	require.Equal(t, 4, mock.retries,
		"Expected exactly %d attempts", 4)
}

func TestProcessorWrapWithRetryMultiMessage(t *testing.T) {
	tCtx := context.Background()
	mgr := mock.NewManager()

	mock := &mockProcRetry{}
	retryProc := wrapWithRetry(mock, mgr, 5)

	msg := message.QuickBatch([][]byte{
		[]byte("not a structured doc"),
		[]byte(`{"foo":"oof"}`),
		[]byte(`{"bar":"rab"}`),
	})

	start := time.Now()
	msgs, err := retryProc.ProcessBatch(tCtx, msg)
	require.Error(t, err, "Expected error on first attempt")
	require.Empty(t, msgs)

	msgs, err = retryProc.ProcessBatch(tCtx, msg)
	require.Error(t, err, "Expected error on second attempt")
	require.Empty(t, msgs)

	msgs, err = retryProc.ProcessBatch(tCtx, msg)
	require.Error(t, err, "Expected error on third attempt")
	require.Empty(t, msgs)

	msgs, err = retryProc.ProcessBatch(tCtx, msg)
	require.Error(t, err, "Expected error on fourth attempt")
	require.Empty(t, msgs)

	msgs, err = retryProc.ProcessBatch(tCtx, msg)
	require.Error(t, err, "Expected error on fifth attempt")
	require.Empty(t, msgs)

	elapsedTime := time.Since(start)
	expectedMinimum := 800 * time.Millisecond // [800ms, 2400ms]
	require.GreaterOrEqualf(t, elapsedTime, expectedMinimum, "Total wait duration should be at least %v, got %v", expectedMinimum, elapsedTime)

	msgs, err = retryProc.ProcessBatch(tCtx, msg)
	require.NoError(t, err, "Expected no error as maxiumum retries exceeded")
	require.Len(t, msgs, 1)

	require.Equal(t, 6, mock.retries,
		"Expected exactly %d attempts", 6)
}
