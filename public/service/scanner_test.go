package service

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockSimpleBatchScanner is a test helper that implements SimpleBatchScanner
type mockSimpleBatchScanner struct {
	batches []MessageBatch
	errs    []error
	index   int
	closed  bool
}

func (m *mockSimpleBatchScanner) NextBatch(ctx context.Context) (MessageBatch, error) {
	if m.index >= len(m.batches) {
		return nil, io.EOF
	}

	batch := m.batches[m.index]
	err := m.errs[m.index]
	m.index++

	return batch, err
}

func (m *mockSimpleBatchScanner) Close(ctx context.Context) error {
	m.closed = true
	return nil
}

func TestAutoAggregateBatchScannerAcksWithNilAckFunc(t *testing.T) {
	// This test verifies that AutoAggregateBatchScannerAcks does not panic
	// when provided with a nil AckFunc and the scanner encounters a non-EOF error.
	// This reproduces the issue where the S3 processor passes nil as AckFunc
	// and the CSV scanner encounters a parse error.

	parseErr := errors.New("csv parse error: bare \" in non-quoted-field")

	mock := &mockSimpleBatchScanner{
		batches: []MessageBatch{nil},
		errs:    []error{parseErr},
	}

	// Pass nil as AckFunc - this is what the S3 processor does
	scanner := AutoAggregateBatchScannerAcks(mock, nil)

	// This should NOT panic even though AckFunc is nil
	require.NotPanics(t, func() {
		_, _, err := scanner.NextBatch(context.Background())
		// The error should be propagated
		assert.Equal(t, parseErr, err)
	})

	// Clean up
	require.NoError(t, scanner.Close(context.Background()))
	assert.True(t, mock.closed)
}

func TestAutoAggregateBatchScannerAcksWithNilAckFuncOnClose(t *testing.T) {
	// Test that Close doesn't panic when AckFunc is nil and scanner wasn't finished

	mock := &mockSimpleBatchScanner{
		batches: []MessageBatch{{NewMessage([]byte("test"))}},
		errs:    []error{nil},
	}

	scanner := AutoAggregateBatchScannerAcks(mock, nil)

	// Close without consuming all messages (simulates early termination)
	// This should NOT panic
	require.NotPanics(t, func() {
		require.NoError(t, scanner.Close(context.Background()))
	})
	assert.True(t, mock.closed)
}

func TestAutoAggregateBatchScannerAcksWithValidAckFunc(t *testing.T) {
	// Test normal operation with a valid AckFunc to ensure the fix
	// doesn't break the normal case

	mock := &mockSimpleBatchScanner{
		batches: []MessageBatch{
			{NewMessage([]byte("first"))},
			{NewMessage([]byte("second"))},
		},
		errs: []error{nil, nil},
	}

	var ackCalled bool
	var ackErr error
	ackFn := func(ctx context.Context, err error) error {
		ackCalled = true
		ackErr = err
		return nil
	}

	scanner := AutoAggregateBatchScannerAcks(mock, ackFn)

	// Read first batch
	batch1, ack1, err := scanner.NextBatch(context.Background())
	require.NoError(t, err)
	require.Len(t, batch1, 1)
	b1, _ := batch1[0].AsBytes()
	assert.Equal(t, "first", string(b1))

	// Read second batch
	batch2, ack2, err := scanner.NextBatch(context.Background())
	require.NoError(t, err)
	require.Len(t, batch2, 1)
	b2, _ := batch2[0].AsBytes()
	assert.Equal(t, "second", string(b2))

	// Read EOF
	_, _, err = scanner.NextBatch(context.Background())
	assert.Equal(t, io.EOF, err)

	// Ack should not be called yet
	assert.False(t, ackCalled)

	// Ack first batch
	require.NoError(t, ack1(context.Background(), nil))
	assert.False(t, ackCalled) // Still waiting for second

	// Ack second batch - this should trigger the source ack
	require.NoError(t, ack2(context.Background(), nil))
	assert.True(t, ackCalled)
	assert.NoError(t, ackErr)

	require.NoError(t, scanner.Close(context.Background()))
}

func TestAutoAggregateBatchScannerAcksErrorPropagatesAck(t *testing.T) {
	// Test that when a batch ack receives an error, it propagates to source ack

	mock := &mockSimpleBatchScanner{
		batches: []MessageBatch{
			{NewMessage([]byte("first"))},
		},
		errs: []error{nil},
	}

	var ackErr error
	ackFn := func(ctx context.Context, err error) error {
		ackErr = err
		return nil
	}

	scanner := AutoAggregateBatchScannerAcks(mock, ackFn)

	// Read batch
	_, ack, err := scanner.NextBatch(context.Background())
	require.NoError(t, err)

	// Read EOF
	_, _, err = scanner.NextBatch(context.Background())
	assert.Equal(t, io.EOF, err)

	// Ack with error
	processingErr := errors.New("processing failed")
	require.NoError(t, ack(context.Background(), processingErr))

	// Source ack should receive the error
	assert.Equal(t, processingErr, ackErr)

	require.NoError(t, scanner.Close(context.Background()))
}
