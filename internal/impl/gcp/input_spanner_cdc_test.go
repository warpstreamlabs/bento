package gcp

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/anicoll/screamer"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/public/service"
)

func TestCDCConfigFromParsed(t *testing.T) {
	tests := []struct {
		name    string
		config  string
		want    cdcConfig
		wantErr bool
	}{
		{
			name: "valid basic config",
			config: `
spanner_dsn: projects/test/instances/test/databases/test
metadata_table: test_table
spanner_metadata_dsn: projects/test/instances/test/databases/metadata
stream_name: test_stream`,
			want: cdcConfig{
				SpannerDSN:           "projects/test/instances/test/databases/test",
				SpannerMetadataTable: "test_table",
				SpannerMetadataDSN:   "projects/test/instances/test/databases/metadata",
				StreamName:           "test_stream",
				HeartbeatInterval:    3 * time.Second,
			},
		},
		{
			name: "config with start and end times",
			config: `
spanner_dsn: projects/test/instances/test/databases/test
metadata_table: test_table
spanner_metadata_dsn: projects/test/instances/test/databases/metadata
stream_name: test_stream
heartbeat_interval: 3s
start_time: 2025-01-01T00:00:00Z
end_time: 2025-12-31T23:59:59Z`,
			want: cdcConfig{
				SpannerDSN:           "projects/test/instances/test/databases/test",
				SpannerMetadataTable: "test_table",
				SpannerMetadataDSN:   "projects/test/instances/test/databases/metadata",
				StreamName:           "test_stream",
				HeartbeatInterval:    3 * time.Second,
				StartTime:            parseTimePtr(t, "2025-01-01T00:00:00Z"),
				EndTime:              parseTimePtr(t, "2025-12-31T23:59:59Z"),
			},
		},
		{
			name:    "missing required field",
			config:  `spanner_dsn: projects/test/instances/test/databases/test`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := spannerCdcSpec()
			parsed, err := spec.ParseYAML(tt.config, nil)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			got, err := cdcConfigFromParsed(parsed)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGcpSpannerCDCInput_Connect(t *testing.T) {
	ctx := context.Background()
	input := &gcpSpannerCDCInput{
		consumer: consumer{
			msgQueue: make(chan []byte, 1000),
		},
		subscriber:  &mockSubscriber{},
		closeSignal: shutdown.NewSignaller(),
	}

	err := input.Connect(ctx)
	time.Sleep(time.Second * 5)
	require.NoError(t, err)

	err = input.Close(ctx)
	require.NoError(t, err)
}

func TestGcpSpannerCDCInput_Read(t *testing.T) {
	ctx := context.Background()
	input := &gcpSpannerCDCInput{
		consumer: consumer{
			msgQueue: make(chan []byte, 1),
		},
	}

	// Test reading when channel is closed
	close(input.consumer.msgQueue)
	_, _, err := input.Read(ctx)
	assert.ErrorIs(t, err, service.ErrEndOfInput)

	// Test context cancellation
	input.consumer.msgQueue = make(chan []byte, 1)
	ctxWithCancel, cancel := context.WithCancel(ctx)
	cancel()
	_, _, err = input.Read(ctxWithCancel)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestGcpSpannerCDCInput_Read_metadata(t *testing.T) {
	ctx := context.Background()
	input := &gcpSpannerCDCInput{
		consumer: consumer{
			msgQueue: make(chan []byte, 1),
		},
	}

	inputMsg := screamer.DataChangeRecord{
		CommitTimestamp:     time.Now(),
		RecordSequence:      "98989",
		ServerTransactionID: uuid.NewString(),
		TableName:           "foo_bar",
		ModType:             screamer.ModType_DELETE,
	}
	expectedMetadata := map[string]any{
		metadataModType:     string(inputMsg.ModType),
		metadataRecordSeq:   inputMsg.RecordSequence,
		metadataServerTxnID: inputMsg.ServerTransactionID,
		metadataTableName:   inputMsg.TableName,
		metadataTimestamp:   inputMsg.CommitTimestamp.Format(time.RFC3339Nano),
	}

	inputData, err := json.Marshal(inputMsg)
	require.NoError(t, err)

	err = input.consumer.Consume(inputData)
	require.NoError(t, err)

	msg, _, err := input.Read(ctx)
	require.NoError(t, err)
	err = msg.MetaWalkMut(func(key string, value any) error {
		expectedValue, found := expectedMetadata[key]
		require.True(t, found)
		require.Equal(t, expectedValue, value)
		return nil
	})
	require.NoError(t, err)
}

func TestGcpSpannerCDCInput_SigTerm(t *testing.T) {
	ctx := context.Background()
	mockSub := &mockSubscriber{}

	input := &gcpSpannerCDCInput{
		consumer: consumer{
			msgQueue: make(chan []byte, 1000),
		},
		subscriber:  mockSub,
		log:         nil,
		closeSignal: shutdown.NewSignaller(),
	}

	// Connect the input
	err := input.Connect(ctx)
	require.NoError(t, err)
	// Wait for subscription goroutine to complete
	time.Sleep(time.Second * 5)

	// Ensure subscriber was called
	assert.True(t, mockSub.subscribeCalled)

	// Verify closeFunc is set
	input.cdcMut.Lock()
	assert.NotNil(t, input.closeFunc)
	input.cdcMut.Unlock()

	// Create a context with cancel to simulate SIGTERM
	sigCtx, cancel := context.WithCancel(ctx)

	// Start a goroutine that will try to read
	readDone := make(chan struct{})
	go func() {
		_, _, _ = input.Read(sigCtx)
		close(readDone)
	}()

	// Cancel the context to simulate SIGTERM
	cancel()

	// Wait for read to exit
	select {
	case <-readDone:
		// Expected path
	case <-time.After(time.Second):
		t.Fatal("Read did not exit after context cancellation")
	}

	// Clean up
	err = input.Close(ctx)
	require.NoError(t, err)
}

func TestGcpSpannerCDCInput_Read_WithValidMessage(t *testing.T) {
	ctx := context.Background()
	input := &gcpSpannerCDCInput{
		consumer: consumer{
			msgQueue: make(chan []byte, 1),
		},
	}

	// Prepare test data
	changeRecord := screamer.DataChangeRecord{
		CommitTimestamp:     time.Now(),
		RecordSequence:      "1234567",
		ServerTransactionID: uuid.NewString(),
		TableName:           "users",
		ModType:             screamer.ModType_INSERT,
		Mods: []*screamer.Mod{
			{
				Keys:      map[string]any{"id": "1"},
				NewValues: map[string]any{"id": "1", "name": "John Doe"},
			},
		},
	}

	recordBytes, err := json.Marshal(changeRecord)
	require.NoError(t, err)

	// Send data to the queue
	input.consumer.msgQueue <- recordBytes

	// Read the message
	msg, ackFunc, err := input.Read(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg)
	require.NotNil(t, ackFunc)

	// Verify message content
	var actualRecord screamer.DataChangeRecord
	bytes, err := msg.AsBytes()
	require.NoError(t, err)

	err = json.Unmarshal(bytes, &actualRecord)
	require.NoError(t, err)
	assert.Equal(t, changeRecord.TableName, actualRecord.TableName)
	assert.Equal(t, changeRecord.ModType, actualRecord.ModType)
	assert.Equal(t, changeRecord.RecordSequence, actualRecord.RecordSequence)
	assert.Equal(t, changeRecord.ServerTransactionID, actualRecord.ServerTransactionID)

	// Verify metadata
	expectedMetadata := map[string]any{
		metadataModType:     string(changeRecord.ModType),
		metadataRecordSeq:   changeRecord.RecordSequence,
		metadataServerTxnID: changeRecord.ServerTransactionID,
		metadataTableName:   changeRecord.TableName,
		metadataTimestamp:   changeRecord.CommitTimestamp.Format(time.RFC3339Nano),
	}

	err = msg.MetaWalkMut(func(key string, value any) error {
		expectedValue, found := expectedMetadata[key]
		assert.True(t, found, "Unexpected metadata key: %s", key)
		assert.Equal(t, expectedValue, value, "Metadata value mismatch for key %s", key)
		return nil
	})
	require.NoError(t, err)

	// Test the ack function
	err = ackFunc(ctx, nil)
	assert.NoError(t, err)
}

func TestGcpSpannerCDCInput_Read_UnmarshalError(t *testing.T) {
	ctx := context.Background()
	input := &gcpSpannerCDCInput{
		consumer: consumer{
			msgQueue: make(chan []byte, 1),
		},
	}

	// Send invalid JSON data
	input.consumer.msgQueue <- []byte(`{"invalid json`)

	// Read should return an error
	_, _, err := input.Read(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected end of JSON input")
}

func TestGcpSpannerCDCInput_Read_WithSigterm(t *testing.T) {
	// Create a context that can be canceled to simulate SIGTERM
	ctx, cancel := context.WithCancel(context.Background())

	input := &gcpSpannerCDCInput{
		consumer: consumer{
			msgQueue: make(chan []byte, 1),
		},
	}

	// Start a goroutine that will read from the input
	readErrCh := make(chan error, 1)
	go func() {
		_, _, err := input.Read(ctx)
		readErrCh <- err
	}()

	// Give the goroutine time to start
	time.Sleep(50 * time.Millisecond)

	// Simulate SIGTERM by canceling the context
	cancel()

	// Verify that Read returns the context canceled error
	select {
	case err := <-readErrCh:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("Read did not exit after context cancellation")
	}
}

func TestGcpSpannerCDCInput_Read_ClosedChannel(t *testing.T) {
	ctx := context.Background()
	input := &gcpSpannerCDCInput{
		consumer: consumer{
			msgQueue: make(chan []byte, 1),
		},
	}

	// Close the channel before attempting to read
	close(input.consumer.msgQueue)

	// Read should return ErrEndOfInput
	_, _, err := input.Read(ctx)
	assert.Equal(t, service.ErrEndOfInput, err)
}

func TestGcpSpannerCDCInput_Read_WithMultipleMessages(t *testing.T) {
	ctx := context.Background()
	input := &gcpSpannerCDCInput{
		consumer: consumer{
			msgQueue: make(chan []byte, 3),
		},
	}

	// Create several different messages
	for i := 0; i < 3; i++ {
		record := screamer.DataChangeRecord{
			CommitTimestamp:     time.Now().Add(time.Duration(i) * time.Second),
			RecordSequence:      fmt.Sprintf("seq-%d", i),
			ServerTransactionID: uuid.NewString(),
			TableName:           fmt.Sprintf("table_%d", i),
			ModType:             screamer.ModType(fmt.Sprintf("MOD_TYPE_%d", i)),
		}

		recordBytes, err := json.Marshal(record)
		require.NoError(t, err)
		input.consumer.msgQueue <- recordBytes
	}

	// Read and verify all three messages
	for i := 0; i < 3; i++ {
		msg, ackFunc, err := input.Read(ctx)
		require.NoError(t, err)
		require.NotNil(t, msg)

		var record screamer.DataChangeRecord
		bytes, err := msg.AsBytes()
		require.NoError(t, err)
		err = json.Unmarshal(bytes, &record)
		require.NoError(t, err)
		assert.Contains(t, record.TableName, "table_")

		tableName, found := msg.MetaGetMut(metadataTableName)
		assert.True(t, found)
		assert.Equal(t, record.TableName, tableName)

		err = ackFunc(ctx, nil)
		assert.NoError(t, err)
	}
}

// Helper function to parse time strings into time pointers
func parseTimePtr(t *testing.T, timeStr string) *time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, timeStr)
	require.NoError(t, err)
	return &parsed
}

// Mock implementations for testing
type mockSubscriber struct {
	subscribeCalled bool
	subscribeErr    error
}

func (m *mockSubscriber) Subscribe(ctx context.Context, consumer screamer.Consumer) error {
	m.subscribeCalled = true
	if m.subscribeErr != nil {
		return m.subscribeErr
	}

	// Keep the subscription running until context is canceled
	<-ctx.Done()
	return nil
}
