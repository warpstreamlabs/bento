package gcp

import (
	"context"
	"encoding/json"
	"testing"
	"time"

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
		subscriber: &mockSubscriber{},
	}

	err := input.Connect(ctx)
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
	assert.Equal(t, service.ErrNotConnected, err)

	// Test context cancellation
	input.consumer.msgQueue = make(chan []byte, 1)
	ctxWithCancel, cancel := context.WithCancel(ctx)
	cancel()
	_, _, err = input.Read(ctxWithCancel)
	assert.Equal(t, context.Canceled, err)
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
		metadataModType:     inputMsg.ModType,
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
	return m.subscribeErr
}
