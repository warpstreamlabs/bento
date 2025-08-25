package gcp

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/spannertest"
	"github.com/Jeffail/shutdown"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	types "github.com/warpstreamlabs/bento/internal/impl/gcp/types"
	"github.com/warpstreamlabs/bento/public/service"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
stream_name: test_stream`,
			want: cdcConfig{
				SpannerDSN:        "projects/test/instances/test/databases/test",
				StreamName:        "test_stream",
				HeartbeatInterval: 3 * time.Second,
			},
		},
		{
			name: "config with start and end times",
			config: `
spanner_dsn: projects/test/instances/test/databases/test
stream_name: test_stream
heartbeat_interval: 3s
start_time: 2025-01-01T00:00:00Z
end_time: 2025-12-31T23:59:59Z`,
			want: cdcConfig{
				SpannerDSN:        "projects/test/instances/test/databases/test",
				StreamName:        "test_stream",
				HeartbeatInterval: 3 * time.Second,
				StartTime:         parseTimePtr(t, "2025-01-01T00:00:00Z"),
				EndTime:           parseTimePtr(t, "2025-12-31T23:59:59Z"),
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

			// For the "valid basic config" test, StartTime is set to time.Now()
			// so we need to check it separately
			if tt.name == "valid basic config" {
				assert.Equal(t, tt.want.SpannerDSN, got.SpannerDSN)
				assert.Equal(t, tt.want.StreamName, got.StreamName)
				assert.Equal(t, tt.want.HeartbeatInterval, got.HeartbeatInterval)
				assert.NotNil(t, got.StartTime)
				assert.Nil(t, got.EndTime)
			} else {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func setupTestSpanner(t *testing.T) (*spanner.Client, string, func()) {
	t.Helper()

	srv, err := spannertest.NewServer("localhost:0")
	require.NoError(t, err)

	t.Setenv("SPANNER_EMULATOR_HOST", srv.Addr)

	database := "projects/test-project/instances/test-instance/databases/test-database"

	conn, err := grpc.NewClient(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	client, err := spanner.NewClient(context.Background(), database, option.WithGRPCConn(conn))
	require.NoError(t, err)

	cleanup := func() {
		client.Close()
		srv.Close()
	}

	return client, database, cleanup
}

func TestGcpSpannerCDCInput_Connect(t *testing.T) {
	ctx := context.Background()
	_, dsn, cleanup := setupTestSpanner(t)
	defer cleanup()

	input := &gcpSpannerCDCInput{
		conf: cdcConfig{
			SpannerDSN:        dsn,
			StreamName:        "test_stream",
			HeartbeatInterval: 3 * time.Second,
		},
		recordsCh:   make(chan changeRecord, 1000),
		shutdownSig: shutdown.NewSignaller(),
	}

	err := input.Connect(ctx)
	require.NoError(t, err)

	err = input.Close(ctx)
	require.NoError(t, err)
}

func TestGcpSpannerCDCInput_Read(t *testing.T) {
	tests := []struct {
		name     string
		setup    func() *gcpSpannerCDCInput
		ctx      func() context.Context
		expected error
	}{
		{
			name: "closed_channel",
			setup: func() *gcpSpannerCDCInput {
				input := &gcpSpannerCDCInput{
					streamClient: &spanner.Client{},
					recordsCh:    make(chan changeRecord, 1),
					shutdownSig:  shutdown.NewSignaller(),
				}
				close(input.recordsCh)
				return input
			},
			ctx:      func() context.Context { return context.Background() },
			expected: service.ErrEndOfInput,
		},
		{
			name: "canceled_context",
			setup: func() *gcpSpannerCDCInput {
				return &gcpSpannerCDCInput{
					streamClient: &spanner.Client{},
					recordsCh:    make(chan changeRecord, 1),
					shutdownSig:  shutdown.NewSignaller(),
				}
			},
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			expected: context.Canceled,
		},
		{
			name: "nil_channel",
			setup: func() *gcpSpannerCDCInput {
				return &gcpSpannerCDCInput{
					streamClient: &spanner.Client{},
					recordsCh:    nil,
					shutdownSig:  shutdown.NewSignaller(),
				}
			},
			ctx:      func() context.Context { return context.Background() },
			expected: service.ErrNotConnected,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := tt.setup()
			ctx := tt.ctx()
			_, _, err := input.Read(ctx)
			assert.ErrorIs(t, err, tt.expected)
		})
	}
}

func TestGcpSpannerCDCInput_ReadMetadata(t *testing.T) {
	ctx := context.Background()
	_, dsn, cleanup := setupTestSpanner(t)
	defer cleanup()

	client, err := spanner.NewClient(ctx, dsn)
	require.NoError(t, err)
	defer client.Close()

	input := &gcpSpannerCDCInput{
		streamClient: client,
		recordsCh:    make(chan changeRecord, 1),
		shutdownSig:  shutdown.NewSignaller(),
	}

	now := time.Now().UTC()
	serverTxnID := uuid.NewString()
	recordSeq := "98989"
	tableName := "foo_bar"

	inputMsg := changeRecord{
		data: &types.DataChangeRecord{
			CommitTimestamp:     types.TimeMills(now),
			RecordSequence:      recordSeq,
			ServerTransactionId: serverTxnID,
			TableName:           tableName,
			ModType:             "DELETE",
		},
		mod: &types.Mod{
			Keys: spanner.NullJSON{Valid: true, Value: map[string]interface{}{"id": "123"}},
		},
		modType: "DELETE",
	}

	expectedMetadata := map[string]any{
		metadataModType:     "DELETE",
		metadataRecordSeq:   recordSeq,
		metadataServerTxnID: serverTxnID,
		metadataTableName:   tableName,
		metadataTimestamp:   now.Format(time.RFC3339Nano),
	}

	input.recordsCh <- inputMsg

	msg, _, err := input.Read(ctx)
	require.NoError(t, err)
	err = msg.MetaWalkMut(func(key string, value any) error {
		expectedValue, found := expectedMetadata[key]
		require.True(t, found, "Key %s not found in expected metadata", key)
		require.Equal(t, expectedValue, value)
		return nil
	})
	require.NoError(t, err)
}

func TestGcpSpannerCDCInput_ContextCancellation(t *testing.T) {
	ctx := context.Background()
	_, dsn, cleanup := setupTestSpanner(t)
	defer cleanup()

	input := &gcpSpannerCDCInput{
		conf: cdcConfig{
			SpannerDSN:        dsn,
			StreamName:        "test_stream",
			HeartbeatInterval: 3 * time.Second,
		},
		recordsCh:   make(chan changeRecord, 1000),
		shutdownSig: shutdown.NewSignaller(),
	}

	err := input.Connect(ctx)
	require.NoError(t, err)

	sigCtx, cancel := context.WithCancel(context.Background())

	readErrCh := make(chan error, 1)
	go func() {
		_, _, err := input.Read(sigCtx)
		readErrCh <- err
	}()

	cancel()

	select {
	case err := <-readErrCh:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("Read did not exit after context cancellation")
	}

	err = input.Close(ctx)
	require.NoError(t, err)
}

func TestGcpSpannerCDCInput_ReadValidMessage(t *testing.T) {
	ctx := context.Background()
	_, dsn, cleanup := setupTestSpanner(t)
	defer cleanup()

	client, err := spanner.NewClient(ctx, dsn)
	require.NoError(t, err)
	defer client.Close()

	input := &gcpSpannerCDCInput{
		streamClient: client,
		recordsCh:    make(chan changeRecord, 1),
		shutdownSig:  shutdown.NewSignaller(),
	}

	now := time.Now().UTC()
	serverTxnID := uuid.NewString()
	recordSeq := "1234567"
	tableName := "users"

	record := changeRecord{
		data: &types.DataChangeRecord{
			CommitTimestamp:     types.TimeMills(now),
			RecordSequence:      recordSeq,
			ServerTransactionId: serverTxnID,
			TableName:           tableName,
			ModType:             "INSERT",
		},
		mod: &types.Mod{
			Keys:      spanner.NullJSON{Valid: true, Value: map[string]interface{}{"id": "1"}},
			NewValues: spanner.NullJSON{Valid: true, Value: map[string]interface{}{"id": "1", "name": "John Doe"}},
		},
		modType: "INSERT",
	}

	input.recordsCh <- record

	msg, ackFunc, err := input.Read(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg)
	require.NotNil(t, ackFunc)

	expectedMetadata := map[string]any{
		metadataModType:     "INSERT",
		metadataRecordSeq:   recordSeq,
		metadataServerTxnID: serverTxnID,
		metadataTableName:   tableName,
		metadataTimestamp:   now.Format(time.RFC3339Nano),
	}

	err = msg.MetaWalkMut(func(key string, value any) error {
		expectedValue, found := expectedMetadata[key]
		assert.True(t, found, "Unexpected metadata key: %s", key)
		assert.Equal(t, expectedValue, value, "Metadata value mismatch for key %s", key)
		return nil
	})
	require.NoError(t, err)

	err = ackFunc(ctx, nil)
	assert.NoError(t, err)
}

func TestGcpSpannerCDCInput_ReadContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	_, dsn, cleanup := setupTestSpanner(t)
	defer cleanup()

	client, err := spanner.NewClient(context.Background(), dsn)
	require.NoError(t, err)
	defer client.Close()

	input := &gcpSpannerCDCInput{
		streamClient: client,
		recordsCh:    make(chan changeRecord, 1),
		shutdownSig:  shutdown.NewSignaller(),
	}

	readErrCh := make(chan error, 1)
	go func() {
		_, _, err := input.Read(ctx)
		readErrCh <- err
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-readErrCh:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("Read did not exit after context cancellation")
	}
}

func TestGcpSpannerCDCInput_ReadMultipleMessages(t *testing.T) {
	ctx := context.Background()
	_, dsn, cleanup := setupTestSpanner(t)
	defer cleanup()

	client, err := spanner.NewClient(ctx, dsn)
	require.NoError(t, err)
	defer client.Close()

	input := &gcpSpannerCDCInput{
		streamClient: client,
		recordsCh:    make(chan changeRecord, 3),
		shutdownSig:  shutdown.NewSignaller(),
	}

	tables := []string{"users", "orders", "products"}
	for i, tableName := range tables {
		now := time.Now().UTC().Add(time.Duration(i) * time.Second)
		record := changeRecord{
			data: &types.DataChangeRecord{
				CommitTimestamp:     types.TimeMills(now),
				RecordSequence:      fmt.Sprintf("seq-%d", i),
				ServerTransactionId: uuid.NewString(),
				TableName:           tableName,
				ModType:             "INSERT",
			},
			mod: &types.Mod{
				Keys: spanner.NullJSON{Valid: true, Value: map[string]interface{}{"id": fmt.Sprintf("%d", i)}},
			},
			modType: "INSERT",
		}
		input.recordsCh <- record
	}

	for range tables {
		msg, ackFunc, err := input.Read(ctx)
		require.NoError(t, err)

		tableName, found := msg.MetaGetMut(metadataTableName)
		require.True(t, found)
		assert.Contains(t, []string{"users", "orders", "products"}, tableName.(string))

		err = ackFunc(ctx, nil)
		require.NoError(t, err)
	}
}

// Helper function to parse time strings into time pointers
func parseTimePtr(t *testing.T, timeStr string) *time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, timeStr)
	require.NoError(t, err)
	return &parsed
}
