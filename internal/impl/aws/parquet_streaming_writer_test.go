package aws

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStreamingParquetWriter_BasicFunctionality tests the core writer without S3
func TestStreamingParquetWriter_BasicFunctionality(t *testing.T) {
	// Define a simple test schema
	type TestRecord struct {
		ID    int64   `parquet:"id"`
		Name  string  `parquet:"name"`
		Value float64 `parquet:"value"`
	}

	schema := parquet.SchemaOf(new(TestRecord))
	messageType := reflect.TypeOf(TestRecord{})

	// Create test events
	testEvents := []map[string]any{
		{"id": int64(1), "name": "Alice", "value": 10.5},
		{"id": int64(2), "name": "Bob", "value": 20.3},
		{"id": int64(3), "name": "Charlie", "value": 15.7},
	}

	t.Run("row buffering", func(t *testing.T) {
		rowGroupSize := int64(10) // Large enough to not flush

		writer := &StreamingParquetWriter{
			rowBuffer: make([]any, 0, rowGroupSize),
		}

		// Write events
		for _, event := range testEvents {
			v := reflect.New(messageType)
			err := mapToStruct(event, v.Interface())
			require.NoError(t, err)

			writer.rowBuffer = append(writer.rowBuffer, v.Interface())
		}

		// Verify buffering
		assert.Len(t, writer.rowBuffer, 3)
		assert.Equal(t, int64(0), writer.totalRows) // Not yet flushed
	})

	t.Run("row group flushing", func(t *testing.T) {
		config := StreamingWriterConfig{
			Schema:          schema,
			MessageType:     messageType,
			CompressionType: &parquet.Snappy,
			RowGroupSize:    2, // Small to trigger flush
		}

		writer := &StreamingParquetWriter{
			schema:          config.Schema,
			messageType:     config.MessageType,
			compressionType: config.CompressionType,
			rowGroupSize:    config.RowGroupSize,
			uploadBuffer:    bytes.NewBuffer(nil),
			rowBuffer:       make([]any, 0, config.RowGroupSize),
			rowGroupsMeta:   []RowGroupMetadata{},
		}

		// Add events
		for _, event := range testEvents[:2] {
			v := reflect.New(messageType)
			err := mapToStruct(event, v.Interface())
			require.NoError(t, err)
			writer.rowBuffer = append(writer.rowBuffer, v.Interface())
		}

		// Flush manually
		err := writer.flushRowGroup(context.Background())
		require.NoError(t, err)

		// Verify flush occurred
		assert.Empty(t, writer.rowBuffer) // Buffer cleared
		assert.Equal(t, int64(2), writer.totalRows)
		assert.Len(t, writer.rowGroupsMeta, 1)
		assert.Greater(t, writer.uploadSize, int64(0))
	})

	t.Run("footer generation", func(t *testing.T) {
		config := StreamingWriterConfig{
			Schema:          schema,
			MessageType:     messageType,
			CompressionType: &parquet.Snappy,
			RowGroupSize:    10,
		}

		writer := &StreamingParquetWriter{
			schema:          config.Schema,
			messageType:     config.MessageType,
			compressionType: config.CompressionType,
			rowGroupSize:    config.RowGroupSize,
			uploadBuffer:    bytes.NewBuffer(nil),
			rowBuffer:       make([]any, 0, config.RowGroupSize),
			totalRows:       100,
			rowGroupsMeta: []RowGroupMetadata{
				{NumRows: 50, TotalByteSize: 1024, FileOffset: 4},
				{NumRows: 50, TotalByteSize: 1024, FileOffset: 1028},
			},
		}

		// Generate footer
		footer, err := writer.generateFooter()
		require.NoError(t, err)

		// Verify footer structure
		assert.NotEmpty(t, footer, "Footer should have content")

		// Footer should end with 4 bytes for length + 4 bytes "PAR1" magic
		// But generateFooter only returns footer + length, not the final "PAR1"
		assert.Greater(t, len(footer), 8, "Footer should be substantial")
	})
}

// TestStreamingParquetWriter_MapToStruct tests the map conversion
func TestStreamingParquetWriter_MapToStruct(t *testing.T) {
	type TestRecord struct {
		ID    int64   `parquet:"id" json:"ID"`
		Name  string  `parquet:"name" json:"Name"`
		Value float64 `parquet:"value" json:"Value"`
	}

	testCases := []struct {
		name     string
		input    map[string]any
		expected TestRecord
		wantErr  bool
	}{
		{
			name: "valid conversion",
			input: map[string]any{
				"ID":    int64(42),
				"Name":  "test",
				"Value": float64(3.14),
			},
			expected: TestRecord{ID: 42, Name: "test", Value: 3.14},
			wantErr:  false,
		},
		{
			name: "missing optional fields",
			input: map[string]any{
				"ID": int64(1),
			},
			expected: TestRecord{ID: 1, Name: "", Value: 0.0},
			wantErr:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var result TestRecord
			err := mapToStruct(tc.input, &result)

			if tc.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

// TestStreamingParquetWriter_Stats tests writer statistics
func TestStreamingParquetWriter_Stats(t *testing.T) {
	config := StreamingWriterConfig{
		Schema: parquet.SchemaOf(new(struct {
			ID int64 `parquet:"id"`
		})),
		MessageType: reflect.TypeOf(struct {
			ID int64 `parquet:"id"`
		}{}),
		CompressionType: &parquet.Snappy,
		RowGroupSize:    10,
	}

	writer := &StreamingParquetWriter{
		schema:          config.Schema,
		messageType:     config.MessageType,
		compressionType: config.CompressionType,
		rowGroupSize:    config.RowGroupSize,
		uploadBuffer:    bytes.NewBuffer(nil),
		rowBuffer:       make([]any, 0, config.RowGroupSize),
		totalRows:       150,
		rowGroupsMeta:   []RowGroupMetadata{{}, {}, {}},
		uploadSize:      5242880, // 5 MB
	}

	// Add some buffered rows
	writer.rowBuffer = append(writer.rowBuffer, &struct {
		ID int64 `parquet:"id"`
	}{ID: 1})
	writer.rowBuffer = append(writer.rowBuffer, &struct {
		ID int64 `parquet:"id"`
	}{ID: 2})

	stats := writer.Stats()

	assert.Equal(t, int64(150), stats.TotalRows)
	assert.Equal(t, 3, stats.RowGroups)
	assert.Equal(t, 2, stats.BufferedRows)
	assert.Equal(t, int64(5242880), stats.BufferedBytes)
}

// S3 Multipart Upload Lifecycle Tests (with mock S3 client)

// mockS3Client is a mock S3 client for testing S3 operations
type mockS3Client struct {
	createMultipartUploadFunc   func(ctx context.Context, input *s3.CreateMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	uploadPartFunc              func(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	completeMultipartUploadFunc func(ctx context.Context, input *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	abortMultipartUploadFunc    func(ctx context.Context, input *s3.AbortMultipartUploadInput, opts ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
}

func (m *mockS3Client) CreateMultipartUpload(ctx context.Context, input *s3.CreateMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	if m.createMultipartUploadFunc != nil {
		return m.createMultipartUploadFunc(ctx, input, opts...)
	}
	return &s3.CreateMultipartUploadOutput{
		UploadId: aws.String("test-upload-id"),
	}, nil
}

func (m *mockS3Client) UploadPart(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	if m.uploadPartFunc != nil {
		return m.uploadPartFunc(ctx, input, opts...)
	}
	return &s3.UploadPartOutput{
		ETag: aws.String("test-etag"),
	}, nil
}

func (m *mockS3Client) CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	if m.completeMultipartUploadFunc != nil {
		return m.completeMultipartUploadFunc(ctx, input, opts...)
	}
	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (m *mockS3Client) AbortMultipartUpload(ctx context.Context, input *s3.AbortMultipartUploadInput, opts ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	if m.abortMultipartUploadFunc != nil {
		return m.abortMultipartUploadFunc(ctx, input, opts...)
	}
	return &s3.AbortMultipartUploadOutput{}, nil
}

// TestParquetStreamingWriter_Initialize tests multipart upload initialization
func TestParquetStreamingWriter_Initialize(t *testing.T) {
	type TestRecord struct {
		ID int64 `parquet:"id"`
	}

	mockClient := &mockS3Client{}
	schema := parquet.SchemaOf(new(TestRecord))
	messageType := reflect.TypeOf(TestRecord{})

	config := StreamingWriterConfig{
		S3Client:        mockClient,
		Bucket:          "test-bucket",
		Key:             "test-key.parquet",
		Schema:          schema,
		MessageType:     messageType,
		CompressionType: &parquet.Snappy,
		RowGroupSize:    1000,
	}

	writer, err := NewStreamingParquetWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	assert.NotNil(t, writer.uploadID)
	assert.Equal(t, "test-upload-id", *writer.uploadID)
	assert.Equal(t, int64(4), writer.uploadSize) // "PAR1" header
}

// TestParquetStreamingWriter_DoubleInitialize tests that double initialization is rejected
func TestParquetStreamingWriter_DoubleInitialize(t *testing.T) {
	type TestRecord struct {
		ID int64 `parquet:"id"`
	}

	mockClient := &mockS3Client{}
	schema := parquet.SchemaOf(new(TestRecord))
	messageType := reflect.TypeOf(TestRecord{})

	config := StreamingWriterConfig{
		S3Client:        mockClient,
		Bucket:          "test-bucket",
		Key:             "test-key.parquet",
		Schema:          schema,
		MessageType:     messageType,
		CompressionType: &parquet.Snappy,
		RowGroupSize:    1000,
	}

	writer, err := NewStreamingParquetWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	// Second initialize should error
	err = writer.Initialize(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already initialized")
}

// TestParquetStreamingWriter_WriteBeforeInitialize tests that writing before initialization fails
func TestParquetStreamingWriter_WriteBeforeInitialize(t *testing.T) {
	type TestRecord struct {
		ID int64 `parquet:"id"`
	}

	mockClient := &mockS3Client{}
	schema := parquet.SchemaOf(new(TestRecord))
	messageType := reflect.TypeOf(TestRecord{})

	config := StreamingWriterConfig{
		S3Client:        mockClient,
		Bucket:          "test-bucket",
		Key:             "test-key.parquet",
		Schema:          schema,
		MessageType:     messageType,
		CompressionType: &parquet.Snappy,
		RowGroupSize:    1000,
	}

	writer, err := NewStreamingParquetWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	event := map[string]any{"ID": int64(1)}
	err = writer.WriteEvent(ctx, event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not initialized")
}

// TestParquetStreamingWriter_WriteAfterClose tests that writing after close fails
func TestParquetStreamingWriter_WriteAfterClose(t *testing.T) {
	type TestRecord struct {
		ID int64 `parquet:"id"`
	}

	mockClient := &mockS3Client{}
	schema := parquet.SchemaOf(new(TestRecord))
	messageType := reflect.TypeOf(TestRecord{})

	config := StreamingWriterConfig{
		S3Client:        mockClient,
		Bucket:          "test-bucket",
		Key:             "test-key.parquet",
		Schema:          schema,
		MessageType:     messageType,
		CompressionType: &parquet.Snappy,
		RowGroupSize:    1000,
	}

	writer, err := NewStreamingParquetWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	err = writer.Close(ctx)
	require.NoError(t, err)

	// Write after close should error
	event := map[string]any{"ID": int64(1)}
	err = writer.WriteEvent(ctx, event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

// TestParquetStreamingWriter_PartUploadTracking tests that parts are tracked correctly when uploaded
func TestParquetStreamingWriter_PartUploadTracking(t *testing.T) {
	type TestRecord struct {
		ID int64 `parquet:"id"`
	}

	uploadedParts := make([]int32, 0)
	mockClient := &mockS3Client{
		uploadPartFunc: func(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
			uploadedParts = append(uploadedParts, *input.PartNumber)
			return &s3.UploadPartOutput{
				ETag: aws.String("test-etag"),
			}, nil
		},
	}

	schema := parquet.SchemaOf(new(TestRecord))
	messageType := reflect.TypeOf(TestRecord{})

	config := StreamingWriterConfig{
		S3Client:        mockClient,
		Bucket:          "test-bucket",
		Key:             "test-key.parquet",
		Schema:          schema,
		MessageType:     messageType,
		CompressionType: &parquet.Snappy,
		RowGroupSize:    10,
	}

	writer, err := NewStreamingParquetWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	// Write a few events
	for i := 0; i < 5; i++ {
		event := map[string]any{"ID": int64(i)}
		err = writer.WriteEvent(ctx, event)
		require.NoError(t, err)
	}

	// Close to finalize
	err = writer.Close(ctx)
	require.NoError(t, err)

	// At least the final part should have been uploaded during Close
	assert.NotEmpty(t, uploadedParts)
}

// TestParquetStreamingWriter_CompleteMultipartUpload tests that CompleteMultipartUpload is called on Close
func TestParquetStreamingWriter_CompleteMultipartUpload(t *testing.T) {
	type TestRecord struct {
		ID int64 `parquet:"id"`
	}

	completeCalled := false
	var capturedParts []types.CompletedPart

	mockClient := &mockS3Client{
		completeMultipartUploadFunc: func(ctx context.Context, input *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
			completeCalled = true
			capturedParts = input.MultipartUpload.Parts
			return &s3.CompleteMultipartUploadOutput{}, nil
		},
	}

	schema := parquet.SchemaOf(new(TestRecord))
	messageType := reflect.TypeOf(TestRecord{})

	config := StreamingWriterConfig{
		S3Client:        mockClient,
		Bucket:          "test-bucket",
		Key:             "test-key.parquet",
		Schema:          schema,
		MessageType:     messageType,
		CompressionType: &parquet.Snappy,
		RowGroupSize:    10,
	}

	writer, err := NewStreamingParquetWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	// Write a few events
	for i := 0; i < 5; i++ {
		event := map[string]any{"ID": int64(i)}
		err = writer.WriteEvent(ctx, event)
		require.NoError(t, err)
	}

	// Close should complete the upload
	err = writer.Close(ctx)
	require.NoError(t, err)

	assert.True(t, completeCalled)
	assert.NotEmpty(t, capturedParts)
	assert.True(t, writer.closed)
}

// TestParquetStreamingWriter_MultiplePartsWithCorrectNumbering tests that multiple parts are uploaded with correct part numbers
func TestParquetStreamingWriter_MultiplePartsWithCorrectNumbering(t *testing.T) {
	type TestRecord struct {
		ID   int64  `parquet:"id"`
		Data string `parquet:"data"`
	}

	uploadedParts := make([]int32, 0)
	var completedParts []types.CompletedPart

	mockClient := &mockS3Client{
		uploadPartFunc: func(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
			uploadedParts = append(uploadedParts, *input.PartNumber)
			return &s3.UploadPartOutput{
				ETag: aws.String("test-etag-" + fmt.Sprint(*input.PartNumber)),
			}, nil
		},
		completeMultipartUploadFunc: func(ctx context.Context, input *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
			completedParts = input.MultipartUpload.Parts
			return &s3.CompleteMultipartUploadOutput{}, nil
		},
	}

	schema := parquet.SchemaOf(new(TestRecord))
	messageType := reflect.TypeOf(TestRecord{})

	config := StreamingWriterConfig{
		S3Client:        mockClient,
		Bucket:          "test-bucket",
		Key:             "test-key.parquet",
		Schema:          schema,
		MessageType:     messageType,
		CompressionType: &parquet.Uncompressed,
		RowGroupSize:    50000, // Large enough to trigger multiple 5MB uploads
	}

	writer, err := NewStreamingParquetWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	// Write enough data to create multiple parts (>10MB total)
	for i := 0; i < 120000; i++ {
		event := map[string]any{
			"ID":   int64(i),
			"Data": "This is test data that adds some size to the row for testing.",
		}
		err = writer.WriteEvent(ctx, event)
		require.NoError(t, err)
	}

	err = writer.Close(ctx)
	require.NoError(t, err)

	// Should have uploaded multiple parts
	assert.Greater(t, len(uploadedParts), 1)

	// Verify part numbers are sequential starting from 1
	for i, partNum := range uploadedParts {
		assert.Equal(t, int32(i+1), partNum)
	}

	// Verify completed parts match uploaded parts
	assert.Len(t, completedParts, len(uploadedParts))
}

// TestParquetStreamingWriter_AbortOnCloseError tests that AbortMultipartUpload is called when Close encounters errors
func TestParquetStreamingWriter_AbortOnCloseError(t *testing.T) {
	type TestRecord struct {
		ID int64 `parquet:"id"`
	}

	abortCalled := false
	uploadAttempts := 0

	mockClient := &mockS3Client{
		uploadPartFunc: func(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
			uploadAttempts++
			// Simulate persistent upload failure
			return nil, fmt.Errorf("simulated S3 upload failure")
		},
		abortMultipartUploadFunc: func(ctx context.Context, input *s3.AbortMultipartUploadInput, opts ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
			abortCalled = true
			return &s3.AbortMultipartUploadOutput{}, nil
		},
	}

	schema := parquet.SchemaOf(new(TestRecord))
	messageType := reflect.TypeOf(TestRecord{})

	config := StreamingWriterConfig{
		S3Client:        mockClient,
		Bucket:          "test-bucket",
		Key:             "test-key.parquet",
		Schema:          schema,
		MessageType:     messageType,
		CompressionType: &parquet.Snappy,
		RowGroupSize:    10,
	}

	writer, err := NewStreamingParquetWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	// Write a few events
	for i := 0; i < 5; i++ {
		event := map[string]any{"ID": int64(i)}
		err = writer.WriteEvent(ctx, event)
		require.NoError(t, err)
	}

	// Close will attempt to upload the final part and fail
	err = writer.Close(ctx)
	require.Error(t, err)

	// Should have attempted upload 3 times (with retries)
	assert.Equal(t, 3, uploadAttempts)
	assert.True(t, abortCalled)
}
