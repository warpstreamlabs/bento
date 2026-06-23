package aws

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockS3StreamClient is a mock S3 client for testing
type mockS3StreamClient struct {
	createMultipartUploadFunc   func(ctx context.Context, input *s3.CreateMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	uploadPartFunc              func(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	completeMultipartUploadFunc func(ctx context.Context, input *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	abortMultipartUploadFunc    func(ctx context.Context, input *s3.AbortMultipartUploadInput, opts ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
	putObjectFunc               func(ctx context.Context, input *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

func (m *mockS3StreamClient) CreateMultipartUpload(ctx context.Context, input *s3.CreateMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	if m.createMultipartUploadFunc != nil {
		return m.createMultipartUploadFunc(ctx, input, opts...)
	}
	return &s3.CreateMultipartUploadOutput{
		UploadId: aws.String("test-upload-id"),
	}, nil
}

func (m *mockS3StreamClient) UploadPart(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	if m.uploadPartFunc != nil {
		return m.uploadPartFunc(ctx, input, opts...)
	}
	return &s3.UploadPartOutput{
		ETag: aws.String("test-etag"),
	}, nil
}

func (m *mockS3StreamClient) CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	if m.completeMultipartUploadFunc != nil {
		return m.completeMultipartUploadFunc(ctx, input, opts...)
	}
	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (m *mockS3StreamClient) AbortMultipartUpload(ctx context.Context, input *s3.AbortMultipartUploadInput, opts ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	if m.abortMultipartUploadFunc != nil {
		return m.abortMultipartUploadFunc(ctx, input, opts...)
	}
	return &s3.AbortMultipartUploadOutput{}, nil
}

func (m *mockS3StreamClient) PutObject(ctx context.Context, input *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if m.putObjectFunc != nil {
		return m.putObjectFunc(ctx, input, opts...)
	}
	return &s3.PutObjectOutput{}, nil
}

func TestS3StreamingWriterCreation(t *testing.T) {
	mockClient := &mockS3StreamClient{}

	config := S3StreamingWriterConfig{
		S3Client:        mockClient,
		Bucket:          "test-bucket",
		Key:             "test-key",
		MaxBufferBytes:  5 * 1024 * 1024,
		MaxBufferCount:  1000,
		MaxBufferPeriod: 5 * time.Second,
		ContentType:     "application/json",
	}

	writer, err := NewS3StreamingWriter(config)
	require.NoError(t, err)
	require.NotNil(t, writer)

	assert.Equal(t, int64(5*1024*1024), writer.maxBufferBytes)
	assert.Equal(t, 1000, writer.maxBufferCount)
	assert.Equal(t, 5*time.Second, writer.maxBufferPeriod)
	assert.Equal(t, "application/json", writer.contentType)
}

func TestS3StreamingWriterDefaults(t *testing.T) {
	mockClient := &mockS3StreamClient{}

	config := S3StreamingWriterConfig{
		S3Client: mockClient,
		Bucket:   "test-bucket",
		Key:      "test-key",
	}

	writer, err := NewS3StreamingWriter(config)
	require.NoError(t, err)

	// Check defaults
	assert.Equal(t, int64(10*1024*1024), writer.maxBufferBytes)
	assert.Equal(t, 10000, writer.maxBufferCount)
	assert.Equal(t, 10*time.Second, writer.maxBufferPeriod)
	assert.Equal(t, "application/octet-stream", writer.contentType)
}

func TestS3StreamingWriterInitialize(t *testing.T) {
	mockClient := &mockS3StreamClient{}

	config := S3StreamingWriterConfig{
		S3Client:        mockClient,
		Bucket:          "test-bucket",
		Key:             "test-key",
		ContentType:     "application/json",
		ContentEncoding: "gzip",
	}

	writer, err := NewS3StreamingWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	assert.NotNil(t, writer.uploadID)
	assert.Equal(t, "test-upload-id", *writer.uploadID)
}

func TestS3StreamingWriterDoubleInitialize(t *testing.T) {
	mockClient := &mockS3StreamClient{}

	config := S3StreamingWriterConfig{
		S3Client: mockClient,
		Bucket:   "test-bucket",
		Key:      "test-key",
	}

	writer, err := NewS3StreamingWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	// Second initialize should error
	err = writer.Initialize(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already initialized")
}

func TestS3StreamingWriterWriteBeforeInitialize(t *testing.T) {
	mockClient := &mockS3StreamClient{}

	config := S3StreamingWriterConfig{
		S3Client: mockClient,
		Bucket:   "test-bucket",
		Key:      "test-key",
	}

	writer, err := NewS3StreamingWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.WriteBytes(ctx, []byte("test data"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not initialized")
}

func TestS3StreamingWriterBufferFlushOnSize(t *testing.T) {
	uploadedParts := make([]int32, 0)
	mockClient := &mockS3StreamClient{
		uploadPartFunc: func(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
			uploadedParts = append(uploadedParts, *input.PartNumber)
			return &s3.UploadPartOutput{
				ETag: aws.String("test-etag"),
			}, nil
		},
	}

	config := S3StreamingWriterConfig{
		S3Client:       mockClient,
		Bucket:         "test-bucket",
		Key:            "test-key",
		MaxBufferBytes: 5 * 1024 * 1024, // 5MB
		MaxBufferCount: 10000,
	}

	writer, err := NewS3StreamingWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	// Write 6MB of data (should trigger flush)
	data := make([]byte, 6*1024*1024)
	err = writer.WriteBytes(ctx, data)
	require.NoError(t, err)

	// Should have uploaded 1 part
	assert.Len(t, uploadedParts, 1)
	assert.Equal(t, int32(1), uploadedParts[0])
}

func TestS3StreamingWriterBufferFlushOnCount(t *testing.T) {
	uploadedParts := make([]int32, 0)
	mockClient := &mockS3StreamClient{
		uploadPartFunc: func(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
			uploadedParts = append(uploadedParts, *input.PartNumber)
			return &s3.UploadPartOutput{
				ETag: aws.String("test-etag"),
			}, nil
		},
	}

	config := S3StreamingWriterConfig{
		S3Client:       mockClient,
		Bucket:         "test-bucket",
		Key:            "test-key",
		MaxBufferBytes: 100 * 1024 * 1024, // Large so count triggers first
		MaxBufferCount: 100,
	}

	writer, err := NewS3StreamingWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	// Write 100 messages with 100KB each = 10MB total (should trigger on count)
	data := make([]byte, 100*1024)
	for range 100 {
		err = writer.WriteBytes(ctx, data)
		require.NoError(t, err)
	}

	// Should have uploaded 1 part (triggered by count)
	assert.Len(t, uploadedParts, 1)
}

func TestS3StreamingWriterClose(t *testing.T) {
	completeCalled := false
	putObjectCalled := false
	mockClient := &mockS3StreamClient{
		completeMultipartUploadFunc: func(ctx context.Context, input *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
			completeCalled = true
			return &s3.CompleteMultipartUploadOutput{}, nil
		},
		putObjectFunc: func(ctx context.Context, input *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			putObjectCalled = true
			return &s3.PutObjectOutput{}, nil
		},
	}

	config := S3StreamingWriterConfig{
		S3Client: mockClient,
		Bucket:   "test-bucket",
		Key:      "test-key",
	}

	writer, err := NewS3StreamingWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	// Write some data (small amount, < 5 MiB)
	err = writer.WriteBytes(ctx, []byte("test data"))
	require.NoError(t, err)

	// Close should use PutObject for small files
	err = writer.Close(ctx)
	require.NoError(t, err)
	assert.True(t, putObjectCalled, "PutObject should be called for small data")
	assert.False(t, completeCalled, "CompleteMultipartUpload should not be called for small data")
	assert.True(t, writer.closed)
}

func TestS3StreamingWriterWriteAfterClose(t *testing.T) {
	mockClient := &mockS3StreamClient{}

	config := S3StreamingWriterConfig{
		S3Client: mockClient,
		Bucket:   "test-bucket",
		Key:      "test-key",
	}

	writer, err := NewS3StreamingWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	err = writer.Close(ctx)
	require.NoError(t, err)

	// Write after close should error
	err = writer.WriteBytes(ctx, []byte("test"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestS3StreamingWriterStats(t *testing.T) {
	uploadedData := bytes.NewBuffer(nil)
	mockClient := &mockS3StreamClient{
		uploadPartFunc: func(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
			// Capture uploaded data
			data := make([]byte, 6*1024*1024)
			n, _ := input.Body.Read(data)
			uploadedData.Write(data[:n])
			return &s3.UploadPartOutput{
				ETag: aws.String("test-etag"),
			}, nil
		},
	}

	config := S3StreamingWriterConfig{
		S3Client:       mockClient,
		Bucket:         "test-bucket",
		Key:            "test-key",
		MaxBufferBytes: 5 * 1024 * 1024,
	}

	writer, err := NewS3StreamingWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	// Write 6MB to trigger flush
	data := make([]byte, 6*1024*1024)
	err = writer.WriteBytes(ctx, data)
	require.NoError(t, err)

	stats := writer.Stats()
	assert.Equal(t, int64(1), stats.TotalMessages)
	assert.Equal(t, int64(6*1024*1024), stats.TotalBytes)
	assert.Equal(t, int32(1), stats.PartsUploaded)
}

func TestS3StreamingWriterMultipleParts(t *testing.T) {
	uploadedParts := make([]types.CompletedPart, 0)
	mockClient := &mockS3StreamClient{
		uploadPartFunc: func(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
			return &s3.UploadPartOutput{
				ETag: aws.String("test-etag"),
			}, nil
		},
		completeMultipartUploadFunc: func(ctx context.Context, input *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
			uploadedParts = input.MultipartUpload.Parts
			return &s3.CompleteMultipartUploadOutput{}, nil
		},
	}

	config := S3StreamingWriterConfig{
		S3Client:       mockClient,
		Bucket:         "test-bucket",
		Key:            "test-key",
		MaxBufferBytes: 5 * 1024 * 1024,
	}

	writer, err := NewS3StreamingWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	// Write 6MB three times (should create 3 parts)
	data := make([]byte, 6*1024*1024)
	for range 3 {
		err = writer.WriteBytes(ctx, data)
		require.NoError(t, err)
	}

	// Close and verify 3 parts were uploaded
	err = writer.Close(ctx)
	require.NoError(t, err)

	assert.Len(t, uploadedParts, 3)
	assert.Equal(t, int32(1), *uploadedParts[0].PartNumber)
	assert.Equal(t, int32(2), *uploadedParts[1].PartNumber)
	assert.Equal(t, int32(3), *uploadedParts[2].PartNumber)
}

func TestS3StreamingWriterContentEncoding(t *testing.T) {
	var capturedInput *s3.CreateMultipartUploadInput
	mockClient := &mockS3StreamClient{
		createMultipartUploadFunc: func(ctx context.Context, input *s3.CreateMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
			capturedInput = input
			return &s3.CreateMultipartUploadOutput{
				UploadId: aws.String("test-upload-id"),
			}, nil
		},
	}

	config := S3StreamingWriterConfig{
		S3Client:        mockClient,
		Bucket:          "test-bucket",
		Key:             "test-key.gz",
		ContentType:     "application/json",
		ContentEncoding: "gzip",
	}

	writer, err := NewS3StreamingWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	// Verify content encoding was set
	require.NotNil(t, capturedInput)
	assert.Equal(t, "application/json", *capturedInput.ContentType)
	assert.Equal(t, "gzip", *capturedInput.ContentEncoding)
}

// TestS3StreamingWriterSmallFile tests that small files (< 5 MiB) use PutObject
func TestS3StreamingWriterSmallFile(t *testing.T) {
	abortCalled := false
	putObjectCalled := false
	completeMultipartCalled := false
	var capturedPutData []byte
	var capturedPutInput *s3.PutObjectInput

	mockClient := &mockS3StreamClient{
		abortMultipartUploadFunc: func(ctx context.Context, input *s3.AbortMultipartUploadInput, opts ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
			abortCalled = true
			return &s3.AbortMultipartUploadOutput{}, nil
		},
		putObjectFunc: func(ctx context.Context, input *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			putObjectCalled = true
			capturedPutInput = input
			// Read the body
			data := make([]byte, 1024*1024)
			n, _ := input.Body.Read(data)
			capturedPutData = data[:n]
			return &s3.PutObjectOutput{}, nil
		},
		completeMultipartUploadFunc: func(ctx context.Context, input *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
			completeMultipartCalled = true
			return &s3.CompleteMultipartUploadOutput{}, nil
		},
	}

	config := S3StreamingWriterConfig{
		S3Client:        mockClient,
		Bucket:          "test-bucket",
		Key:             "test-key",
		ContentType:     "application/json",
		ContentEncoding: "gzip",
		MaxBufferBytes:  10 * 1024 * 1024, // 10MB buffer
	}

	writer, err := NewS3StreamingWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	// Write small amount of data (1 KB)
	testData := []byte("small test data")
	err = writer.WriteBytes(ctx, testData)
	require.NoError(t, err)

	// Close should use PutObject, not multipart
	err = writer.Close(ctx)
	require.NoError(t, err)

	// Verify behavior
	assert.True(t, abortCalled, "multipart upload should be aborted")
	assert.True(t, putObjectCalled, "PutObject should be called for small files")
	assert.False(t, completeMultipartCalled, "CompleteMultipartUpload should NOT be called")

	// Verify the data was uploaded correctly
	assert.Equal(t, testData, capturedPutData)

	// Verify content type and encoding were set
	require.NotNil(t, capturedPutInput)
	assert.Equal(t, "application/json", *capturedPutInput.ContentType)
	assert.Equal(t, "gzip", *capturedPutInput.ContentEncoding)
	assert.Equal(t, "test-bucket", *capturedPutInput.Bucket)
	assert.Equal(t, "test-key", *capturedPutInput.Key)
}

// TestS3StreamingWriterSmallFileMultipleWrites tests small file with multiple writes
func TestS3StreamingWriterSmallFileMultipleWrites(t *testing.T) {
	putObjectCalled := false
	var capturedPutData []byte

	mockClient := &mockS3StreamClient{
		putObjectFunc: func(ctx context.Context, input *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			putObjectCalled = true
			// Read all data
			data := make([]byte, 1024*1024)
			n, _ := input.Body.Read(data)
			capturedPutData = data[:n]
			return &s3.PutObjectOutput{}, nil
		},
	}

	config := S3StreamingWriterConfig{
		S3Client:       mockClient,
		Bucket:         "test-bucket",
		Key:            "test-key",
		MaxBufferBytes: 10 * 1024 * 1024, // 10MB buffer
	}

	writer, err := NewS3StreamingWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	// Write multiple small chunks (total < 5 MiB)
	chunk1 := []byte("chunk1")
	chunk2 := []byte("chunk2")
	chunk3 := []byte("chunk3")

	err = writer.WriteBytes(ctx, chunk1)
	require.NoError(t, err)
	err = writer.WriteBytes(ctx, chunk2)
	require.NoError(t, err)
	err = writer.WriteBytes(ctx, chunk3)
	require.NoError(t, err)

	// Close should use PutObject
	err = writer.Close(ctx)
	require.NoError(t, err)

	assert.True(t, putObjectCalled)
	// Verify all chunks were combined
	expected := append(append(chunk1, chunk2...), chunk3...)
	assert.Equal(t, expected, capturedPutData)
}

// TestS3StreamingWriterExactly5MB tests the boundary at exactly 5 MiB
func TestS3StreamingWriterExactly5MB(t *testing.T) {
	uploadPartCalled := false
	completeMultipartCalled := false
	putObjectCalled := false

	mockClient := &mockS3StreamClient{
		uploadPartFunc: func(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
			uploadPartCalled = true
			return &s3.UploadPartOutput{
				ETag: aws.String("test-etag"),
			}, nil
		},
		completeMultipartUploadFunc: func(ctx context.Context, input *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
			completeMultipartCalled = true
			return &s3.CompleteMultipartUploadOutput{}, nil
		},
		putObjectFunc: func(ctx context.Context, input *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			putObjectCalled = true
			return &s3.PutObjectOutput{}, nil
		},
	}

	config := S3StreamingWriterConfig{
		S3Client:       mockClient,
		Bucket:         "test-bucket",
		Key:            "test-key",
		MaxBufferBytes: 5 * 1024 * 1024, // Exactly 5 MiB
	}

	writer, err := NewS3StreamingWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	// Write exactly 5 MiB
	data := make([]byte, 5*1024*1024)
	err = writer.WriteBytes(ctx, data)
	require.NoError(t, err)

	// Close - should complete multipart (not use PutObject)
	err = writer.Close(ctx)
	require.NoError(t, err)

	// At 5 MiB exactly, it should trigger multipart upload
	assert.True(t, uploadPartCalled, "UploadPart should be called at 5 MiB")
	assert.True(t, completeMultipartCalled, "CompleteMultipartUpload should be called")
	assert.False(t, putObjectCalled, "PutObject should NOT be called")
}

// TestS3StreamingWriterJustUnder5MB tests just under 5 MiB boundary
func TestS3StreamingWriterJustUnder5MB(t *testing.T) {
	uploadPartCalled := false
	putObjectCalled := false

	mockClient := &mockS3StreamClient{
		uploadPartFunc: func(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
			uploadPartCalled = true
			return &s3.UploadPartOutput{
				ETag: aws.String("test-etag"),
			}, nil
		},
		putObjectFunc: func(ctx context.Context, input *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			putObjectCalled = true
			return &s3.PutObjectOutput{}, nil
		},
	}

	config := S3StreamingWriterConfig{
		S3Client:       mockClient,
		Bucket:         "test-bucket",
		Key:            "test-key",
		MaxBufferBytes: 10 * 1024 * 1024, // 10 MiB buffer
	}

	writer, err := NewS3StreamingWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	// Write just under 5 MiB (5 MiB - 1 byte)
	data := make([]byte, 5*1024*1024-1)
	err = writer.WriteBytes(ctx, data)
	require.NoError(t, err)

	// Close should use PutObject
	err = writer.Close(ctx)
	require.NoError(t, err)

	assert.False(t, uploadPartCalled, "UploadPart should NOT be called for < 5 MiB")
	assert.True(t, putObjectCalled, "PutObject should be called for < 5 MiB")
}

// TestS3StreamingWriterEmptyFile tests closing without writing any data
func TestS3StreamingWriterEmptyFile(t *testing.T) {
	abortCalled := false
	putObjectCalled := false
	var capturedPutData []byte

	mockClient := &mockS3StreamClient{
		abortMultipartUploadFunc: func(ctx context.Context, input *s3.AbortMultipartUploadInput, opts ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
			abortCalled = true
			return &s3.AbortMultipartUploadOutput{}, nil
		},
		putObjectFunc: func(ctx context.Context, input *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			putObjectCalled = true
			data := make([]byte, 1024)
			n, _ := input.Body.Read(data)
			capturedPutData = data[:n]
			return &s3.PutObjectOutput{}, nil
		},
	}

	config := S3StreamingWriterConfig{
		S3Client: mockClient,
		Bucket:   "test-bucket",
		Key:      "test-key",
	}

	writer, err := NewS3StreamingWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	// Close without writing any data
	err = writer.Close(ctx)
	require.NoError(t, err)

	assert.True(t, abortCalled, "multipart should be aborted")
	assert.True(t, putObjectCalled, "PutObject should be called for empty file")
	assert.Empty(t, capturedPutData, "uploaded data should be empty")
}

// Additional tests for timer-based flush fix

// TestS3StreamingWriterTimerFlushSmallData tests that timer-based flush
// does NOT upload parts < 5MB (waits for Close() to use PutObject)
func TestS3StreamingWriterTimerFlushSmallData(t *testing.T) {
	uploadPartCalled := false
	putObjectCalled := false
	abortCalled := false
	var capturedPutData []byte

	mockClient := &mockS3StreamClient{
		uploadPartFunc: func(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
			uploadPartCalled = true
			return &s3.UploadPartOutput{
				ETag: aws.String("test-etag"),
			}, nil
		},
		abortMultipartUploadFunc: func(ctx context.Context, input *s3.AbortMultipartUploadInput, opts ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
			abortCalled = true
			return &s3.AbortMultipartUploadOutput{}, nil
		},
		putObjectFunc: func(ctx context.Context, input *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			putObjectCalled = true
			data := make([]byte, 10*1024*1024) // Large enough buffer
			n, _ := input.Body.Read(data)
			capturedPutData = data[:n]
			return &s3.PutObjectOutput{}, nil
		},
	}

	config := S3StreamingWriterConfig{
		S3Client:        mockClient,
		Bucket:          "test-bucket",
		Key:             "test-key",
		MaxBufferPeriod: 100 * time.Millisecond, // Short period for testing
		MaxBufferBytes:  10 * 1024 * 1024,       // 10MB
	}

	writer, err := NewS3StreamingWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	// Write small amount of data (2MB)
	testData := make([]byte, 2*1024*1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	err = writer.WriteBytes(ctx, testData)
	require.NoError(t, err)

	// Wait for timer to fire (should NOT flush since < 5MB)
	time.Sleep(200 * time.Millisecond)

	// Verify no upload happened yet
	assert.False(t, uploadPartCalled, "UploadPart should NOT be called for data < 5MB")

	// Close should use PutObject
	err = writer.Close(ctx)
	require.NoError(t, err)

	assert.False(t, uploadPartCalled, "UploadPart should never be called")
	assert.True(t, abortCalled, "multipart should be aborted")
	assert.True(t, putObjectCalled, "PutObject should be called")
	assert.Equal(t, testData, capturedPutData)
}

// TestS3StreamingWriterTimerFlushLargeEnoughData tests that timer-based flush
// DOES upload parts >= 5MB
func TestS3StreamingWriterTimerFlushLargeEnoughData(t *testing.T) {
	uploadPartCalls := 0
	completeMultipartCalled := false
	var uploadedParts [][]byte

	mockClient := &mockS3StreamClient{
		uploadPartFunc: func(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
			uploadPartCalls++
			// Capture uploaded data
			data := make([]byte, 10*1024*1024)
			n, _ := input.Body.Read(data)
			uploadedParts = append(uploadedParts, data[:n])
			return &s3.UploadPartOutput{
				ETag: aws.String("test-etag"),
			}, nil
		},
		completeMultipartUploadFunc: func(ctx context.Context, input *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
			completeMultipartCalled = true
			return &s3.CompleteMultipartUploadOutput{}, nil
		},
	}

	config := S3StreamingWriterConfig{
		S3Client:        mockClient,
		Bucket:          "test-bucket",
		Key:             "test-key",
		MaxBufferPeriod: 100 * time.Millisecond,
		MaxBufferBytes:  10 * 1024 * 1024,
	}

	writer, err := NewS3StreamingWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	// Write 6MB (enough for timer flush)
	testData := make([]byte, 6*1024*1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	err = writer.WriteBytes(ctx, testData)
	require.NoError(t, err)

	// Wait for timer to fire (should flush since >= 5MB)
	time.Sleep(200 * time.Millisecond)

	// Verify upload happened
	assert.Equal(t, 1, uploadPartCalls, "UploadPart should be called once")
	assert.Equal(t, testData, uploadedParts[0])

	// Close should complete multipart
	err = writer.Close(ctx)
	require.NoError(t, err)

	assert.True(t, completeMultipartCalled, "CompleteMultipartUpload should be called")
}

// TestS3StreamingWriterSlowStreamSmallTotal tests slow data arrival
// where total is < 5MB (simulates the Anthropic API issue)
func TestS3StreamingWriterSlowStreamSmallTotal(t *testing.T) {
	uploadPartCalled := false
	putObjectCalled := false
	var capturedPutData []byte

	mockClient := &mockS3StreamClient{
		uploadPartFunc: func(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
			uploadPartCalled = true
			return &s3.UploadPartOutput{ETag: aws.String("test-etag")}, nil
		},
		putObjectFunc: func(ctx context.Context, input *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			putObjectCalled = true
			data := make([]byte, 10*1024*1024)
			n, _ := input.Body.Read(data)
			capturedPutData = data[:n]
			return &s3.PutObjectOutput{}, nil
		},
	}

	config := S3StreamingWriterConfig{
		S3Client:        mockClient,
		Bucket:          "test-bucket",
		Key:             "test-key",
		MaxBufferPeriod: 50 * time.Millisecond, // Simulate 10s period
		MaxBufferBytes:  10 * 1024 * 1024,
	}

	writer, err := NewS3StreamingWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	// Simulate slow data arrival (like slow API pagination)
	// Write 500KB chunks with delays
	totalData := []byte{}
	for i := range 6 {
		chunk := make([]byte, 500*1024) // 500KB
		for j := range chunk {
			chunk[j] = byte((i*1000 + j) % 256)
		}
		totalData = append(totalData, chunk...)

		err = writer.WriteBytes(ctx, chunk)
		require.NoError(t, err)

		// Wait for timer (should NOT flush since < 5MB)
		time.Sleep(60 * time.Millisecond)
	}

	// Total written: 3MB, timer fired multiple times but shouldn't have uploaded
	assert.False(t, uploadPartCalled, "UploadPart should NOT be called for data < 5MB")

	// Close should use PutObject
	err = writer.Close(ctx)
	require.NoError(t, err)

	assert.False(t, uploadPartCalled, "UploadPart should never be called")
	assert.True(t, putObjectCalled, "PutObject should be called")
	assert.Equal(t, totalData, capturedPutData)
}

// TestS3StreamingWriterSlowStreamMultipleParts tests slow data arrival
// where total is > 5MB and should create multiple parts
func TestS3StreamingWriterSlowStreamMultipleParts(t *testing.T) {
	uploadPartCalls := 0
	var uploadedParts [][]byte

	mockClient := &mockS3StreamClient{
		uploadPartFunc: func(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
			uploadPartCalls++
			data := make([]byte, 20*1024*1024)
			n, _ := input.Body.Read(data)
			uploadedParts = append(uploadedParts, data[:n])
			return &s3.UploadPartOutput{ETag: aws.String("test-etag")}, nil
		},
	}

	config := S3StreamingWriterConfig{
		S3Client:        mockClient,
		Bucket:          "test-bucket",
		Key:             "test-key",
		MaxBufferPeriod: 50 * time.Millisecond,
		MaxBufferBytes:  10 * 1024 * 1024,
	}

	writer, err := NewS3StreamingWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	// Write 2MB chunks slowly until we hit 6MB (enough for first part)
	for i := range 3 {
		chunk := make([]byte, 2*1024*1024)
		for j := range chunk {
			chunk[j] = byte((i*1000 + j) % 256)
		}
		err = writer.WriteBytes(ctx, chunk)
		require.NoError(t, err)
		time.Sleep(60 * time.Millisecond)
	}

	// After 6MB, timer should have flushed
	assert.Equal(t, 1, uploadPartCalls, "First part should be uploaded after reaching 5MB")

	// Write another 4MB slowly
	for range 2 {
		chunk := make([]byte, 2*1024*1024)
		err = writer.WriteBytes(ctx, chunk)
		require.NoError(t, err)
		time.Sleep(60 * time.Millisecond)
	}

	// Timer should NOT flush second part yet (< 5MB)
	assert.Equal(t, 1, uploadPartCalls, "Second part should NOT be uploaded (< 5MB)")

	// Close should flush remaining 4MB as final part
	err = writer.Close(ctx)
	require.NoError(t, err)

	assert.Equal(t, 2, uploadPartCalls, "Two parts total")
	assert.Len(t, uploadedParts[0], 6*1024*1024, "First part should be 6MB")
	assert.Len(t, uploadedParts[1], 4*1024*1024, "Second (final) part should be 4MB")
}

// TestS3StreamingWriterBufferSizeFlush tests that buffer size trigger
// still works correctly
func TestS3StreamingWriterBufferSizeFlush(t *testing.T) {
	uploadPartCalls := 0

	mockClient := &mockS3StreamClient{
		uploadPartFunc: func(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
			uploadPartCalls++
			return &s3.UploadPartOutput{ETag: aws.String("test-etag")}, nil
		},
	}

	config := S3StreamingWriterConfig{
		S3Client:        mockClient,
		Bucket:          "test-bucket",
		Key:             "test-key",
		MaxBufferPeriod: 10 * time.Second, // Long period (won't fire)
		MaxBufferBytes:  5 * 1024 * 1024,  // 5MB buffer
	}

	writer, err := NewS3StreamingWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	// Write 6MB (should trigger buffer size flush)
	testData := make([]byte, 6*1024*1024)
	err = writer.WriteBytes(ctx, testData)
	require.NoError(t, err)

	// Should have flushed immediately due to buffer size (not timer)
	assert.Equal(t, 1, uploadPartCalls, "Should flush when buffer size reached")

	err = writer.Close(ctx)
	require.NoError(t, err)
}

// TestS3StreamingWriterExactly5MBTimer tests edge case of exactly 5MB with timer
func TestS3StreamingWriterExactly5MBTimer(t *testing.T) {
	uploadPartCalls := 0

	mockClient := &mockS3StreamClient{
		uploadPartFunc: func(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
			uploadPartCalls++
			return &s3.UploadPartOutput{ETag: aws.String("test-etag")}, nil
		},
	}

	config := S3StreamingWriterConfig{
		S3Client:        mockClient,
		Bucket:          "test-bucket",
		Key:             "test-key",
		MaxBufferPeriod: 50 * time.Millisecond,
		MaxBufferBytes:  10 * 1024 * 1024,
	}

	writer, err := NewS3StreamingWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	// Write exactly 5MB
	testData := make([]byte, 5*1024*1024)
	err = writer.WriteBytes(ctx, testData)
	require.NoError(t, err)

	// Wait for timer (should flush since == 5MB)
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, 1, uploadPartCalls, "Should flush exactly 5MB")

	err = writer.Close(ctx)
	require.NoError(t, err)
}
