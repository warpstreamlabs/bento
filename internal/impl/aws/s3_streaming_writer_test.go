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
	for i := 0; i < 100; i++ {
		err = writer.WriteBytes(ctx, data)
		require.NoError(t, err)
	}

	// Should have uploaded 1 part (triggered by count)
	assert.Len(t, uploadedParts, 1)
}

func TestS3StreamingWriterClose(t *testing.T) {
	completeCalled := false
	mockClient := &mockS3StreamClient{
		completeMultipartUploadFunc: func(ctx context.Context, input *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
			completeCalled = true
			return &s3.CompleteMultipartUploadOutput{}, nil
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

	// Write some data
	err = writer.WriteBytes(ctx, []byte("test data"))
	require.NoError(t, err)

	// Close should complete upload
	err = writer.Close(ctx)
	require.NoError(t, err)
	assert.True(t, completeCalled)
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
	for i := 0; i < 3; i++ {
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
