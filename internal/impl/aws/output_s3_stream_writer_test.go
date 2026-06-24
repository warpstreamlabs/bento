package aws

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockS3StreamClient is a mock S3 client for testing
type mockS3StreamClient struct {
	createMultipartUploadFunc   func(ctx context.Context, input *s3.CreateMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	uploadPartFunc              func(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	completeMultipartUploadFunc func(ctx context.Context, input *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	listMultipartUploadsFunc    func(ctx context.Context, input *s3.ListMultipartUploadsInput, opts ...func(*s3.Options)) (*s3.ListMultipartUploadsOutput, error)
	listPartsFunc               func(ctx context.Context, input *s3.ListPartsInput, opts ...func(*s3.Options)) (*s3.ListPartsOutput, error)
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

func (m *mockS3StreamClient) ListMultipartUploads(ctx context.Context, input *s3.ListMultipartUploadsInput, opts ...func(*s3.Options)) (*s3.ListMultipartUploadsOutput, error) {
	if m.listMultipartUploadsFunc != nil {
		return m.listMultipartUploadsFunc(ctx, input, opts...)
	}
	return &s3.ListMultipartUploadsOutput{}, nil
}

func (m *mockS3StreamClient) ListParts(ctx context.Context, input *s3.ListPartsInput, opts ...func(*s3.Options)) (*s3.ListPartsOutput, error) {
	if m.listPartsFunc != nil {
		return m.listPartsFunc(ctx, input, opts...)
	}
	return &s3.ListPartsOutput{}, nil
}

type ackRecord struct {
	called int
	err    error
}

func (a *ackRecord) fn(ctx context.Context, err error) error {
	a.called++
	a.err = err
	return nil
}

func noRetryBackoff() backoff.BackOff {
	return backoff.WithMaxRetries(backoff.NewConstantBackOff(0), 0)
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
	err = writer.WriteBytes(ctx, []byte("test data"), nil)
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

	// Write 6MB of data. This seals and uploads the part, but it is not acked
	// until a later message for the same key is owned.
	data := make([]byte, 6*1024*1024)
	firstAck := &ackRecord{}
	err = writer.WriteBytes(ctx, data, firstAck.fn)
	require.NoError(t, err)

	assert.Len(t, uploadedParts, 1)
	assert.Equal(t, int32(1), uploadedParts[0])
	assert.Equal(t, 0, firstAck.called)

	secondAck := &ackRecord{}
	err = writer.WriteBytes(ctx, []byte("next"), secondAck.fn)
	require.NoError(t, err)

	assert.Len(t, uploadedParts, 1)
	assert.Equal(t, 1, firstAck.called)
	require.NoError(t, firstAck.err)
	assert.Equal(t, 0, secondAck.called)
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
	firstAck := &ackRecord{}
	for range 100 {
		err = writer.WriteBytes(ctx, data, firstAck.fn)
		require.NoError(t, err)
	}

	assert.Len(t, uploadedParts, 1)
	assert.Equal(t, 0, firstAck.called)

	err = writer.WriteBytes(ctx, []byte("next"), nil)
	require.NoError(t, err)

	assert.Len(t, uploadedParts, 1)
	assert.Equal(t, 100, firstAck.called)
}

func TestS3StreamingWriterBufferFlushOnPeriod(t *testing.T) {
	uploaded := make(chan struct{}, 1)
	mockClient := &mockS3StreamClient{
		uploadPartFunc: func(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
			uploaded <- struct{}{}
			return &s3.UploadPartOutput{
				ETag: aws.String("test-etag"),
			}, nil
		},
	}

	writer, err := NewS3StreamingWriter(S3StreamingWriterConfig{
		S3Client:        mockClient,
		Bucket:          "test-bucket",
		Key:             "test-key",
		MaxBufferBytes:  100 * 1024 * 1024,
		MaxBufferCount:  10000,
		MaxBufferPeriod: 5 * time.Millisecond,
	})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, writer.Initialize(ctx))

	ack := &ackRecord{}
	require.NoError(t, writer.WriteBytes(ctx, make([]byte, 6*1024*1024), ack.fn))

	select {
	case <-uploaded:
	case <-time.After(time.Second):
		t.Fatal("timed buffer flush did not upload a valid multipart part")
	}
	assert.Equal(t, 0, ack.called)
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

	ack := &ackRecord{}
	err = writer.WriteBytes(ctx, []byte("test data"), ack.fn)
	require.NoError(t, err)
	assert.Equal(t, 0, ack.called)

	// Close should complete upload
	err = writer.Close(ctx)
	require.NoError(t, err)
	assert.True(t, completeCalled)
	assert.True(t, writer.closed)
	assert.Equal(t, 1, ack.called)
	require.NoError(t, ack.err)
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
	err = writer.WriteBytes(ctx, []byte("test"), nil)
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

	// Write 6MB to seal and upload a part.
	data := make([]byte, 6*1024*1024)
	err = writer.WriteBytes(ctx, data, nil)
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
		err = writer.WriteBytes(ctx, data, nil)
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

func TestS3StreamingWriterUploadFailureNacksOwnedCallbacks(t *testing.T) {
	uploadErr := errors.New("upload failed")
	mockClient := &mockS3StreamClient{
		uploadPartFunc: func(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
			return nil, uploadErr
		},
	}

	writer, err := NewS3StreamingWriter(S3StreamingWriterConfig{
		S3Client:       mockClient,
		Bucket:         "test-bucket",
		Key:            "test-key",
		MaxBufferBytes: 5 * 1024 * 1024,
		BackoffCtor:    noRetryBackoff,
	})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, writer.Initialize(ctx))

	firstAck := &ackRecord{}
	err = writer.WriteBytes(ctx, make([]byte, 6*1024*1024), firstAck.fn)
	require.Error(t, err)
	assert.Equal(t, 1, firstAck.called)
	assert.Error(t, firstAck.err)
}

// A part-upload failure must NOT abort the multipart upload: prior parts stay
// durable (so already-acked messages aren't silently lost), the writer keeps a
// valid upload and resumes, and only the failing part's messages are nacked.
func TestS3StreamingWriterPartFailureKeepsUploadAndResumes(t *testing.T) {
	var uploadCalls int
	var completedParts []types.CompletedPart
	mockClient := &mockS3StreamClient{
		uploadPartFunc: func(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
			uploadCalls++
			if uploadCalls == 2 {
				return nil, errors.New("transient upload failure")
			}
			return &s3.UploadPartOutput{ETag: aws.String("test-etag")}, nil
		},
		completeMultipartUploadFunc: func(ctx context.Context, input *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
			completedParts = input.MultipartUpload.Parts
			return &s3.CompleteMultipartUploadOutput{}, nil
		},
	}

	writer, err := NewS3StreamingWriter(S3StreamingWriterConfig{
		S3Client:       mockClient,
		Bucket:         "test-bucket",
		Key:            "test-key",
		MaxBufferBytes: 5 * 1024 * 1024,
		BackoffCtor:    noRetryBackoff,
	})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, writer.Initialize(ctx))
	part := make([]byte, 6*1024*1024)

	// Part 1 uploads successfully; its ack is held, not yet released.
	ackA := &ackRecord{}
	require.NoError(t, writer.WriteBytes(ctx, part, ackA.fn))
	assert.Equal(t, 0, ackA.called)

	// Next message releases ackA (part 1 is now acked), then seals part 2 whose
	// upload fails. Part 1 must survive and only ackB is nacked.
	ackB := &ackRecord{}
	err = writer.WriteBytes(ctx, part, ackB.fn)
	require.Error(t, err)
	assert.Equal(t, 1, ackA.called)
	require.NoError(t, ackA.err, "already-uploaded part 1 must stay acked, not lost")
	assert.Equal(t, 1, ackB.called)
	assert.Error(t, ackB.err, "only the failing part's messages are nacked")

	// Upload is still alive (not aborted/poisoned): part 1 retained, slot reused.
	require.Len(t, writer.completedParts, 1)
	assert.Equal(t, int32(1), writer.partNumber)
	assert.False(t, writer.closed)

	// The writer resumes: a later message uploads as part 2 and close completes.
	ackC := &ackRecord{}
	require.NoError(t, writer.WriteBytes(ctx, part, ackC.fn))
	require.NoError(t, writer.Close(ctx))

	require.Len(t, completedParts, 2)
	assert.Equal(t, int32(1), aws.ToInt32(completedParts[0].PartNumber))
	assert.Equal(t, int32(2), aws.ToInt32(completedParts[1].PartNumber))
	assert.Equal(t, 1, ackC.called)
	require.NoError(t, ackC.err)
}

func TestS3StreamingWriterCompleteFailureNacksFinalCallbacks(t *testing.T) {
	completeErr := errors.New("complete failed")
	mockClient := &mockS3StreamClient{
		completeMultipartUploadFunc: func(ctx context.Context, input *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
			return nil, completeErr
		},
	}

	writer, err := NewS3StreamingWriter(S3StreamingWriterConfig{
		S3Client:    mockClient,
		Bucket:      "test-bucket",
		Key:         "test-key",
		BackoffCtor: noRetryBackoff,
	})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, writer.Initialize(ctx))

	ack := &ackRecord{}
	require.NoError(t, writer.WriteBytes(ctx, []byte("final"), ack.fn))

	err = writer.Close(ctx)
	require.Error(t, err)
	assert.Equal(t, 1, ack.called)
	assert.Error(t, ack.err)
}

func TestS3StreamingWriterRecoversExactKeyMultipartUpload(t *testing.T) {
	var createCalled bool
	var uploadedPartNumber int32
	mockClient := &mockS3StreamClient{
		listMultipartUploadsFunc: func(ctx context.Context, input *s3.ListMultipartUploadsInput, opts ...func(*s3.Options)) (*s3.ListMultipartUploadsOutput, error) {
			return &s3.ListMultipartUploadsOutput{
				Uploads: []types.MultipartUpload{
					{Key: aws.String("test-key"), UploadId: aws.String("existing-upload")},
					{Key: aws.String("test-key-other"), UploadId: aws.String("other-upload")},
				},
			}, nil
		},
		listPartsFunc: func(ctx context.Context, input *s3.ListPartsInput, opts ...func(*s3.Options)) (*s3.ListPartsOutput, error) {
			assert.Equal(t, "existing-upload", aws.ToString(input.UploadId))
			// Recovered non-final parts are always >=5MiB (S3's minimum), as our
			// writer only seals at that threshold; ListParts reports real sizes.
			return &s3.ListPartsOutput{
				Parts: []types.Part{
					{PartNumber: aws.Int32(1), ETag: aws.String("etag-1"), Size: aws.Int64(5 * 1024 * 1024)},
					{PartNumber: aws.Int32(2), ETag: aws.String("etag-2"), Size: aws.Int64(5 * 1024 * 1024)},
				},
			}, nil
		},
		createMultipartUploadFunc: func(ctx context.Context, input *s3.CreateMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
			createCalled = true
			return &s3.CreateMultipartUploadOutput{UploadId: aws.String("new-upload")}, nil
		},
		uploadPartFunc: func(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
			uploadedPartNumber = aws.ToInt32(input.PartNumber)
			return &s3.UploadPartOutput{ETag: aws.String("etag-3")}, nil
		},
	}

	writer, err := NewS3StreamingWriter(S3StreamingWriterConfig{
		S3Client:    mockClient,
		Bucket:      "test-bucket",
		Key:         "test-key",
		BackoffCtor: noRetryBackoff,
	})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, writer.Initialize(ctx))
	assert.False(t, createCalled)
	assert.Equal(t, "existing-upload", aws.ToString(writer.uploadID))
	assert.Equal(t, int32(2), writer.partNumber)

	require.NoError(t, writer.WriteBytes(ctx, []byte("final"), nil))
	require.NoError(t, writer.Close(ctx))
	assert.Equal(t, int32(3), uploadedPartNumber)
}

// If a crash interrupts Close between the final (<5MiB) part upload and
// CompleteMultipartUpload, recovery must drop that sub-minimum trailing part and
// reuse its slot — otherwise appending redelivered data after it makes it an
// illegal non-final part and CompleteMultipartUpload fails with EntityTooSmall
// forever. This must not depend on a bucket lifecycle rule.
func TestS3StreamingWriterRecoveryDropsSubMinimumFinalPart(t *testing.T) {
	var completedParts []types.CompletedPart
	var uploadedPartNumbers []int32
	mockClient := &mockS3StreamClient{
		listMultipartUploadsFunc: func(ctx context.Context, input *s3.ListMultipartUploadsInput, opts ...func(*s3.Options)) (*s3.ListMultipartUploadsOutput, error) {
			return &s3.ListMultipartUploadsOutput{
				Uploads: []types.MultipartUpload{{Key: aws.String("test-key"), UploadId: aws.String("existing-upload")}},
			}, nil
		},
		listPartsFunc: func(ctx context.Context, input *s3.ListPartsInput, opts ...func(*s3.Options)) (*s3.ListPartsOutput, error) {
			// Part 1 is a valid >=5MiB part; part 2 is the interrupted final part (<5MiB).
			return &s3.ListPartsOutput{
				Parts: []types.Part{
					{PartNumber: aws.Int32(1), ETag: aws.String("etag-1"), Size: aws.Int64(5 * 1024 * 1024)},
					{PartNumber: aws.Int32(2), ETag: aws.String("etag-2"), Size: aws.Int64(1024)},
				},
			}, nil
		},
		uploadPartFunc: func(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
			uploadedPartNumbers = append(uploadedPartNumbers, aws.ToInt32(input.PartNumber))
			return &s3.UploadPartOutput{ETag: aws.String("etag-new")}, nil
		},
		completeMultipartUploadFunc: func(ctx context.Context, input *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
			completedParts = input.MultipartUpload.Parts
			return &s3.CompleteMultipartUploadOutput{}, nil
		},
	}

	writer, err := NewS3StreamingWriter(S3StreamingWriterConfig{
		S3Client:    mockClient,
		Bucket:      "test-bucket",
		Key:         "test-key",
		BackoffCtor: noRetryBackoff,
	})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, writer.Initialize(ctx))

	// The <5MiB part 2 is dropped; only part 1 is retained and the next part
	// reuses slot 2.
	assert.Equal(t, int32(1), writer.partNumber)
	require.Len(t, writer.completedParts, 1)
	assert.Equal(t, int32(1), aws.ToInt32(writer.completedParts[0].PartNumber))

	require.NoError(t, writer.WriteBytes(ctx, []byte("redelivered tail"), nil))
	require.NoError(t, writer.Close(ctx))

	// New data overwrote slot 2; completion contains parts 1 and 2 only, no
	// orphaned sub-minimum non-final part.
	assert.Equal(t, []int32{2}, uploadedPartNumbers)
	require.Len(t, completedParts, 2)
	assert.Equal(t, int32(1), aws.ToInt32(completedParts[0].PartNumber))
	assert.Equal(t, int32(2), aws.ToInt32(completedParts[1].PartNumber))
}

func TestS3StreamingWriterMultipleExactKeyMultipartUploadsErrors(t *testing.T) {
	mockClient := &mockS3StreamClient{
		listMultipartUploadsFunc: func(ctx context.Context, input *s3.ListMultipartUploadsInput, opts ...func(*s3.Options)) (*s3.ListMultipartUploadsOutput, error) {
			return &s3.ListMultipartUploadsOutput{
				Uploads: []types.MultipartUpload{
					{Key: aws.String("test-key"), UploadId: aws.String("upload-1")},
					{Key: aws.String("test-key"), UploadId: aws.String("upload-2")},
				},
			}, nil
		},
	}

	writer, err := NewS3StreamingWriter(S3StreamingWriterConfig{
		S3Client: mockClient,
		Bucket:   "test-bucket",
		Key:      "test-key",
	})
	require.NoError(t, err)

	err = writer.Initialize(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "multiple in-progress multipart uploads")
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
