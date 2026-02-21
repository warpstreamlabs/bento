package aws

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3API defines the S3 operations required by the streaming writer
type s3StreamingAPI interface {
	CreateMultipartUpload(ctx context.Context, input *s3.CreateMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(ctx context.Context, input *s3.AbortMultipartUploadInput, opts ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
}

// S3StreamingWriter writes content incrementally to S3 using multipart uploads.
// It buffers bytes and uploads parts as they reach the S3 minimum part size (5MB).
type S3StreamingWriter struct {
	// Configuration
	maxBufferBytes  int64
	maxBufferCount  int
	maxBufferPeriod time.Duration
	contentType     string
	contentEncoding string

	// S3 client and upload state
	s3Client       s3StreamingAPI
	bucket         string
	key            string
	uploadID       *string
	partNumber     int32
	completedParts []types.CompletedPart

	// Buffering
	messageBuffer [][]byte
	uploadBuffer  *bytes.Buffer
	uploadSize    int64

	// Statistics tracking
	totalMessages int64
	totalBytes    int64

	// Lifecycle
	created    time.Time
	lastWrite  time.Time
	lastFlush  time.Time
	closed     bool
	flushTimer *time.Timer

	// Thread safety
	mu sync.Mutex
}

// S3StreamingWriterConfig contains configuration for creating an S3StreamingWriter
type S3StreamingWriterConfig struct {
	S3Client        s3StreamingAPI
	Bucket          string
	Key             string
	MaxBufferBytes  int64         // Maximum bytes to buffer before flushing (default: 10MB)
	MaxBufferCount  int           // Maximum messages to buffer before flushing (default: 10000)
	MaxBufferPeriod time.Duration // Maximum time to buffer before flushing (default: 10s)
	ContentType     string        // Content type for S3 object
	ContentEncoding string        // Content encoding for S3 object (optional)
}

// NewS3StreamingWriter creates a new streaming S3 writer
func NewS3StreamingWriter(config S3StreamingWriterConfig) (*S3StreamingWriter, error) {
	if config.MaxBufferBytes <= 0 {
		config.MaxBufferBytes = 10 * 1024 * 1024 // Default 10MB
	}
	if config.MaxBufferCount <= 0 {
		config.MaxBufferCount = 10000 // Default
	}
	if config.MaxBufferPeriod <= 0 {
		config.MaxBufferPeriod = 10 * time.Second // Default
	}
	if config.ContentType == "" {
		config.ContentType = "application/octet-stream"
	}

	w := &S3StreamingWriter{
		maxBufferBytes:  config.MaxBufferBytes,
		maxBufferCount:  config.MaxBufferCount,
		maxBufferPeriod: config.MaxBufferPeriod,
		contentType:     config.ContentType,
		contentEncoding: config.ContentEncoding,
		s3Client:        config.S3Client,
		bucket:          config.Bucket,
		key:             config.Key,
		uploadBuffer:    bytes.NewBuffer(nil),
		messageBuffer:   make([][]byte, 0, config.MaxBufferCount),
		created:         time.Now(),
		lastWrite:       time.Now(),
		lastFlush:       time.Now(),
	}

	return w, nil
}

// Initialize starts the S3 multipart upload
func (w *S3StreamingWriter) Initialize(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.uploadID != nil {
		return errors.New("writer already initialized")
	}

	// Start multipart upload
	input := &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(w.bucket),
		Key:         aws.String(w.key),
		ContentType: aws.String(w.contentType),
	}

	if w.contentEncoding != "" {
		input.ContentEncoding = aws.String(w.contentEncoding)
	}

	resp, err := w.s3Client.CreateMultipartUpload(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to create multipart upload: %w", err)
	}

	w.uploadID = resp.UploadId

	// Start flush timer
	w.flushTimer = time.AfterFunc(w.maxBufferPeriod, func() {
		w.flushIfNeeded(context.Background())
	})

	return nil
}

// WriteBytes adds bytes to the buffer and flushes when thresholds are reached
func (w *S3StreamingWriter) WriteBytes(ctx context.Context, data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.New("writer is closed")
	}

	if w.uploadID == nil {
		return errors.New("writer not initialized")
	}

	// Add to message buffer
	w.messageBuffer = append(w.messageBuffer, data)
	w.uploadBuffer.Write(data)
	w.uploadSize += int64(len(data))
	w.totalMessages++
	w.totalBytes += int64(len(data))
	w.lastWrite = time.Now()

	// Check if we should flush
	shouldFlush := false
	if w.uploadSize >= w.maxBufferBytes {
		shouldFlush = true
	} else if len(w.messageBuffer) >= w.maxBufferCount {
		shouldFlush = true
	}

	if shouldFlush {
		return w.flush(ctx)
	}

	return nil
}

// flush uploads the current buffer as an S3 part (if large enough)
func (w *S3StreamingWriter) flush(ctx context.Context) error {
	if w.uploadBuffer.Len() == 0 {
		return nil
	}

	// Only upload if we have enough data (S3 requires 5MB minimum except for last part)
	if w.uploadSize < 5*1024*1024 {
		// Buffer more data unless we're at max buffer period
		if time.Since(w.lastFlush) < w.maxBufferPeriod {
			return nil
		}
	}

	w.partNumber++

	// Upload part with retry
	var uploadErr error
	for attempt := 0; attempt < 3; attempt++ {
		resp, err := w.s3Client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(w.bucket),
			Key:        aws.String(w.key),
			PartNumber: aws.Int32(w.partNumber),
			UploadId:   w.uploadID,
			Body:       bytes.NewReader(w.uploadBuffer.Bytes()),
		})

		if err == nil {
			// Success
			w.completedParts = append(w.completedParts, types.CompletedPart{
				ETag:       resp.ETag,
				PartNumber: aws.Int32(w.partNumber),
			})

			// Clear buffers
			w.uploadBuffer.Reset()
			w.uploadSize = 0
			w.messageBuffer = w.messageBuffer[:0]
			w.lastFlush = time.Now()

			// Reset flush timer
			if w.flushTimer != nil {
				w.flushTimer.Stop()
			}
			w.flushTimer = time.AfterFunc(w.maxBufferPeriod, func() {
				w.flushIfNeeded(context.Background())
			})

			return nil
		}

		uploadErr = err
		time.Sleep(time.Second * time.Duration(1<<attempt)) // Exponential backoff
	}

	// All retries failed
	w.abortMultipartUpload(context.Background())
	return fmt.Errorf("failed to upload part after retries: %w", uploadErr)
}

// flushIfNeeded checks if buffer should be flushed based on time period
func (w *S3StreamingWriter) flushIfNeeded(ctx context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return
	}

	if time.Since(w.lastFlush) >= w.maxBufferPeriod && w.uploadBuffer.Len() > 0 {
		// Force flush even if under 5MB threshold
		w.forceFlush(ctx)
	}

	// Reschedule timer
	if w.flushTimer != nil {
		w.flushTimer.Stop()
	}
	w.flushTimer = time.AfterFunc(w.maxBufferPeriod, func() {
		w.flushIfNeeded(context.Background())
	})
}

// forceFlush uploads whatever is in the buffer, even if under 5MB threshold
func (w *S3StreamingWriter) forceFlush(ctx context.Context) error {
	if w.uploadBuffer.Len() == 0 {
		return nil
	}

	w.partNumber++

	// Upload part
	resp, err := w.s3Client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(w.bucket),
		Key:        aws.String(w.key),
		PartNumber: aws.Int32(w.partNumber),
		UploadId:   w.uploadID,
		Body:       bytes.NewReader(w.uploadBuffer.Bytes()),
	})

	if err != nil {
		w.abortMultipartUpload(context.Background())
		return fmt.Errorf("failed to force flush part: %w", err)
	}

	// Success
	w.completedParts = append(w.completedParts, types.CompletedPart{
		ETag:       resp.ETag,
		PartNumber: aws.Int32(w.partNumber),
	})

	// Clear buffers
	w.uploadBuffer.Reset()
	w.uploadSize = 0
	w.messageBuffer = w.messageBuffer[:0]
	w.lastFlush = time.Now()

	return nil
}

// Close finalizes the file by flushing remaining data and completing the upload
func (w *S3StreamingWriter) Close(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	if w.uploadID == nil {
		return errors.New("writer not initialized")
	}

	// Stop flush timer
	if w.flushTimer != nil {
		w.flushTimer.Stop()
	}

	// Flush any remaining data
	if w.uploadBuffer.Len() > 0 {
		if err := w.forceFlush(ctx); err != nil {
			return fmt.Errorf("failed to flush final buffer: %w", err)
		}
	}

	// Handle edge case: no parts uploaded (empty file)
	if len(w.completedParts) == 0 {
		// Upload a single empty part to satisfy S3 requirements
		w.partNumber++
		resp, err := w.s3Client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(w.bucket),
			Key:        aws.String(w.key),
			PartNumber: aws.Int32(w.partNumber),
			UploadId:   w.uploadID,
			Body:       bytes.NewReader([]byte{}),
		})
		if err != nil {
			w.abortMultipartUpload(ctx)
			return fmt.Errorf("failed to upload empty part: %w", err)
		}
		w.completedParts = append(w.completedParts, types.CompletedPart{
			ETag:       resp.ETag,
			PartNumber: aws.Int32(w.partNumber),
		})
	}

	// Complete multipart upload
	_, err := w.s3Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(w.bucket),
		Key:      aws.String(w.key),
		UploadId: w.uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: w.completedParts,
		},
	})

	if err != nil {
		w.abortMultipartUpload(ctx)
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	w.closed = true
	return nil
}

// abortMultipartUpload aborts the multipart upload on error
func (w *S3StreamingWriter) abortMultipartUpload(ctx context.Context) {
	if w.uploadID == nil {
		return
	}

	_, _ = w.s3Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(w.bucket),
		Key:      aws.String(w.key),
		UploadId: w.uploadID,
	})
}

// WriterStats contains statistics about the writer
type S3StreamWriterStats struct {
	TotalMessages int64
	TotalBytes    int64
	PartsUploaded int32
	Age           time.Duration
	LastWriteAge  time.Duration
}

// Stats returns current writer statistics
func (w *S3StreamingWriter) Stats() S3StreamWriterStats {
	w.mu.Lock()
	defer w.mu.Unlock()

	return S3StreamWriterStats{
		TotalMessages: w.totalMessages,
		TotalBytes:    w.totalBytes,
		PartsUploaded: w.partNumber,
		Age:           time.Since(w.created),
		LastWriteAge:  time.Since(w.lastWrite),
	}
}
