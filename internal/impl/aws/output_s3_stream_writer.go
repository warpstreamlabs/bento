package aws

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/cenkalti/backoff/v4"
)

const s3MultipartMinPartSize = 5 * 1024 * 1024

type s3AckFunc func(context.Context, error) error

// S3API defines the S3 operations required by the streaming writer
type s3StreamingAPI interface {
	CreateMultipartUpload(ctx context.Context, input *s3.CreateMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	ListMultipartUploads(ctx context.Context, input *s3.ListMultipartUploadsInput, opts ...func(*s3.Options)) (*s3.ListMultipartUploadsOutput, error)
	ListParts(ctx context.Context, input *s3.ListPartsInput, opts ...func(*s3.Options)) (*s3.ListPartsOutput, error)
}

type s3BufferedPart struct {
	data    []byte
	acks    []s3AckFunc
	size    int64
	msgs    int
	created time.Time
}

// S3StreamingWriter writes content incrementally to S3 using multipart uploads.
// It only acknowledges messages once their bytes are durably represented in S3.
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
	backoffCtor    func() backoff.BackOff

	// Buffering
	activeBuffer  *bytes.Buffer
	activeAcks    []s3AckFunc
	activeMsgs    int
	activeCreated time.Time
	sealedPart    *s3BufferedPart
	pendingAcks   []s3AckFunc

	// Statistics tracking
	totalMessages int64
	totalBytes    int64

	// Lifecycle
	created    time.Time
	lastWrite  time.Time
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
	MaxBufferBytes  int64
	MaxBufferCount  int
	MaxBufferPeriod time.Duration
	ContentType     string
	ContentEncoding string
	BackoffCtor     func() backoff.BackOff
}

// NewS3StreamingWriter creates a new streaming S3 writer
func NewS3StreamingWriter(config S3StreamingWriterConfig) (*S3StreamingWriter, error) {
	if config.MaxBufferBytes <= 0 {
		config.MaxBufferBytes = 10 * 1024 * 1024
	}
	if config.MaxBufferCount <= 0 {
		config.MaxBufferCount = 10000
	}
	if config.MaxBufferPeriod <= 0 {
		config.MaxBufferPeriod = 10 * time.Second
	}
	if config.ContentType == "" {
		config.ContentType = "application/octet-stream"
	}

	if config.BackoffCtor == nil {
		maxRetries := 2
		expBackoff := backoff.NewExponentialBackOff()
		expBackoff.InitialInterval = time.Second
		expBackoff.MaxInterval = time.Second * 5
		expBackoff.MaxElapsedTime = time.Second * 30

		config.BackoffCtor = func() backoff.BackOff {
			boff := *expBackoff
			boff.Reset()
			return backoff.WithMaxRetries(&boff, uint64(maxRetries))
		}
	}

	now := time.Now()
	w := &S3StreamingWriter{
		maxBufferBytes:  config.MaxBufferBytes,
		maxBufferCount:  config.MaxBufferCount,
		maxBufferPeriod: config.MaxBufferPeriod,
		contentType:     config.ContentType,
		contentEncoding: config.ContentEncoding,
		s3Client:        config.S3Client,
		bucket:          config.Bucket,
		key:             config.Key,
		backoffCtor:     config.BackoffCtor,
		activeBuffer:    bytes.NewBuffer(nil),
		activeCreated:   now,
		created:         now,
		lastWrite:       now,
	}

	return w, nil
}

// Initialize starts or recovers the S3 multipart upload for this exact key.
func (w *S3StreamingWriter) Initialize(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.uploadID != nil {
		return errors.New("writer already initialized")
	}

	if err := w.recoverMultipartUpload(ctx); err != nil {
		return err
	}
	if w.uploadID != nil {
		return nil
	}

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
	return nil
}

func (w *S3StreamingWriter) recoverMultipartUpload(ctx context.Context) error {
	var matches []types.MultipartUpload
	var keyMarker *string
	var uploadIDMarker *string

	for {
		resp, err := w.s3Client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
			Bucket:         aws.String(w.bucket),
			Prefix:         aws.String(w.key),
			KeyMarker:      keyMarker,
			UploadIdMarker: uploadIDMarker,
		})
		if err != nil {
			return fmt.Errorf("failed to list multipart uploads: %w", err)
		}
		for _, upload := range resp.Uploads {
			if upload.Key != nil && *upload.Key == w.key {
				matches = append(matches, upload)
			}
		}
		if resp.IsTruncated == nil || !*resp.IsTruncated {
			break
		}
		keyMarker = resp.NextKeyMarker
		uploadIDMarker = resp.NextUploadIdMarker
	}

	if len(matches) == 0 {
		return nil
	}
	if len(matches) > 1 {
		return fmt.Errorf("found multiple in-progress multipart uploads for key %q", w.key)
	}

	w.uploadID = matches[0].UploadId
	if w.uploadID == nil {
		return fmt.Errorf("found multipart upload for key %q without upload id", w.key)
	}

	type recoveredPart struct {
		number int32
		etag   *string
		size   int64
	}
	var parts []recoveredPart
	var partMarker *string
	for {
		resp, err := w.s3Client.ListParts(ctx, &s3.ListPartsInput{
			Bucket:           aws.String(w.bucket),
			Key:              aws.String(w.key),
			UploadId:         w.uploadID,
			PartNumberMarker: partMarker,
		})
		if err != nil {
			return fmt.Errorf("failed to list multipart upload parts: %w", err)
		}
		for _, part := range resp.Parts {
			if part.PartNumber == nil {
				continue
			}
			parts = append(parts, recoveredPart{
				number: *part.PartNumber,
				etag:   part.ETag,
				size:   aws.ToInt64(part.Size),
			})
		}
		if resp.IsTruncated == nil || !*resp.IsTruncated {
			break
		}
		partMarker = resp.NextPartNumberMarker
	}

	sort.Slice(parts, func(i, j int) bool {
		return parts[i].number < parts[j].number
	})

	// Drop an interrupted final part smaller than S3's minimum part size. This
	// occurs when a crash lands between the final part upload and
	// CompleteMultipartUpload in Close: the sub-minimum part is durable on S3 but
	// not yet part of a completed object. Our writer never produces a <5MiB
	// non-final part (shouldSealActive only seals at >=5MiB), so a sub-minimum
	// highest part can only be such an interrupted final part. Keeping it and
	// appending redelivered data after it would make it an illegal non-final part,
	// failing every CompleteMultipartUpload with EntityTooSmall. We drop it and
	// reuse its part number — its data is redelivered, and the next upload
	// overwrites the slot on S3 (we cannot rely on an optional bucket lifecycle
	// rule to reap it).
	if n := len(parts); n > 0 && parts[n-1].size < s3MultipartMinPartSize {
		parts = parts[:n-1]
	}

	for _, p := range parts {
		w.completedParts = append(w.completedParts, types.CompletedPart{
			ETag:       p.etag,
			PartNumber: aws.Int32(p.number),
		})
		if p.number > w.partNumber {
			w.partNumber = p.number
		}
	}
	return nil
}

// WriteBytes adds bytes and their ack callback to the active buffer.
func (w *S3StreamingWriter) WriteBytes(ctx context.Context, data []byte, ackFn s3AckFunc) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Once an ack is handed to the writer, the writer owns resolving it exactly
	// once. These early failures never store ackFn, so they must nack it here;
	// otherwise the upstream transaction leaks (neither acked nor nacked).
	if w.closed {
		err := errors.New("writer is closed")
		if ackFn != nil {
			_ = ackFn(ctx, err)
		}
		return err
	}
	if w.uploadID == nil {
		err := errors.New("writer not initialized")
		if ackFn != nil {
			_ = ackFn(ctx, err)
		}
		return err
	}

	w.activeBuffer.Write(data)
	w.activeAcks = append(w.activeAcks, ackFn)
	w.activeMsgs++
	w.totalMessages++
	w.totalBytes += int64(len(data))
	w.lastWrite = time.Now()
	defer w.scheduleFlushTimerLocked()

	if len(w.pendingAcks) > 0 {
		if err := ackAll(ctx, w.pendingAcks, nil); err != nil {
			return err
		}
		w.pendingAcks = nil
	}

	if w.shouldSealActive() {
		w.sealActive()
		if err := w.uploadSealed(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (w *S3StreamingWriter) scheduleFlushTimerLocked() {
	if w.maxBufferPeriod <= 0 || w.closed {
		return
	}
	if w.flushTimer != nil {
		w.flushTimer.Stop()
		w.flushTimer = nil
	}
	if w.activeBuffer.Len() == 0 {
		return
	}
	untilNext := time.Until(w.activeCreated.Add(w.maxBufferPeriod))
	if untilNext <= 0 {
		untilNext = w.maxBufferPeriod
	}
	w.flushTimer = time.AfterFunc(untilNext, func() {
		w.flushDueToPeriod(context.Background())
	})
}

func (w *S3StreamingWriter) flushDueToPeriod(ctx context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed || w.uploadID == nil {
		return
	}
	if w.activeBuffer.Len() >= s3MultipartMinPartSize && time.Since(w.activeCreated) >= w.maxBufferPeriod {
		w.sealActive()
		_ = w.uploadSealed(ctx)
	}
	w.scheduleFlushTimerLocked()
}

func (w *S3StreamingWriter) shouldSealActive() bool {
	if w.activeBuffer.Len() < s3MultipartMinPartSize {
		return false
	}
	return int64(w.activeBuffer.Len()) >= w.maxBufferBytes ||
		w.activeMsgs >= w.maxBufferCount ||
		time.Since(w.activeCreated) >= w.maxBufferPeriod
}

func (w *S3StreamingWriter) sealActive() {
	if w.activeBuffer.Len() == 0 {
		return
	}
	w.sealedPart = &s3BufferedPart{
		data:    append([]byte(nil), w.activeBuffer.Bytes()...),
		acks:    append([]s3AckFunc(nil), w.activeAcks...),
		size:    int64(w.activeBuffer.Len()),
		msgs:    w.activeMsgs,
		created: w.activeCreated,
	}
	w.activeBuffer.Reset()
	w.activeAcks = nil
	w.activeMsgs = 0
	w.activeCreated = time.Now()
}

func (w *S3StreamingWriter) uploadSealed(ctx context.Context) error {
	part := w.sealedPart
	if part == nil {
		return nil
	}
	if err := w.uploadPart(ctx, part.data); err != nil {
		// Do NOT abort the multipart upload. Prior parts are durable on S3 and
		// the upload stays resumable — in-process on the next redelivery (for a
		// transient failure), or via recoverMultipartUpload after a crash.
		// Aborting here would discard already-acked prior parts (silent data
		// loss) and delete exactly the state recovery relies on. We nack only
		// this part's messages, whose bytes are not durable; they redeliver and
		// rejoin the file later, possibly out of order (acceptable — Bento makes
		// no ordering guarantee). pendingAcks and the active buffer are left
		// untouched (the active buffer is already empty after sealActive).
		ackErr := fmt.Errorf("failed to upload part: %w", err)
		_ = ackAll(ctx, part.acks, ackErr)
		w.sealedPart = nil
		return ackErr
	}
	w.pendingAcks = append(w.pendingAcks, part.acks...)
	w.sealedPart = nil
	return nil
}

func (w *S3StreamingWriter) uploadPart(ctx context.Context, data []byte) error {
	w.partNumber++

	boff := w.backoffCtor()
	var uploadErr error
retryLoop:
	for {
		resp, err := w.s3Client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(w.bucket),
			Key:        aws.String(w.key),
			PartNumber: aws.Int32(w.partNumber),
			UploadId:   w.uploadID,
			Body:       bytes.NewReader(data),
		})

		if err == nil {
			w.completedParts = append(w.completedParts, types.CompletedPart{
				ETag:       resp.ETag,
				PartNumber: aws.Int32(w.partNumber),
			})
			return nil
		}

		uploadErr = err
		tNext := boff.NextBackOff()
		if tNext == backoff.Stop {
			break
		}
		select {
		case <-ctx.Done():
			break retryLoop
		case <-time.After(tNext):
		}
	}

	w.partNumber--
	return fmt.Errorf("failed to upload part after retries: %w", uploadErr)
}

// Close finalizes the file and acks remaining owned messages only after completion.
func (w *S3StreamingWriter) Close(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	if w.uploadID == nil {
		return errors.New("writer not initialized")
	}
	if w.flushTimer != nil {
		w.flushTimer.Stop()
		w.flushTimer = nil
	}

	var finalAcks []s3AckFunc
	finalAcks = append(finalAcks, w.pendingAcks...)
	w.pendingAcks = nil
	if w.sealedPart != nil {
		if err := w.uploadPart(ctx, w.sealedPart.data); err != nil {
			_ = ackAll(ctx, finalAcks, err)
			return w.failOwned(ctx, fmt.Errorf("failed to flush sealed part: %w", err))
		}
		finalAcks = append(finalAcks, w.sealedPart.acks...)
		w.sealedPart = nil
	}
	if w.activeBuffer.Len() > 0 {
		if err := w.uploadPart(ctx, w.activeBuffer.Bytes()); err != nil {
			_ = ackAll(ctx, finalAcks, err)
			return w.failOwned(ctx, fmt.Errorf("failed to flush final buffer: %w", err))
		}
		finalAcks = append(finalAcks, w.activeAcks...)
		w.activeBuffer.Reset()
		w.activeAcks = nil
		w.activeMsgs = 0
	}

	if len(w.completedParts) == 0 {
		if err := w.uploadPart(ctx, []byte{}); err != nil {
			_ = ackAll(ctx, finalAcks, err)
			return w.failOwned(ctx, fmt.Errorf("failed to upload empty part: %w", err))
		}
	}

	_, err := w.s3Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(w.bucket),
		Key:      aws.String(w.key),
		UploadId: w.uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: w.completedParts,
		},
	})
	if err != nil {
		_ = ackAll(ctx, finalAcks, err)
		return w.failOwned(ctx, fmt.Errorf("failed to complete multipart upload: %w", err))
	}

	if err := ackAll(ctx, finalAcks, nil); err != nil {
		return err
	}
	w.closed = true
	return nil
}

func (w *S3StreamingWriter) failOwned(ctx context.Context, err error) error {
	// Do NOT abort: leave the multipart upload in progress so a restart can
	// recover and complete it (for a deterministic key). Nack everything still
	// owned so it redelivers — none of it belongs to a completed object yet.
	_ = ackAll(ctx, w.pendingAcks, err)
	w.pendingAcks = nil
	if w.sealedPart != nil {
		_ = ackAll(ctx, w.sealedPart.acks, err)
		w.sealedPart = nil
	}
	_ = ackAll(ctx, w.activeAcks, err)
	w.activeAcks = nil
	w.activeBuffer.Reset()
	return err
}

func ackAll(ctx context.Context, acks []s3AckFunc, err error) error {
	for _, ack := range acks {
		if ack == nil {
			continue
		}
		if ackErr := ack(ctx, err); ackErr != nil {
			return ackErr
		}
	}
	return nil
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
