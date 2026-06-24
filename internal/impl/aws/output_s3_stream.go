package aws

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cenkalti/backoff/v4"

	"github.com/Jeffail/shutdown"

	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/component/interop"
	"github.com/warpstreamlabs/bento/internal/impl/aws/config"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	// S3 Stream Output Fields
	ssoFieldBucket             = "bucket"
	ssoFieldPath               = "path"
	ssoFieldPartitionBy        = "partition_by"
	ssoFieldForcePathStyleURLs = "force_path_style_urls"
	ssoFieldMaxBufferBytes     = "max_buffer_bytes"
	ssoFieldMaxBufferCount     = "max_buffer_count"
	ssoFieldMaxBufferPeriod    = "max_buffer_period"
	ssoFieldBatching           = "batching"
	ssoFieldContentType        = "content_type"
	ssoFieldContentEncoding    = "content_encoding"
	ssoFieldBackoff            = "backoff"
	ssoFieldMaxRetries         = "max_retries"
)

func s3StreamOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Version("1.16.0").
		Categories("Services", "AWS").
		Summary(`Streams data to S3 using multipart uploads.`).
		Description(`
This output writes to S3 using multipart uploads, streaming content incrementally rather than
buffering entire files in memory. This makes it ideal for writing large files or continuous streams
where memory efficiency is critical.

The `+"`partition_by`"+` parameter allows you to maintain separate S3 multipart uploads for different
partition values. Messages with matching partition values are written to the same file, and the full
path expression is evaluated only once per partition (allowing use of functions like `+"`uuid_v4()`"+`
for unique filenames). Without `+"`partition_by`"+`, each message evaluates the full path independently.

:::warning
### Weakens delivery guarantees

This output can weaken delivery guarantees when the input cannot let the stream reach finalization.
Final buffered bytes are uploaded and acknowledged when the input closes or the output is closed, so
bounded inputs are compatible only when they can close their transaction channel without waiting for
the final message acknowledgement. Inputs that wait for a matching final acknowledgement before
closing, such as `+"`read_until.check`"+`, can still deadlock if the final buffered output is smaller
than the S3 multipart part size. For finite drain-and-exit jobs, prefer
`+"`read_until.idle_timeout`"+`, which lets the input close on idle and allows this output to finalize
and acknowledge the tail.
:::

Messages are acknowledged only after their bytes are durably represented in S3. Buffered bytes are
not acknowledged while they are only held in memory, and the final buffered bytes are acknowledged
only after the upload is completed successfully.

On restart this output attempts to recover one in-progress multipart upload for the exact same S3
path by using S3 multipart listing APIs. This means crash recovery requires a deterministic `+"`path`"+`
that redelivered messages can recompute exactly. Paths using nondeterministic functions such as
`+"`uuid_v4()`"+` or the current timestamp are not crash-recoverable without a future manifest or cache
feature, unless they still recompute the exact same S3 key. Duplicate records can appear after a
crash and should be tolerated downstream.

## Delivery and Failures

Each part upload is retried according to `+"`max_retries`"+` and `+"`backoff`"+`. Once those are exhausted the
affected messages are rejected so an at-least-once input can redeliver them. A failed upload is never
aborted, so redelivered data resumes the same multipart upload (in-process, or via recovery after a
crash) rather than restarting the file. To apply back pressure indefinitely instead of giving up on a
part, set `+"`max_retries`"+` to `+"`0`"+` and leave `+"`backoff.max_elapsed_time`"+` empty.

Because messages are acknowledged only once their bytes are durable in a part, this output relies on a
continuous flow of messages to release acknowledgements: a part is only sealed and uploaded once enough
data accumulates, and the final bytes are acknowledged on close. For this reason it **cannot** be wrapped
in a `+"`drop_on`"+` output — `+"`drop_on`"+` waits for each message to be acknowledged before delivering the next, which
deadlocks against deferred acknowledgement. For the same reason it should not be fed by an input limited
to a single in-flight message.

Data that can never be written — for example a single message larger than S3's 5GiB maximum part size,
or a key S3 permanently rejects — will otherwise be redelivered indefinitely by an at-least-once input.
To divert such records to a dead-letter destination, wrap `+"`aws_s3_stream`"+` in a `+"`fallback`"+` output, which
forwards messages without waiting on acknowledgement and so composes correctly.

Because failed uploads are never aborted, an interrupted or permanently failing upload is left as an
in-progress multipart upload on S3. Configure an `+"`AbortIncompleteMultipartUpload`"+` lifecycle rule on
the bucket to clean these up automatically.

## When to Use

Use `+"`aws_s3_stream`"+` instead of `+"`aws_s3`"+` when:
- Writing large files (>100MB) where memory usage is a concern
- Streaming continuous data in memory-constrained environments
- You need per-partition file grouping with dynamic paths

## Credentials

By default Bento will use a shared credentials file when connecting to AWS services.
You can find out more [in this document](/docs/guides/cloud/aws).
`).
		Fields(
			service.NewStringField(ssoFieldBucket).
				Description("The S3 bucket to upload files to."),
			service.NewInterpolatedStringField(ssoFieldPath).
				Description("The path for each file.").
				Example(`logs/${! timestamp_unix() }-${! uuid_v4() }.log`).
				Example(`data/date=${! meta("date") }/account=${! meta("account") }/${! uuid_v4() }.json`),
			service.NewInterpolatedStringListField(ssoFieldPartitionBy).
				Description("Optional list of interpolated string expressions that determine writer partitioning. Messages with the same partition values are written to the same file. The full path is only evaluated once when a new partition is encountered. This allows using functions like uuid_v4() in the path for unique filenames per partition. If omitted, the full path is evaluated per message for backwards compatibility.").
				Example([]any{
					"${! meta(\"date\") }",
					"${! meta(\"account\") }",
				}).
				Optional().
				Advanced(),
			service.NewBoolField(ssoFieldForcePathStyleURLs).
				Description("Forces path style URLs for S3 requests.").
				Default(false).
				Advanced(),
			service.NewIntField(ssoFieldMaxBufferBytes).
				Description("Maximum bytes to buffer before uploading a multipart part. A part is only uploaded once the buffer reaches S3's 5MiB minimum part size; smaller amounts are uploaded only as the final part when the writer closes. Default is 10MB.").
				Default(10*1024*1024).
				Advanced(),
			service.NewIntField(ssoFieldMaxBufferCount).
				Description("Maximum messages to buffer before uploading a multipart part, subject to the same 5MiB minimum part size as `max_buffer_bytes`.").
				Default(10000).
				Advanced(),
			service.NewDurationField(ssoFieldMaxBufferPeriod).
				Description("Maximum duration to buffer before uploading a multipart part. Data below S3's 5MiB minimum part size is not uploaded on this interval, so low-volume streams are uploaded only as the final part when the writer closes.").
				Default("10s").
				Advanced(),
			service.NewInterpolatedStringField(ssoFieldContentType).
				Description("The content type to set for uploaded files.").
				Default("application/octet-stream").
				Advanced(),
			service.NewInterpolatedStringField(ssoFieldContentEncoding).
				Description("The content encoding to set for uploaded files (e.g., gzip).").
				Optional().
				Advanced(),
			service.NewIntField(ssoFieldMaxRetries).
				Description("The maximum number of retries for each individual part upload. Set to zero to disable retries.").
				Advanced().Default(2),
			service.NewBackOffField(ssoFieldBackoff, false, &backoff.ExponentialBackOff{
				InitialInterval: time.Second * 1,
				MaxInterval:     time.Second * 5,
				MaxElapsedTime:  time.Second * 30,
			}).Advanced(),
		).
		Fields(config.SessionFields()...).
		Field(service.NewOutputMaxInFlightField()).
		Field(service.NewBatchPolicyField(ssoFieldBatching)).
		Example(
			"Writing Partitioned Log Files",
			"This example writes streaming log files partitioned by date and service to S3.",
			`
output:
  aws_s3_stream:
    bucket: my-logs-bucket
    path: 'logs/date=${! meta("date") }/service=${! meta("service") }/${! uuid_v4() }.log'

    # Messages with same date+service go to same file
    partition_by:
      - '${! meta("date") }'
      - '${! meta("service") }'

    max_buffer_bytes: 10485760  # 10MB
    max_buffer_count: 10000
    max_buffer_period: 10s
`,
		).
		Example(
			"Low Memory JSON Streaming",
			"This example demonstrates memory-efficient streaming for large JSON datasets.",
			`
output:
  aws_s3_stream:
    bucket: data-lake
    path: 'events/date=${! now().ts_format("2006-01-02") }/${! uuid_v4() }.json'
    content_type: application/json

    batching:
      count: 10000
      period: 10s
      processors:
        - archive:
            format: lines
`,
		)
}

type s3StreamConfig struct {
	Bucket       string
	Path         *service.InterpolatedString
	PartitionBy  []*service.InterpolatedString
	UsePathStyle bool

	MaxBufferBytes  int64
	MaxBufferCount  int
	MaxBufferPeriod time.Duration
	ContentType     *service.InterpolatedString
	ContentEncoding *service.InterpolatedString

	aconf       aws.Config
	backoffCtor func() backoff.BackOff
}

func s3StreamConfigFromParsed(pConf *service.ParsedConfig) (conf s3StreamConfig, err error) {
	if conf.Bucket, err = pConf.FieldString(ssoFieldBucket); err != nil {
		return
	}

	if conf.Path, err = pConf.FieldInterpolatedString(ssoFieldPath); err != nil {
		return
	}

	// Parse partition_by (optional)
	if pConf.Contains(ssoFieldPartitionBy) {
		if conf.PartitionBy, err = pConf.FieldInterpolatedStringList(ssoFieldPartitionBy); err != nil {
			return
		}
	}

	if conf.UsePathStyle, err = pConf.FieldBool(ssoFieldForcePathStyleURLs); err != nil {
		return
	}

	// Buffer settings
	var maxBufferBytes int
	if maxBufferBytes, err = pConf.FieldInt(ssoFieldMaxBufferBytes); err != nil {
		return
	}
	conf.MaxBufferBytes = int64(maxBufferBytes)

	if conf.MaxBufferCount, err = pConf.FieldInt(ssoFieldMaxBufferCount); err != nil {
		return
	}

	if conf.MaxBufferPeriod, err = pConf.FieldDuration(ssoFieldMaxBufferPeriod); err != nil {
		return
	}

	if conf.ContentType, err = pConf.FieldInterpolatedString(ssoFieldContentType); err != nil {
		return
	}

	// Content encoding (optional)
	if pConf.Contains(ssoFieldContentEncoding) {
		if conf.ContentEncoding, err = pConf.FieldInterpolatedString(ssoFieldContentEncoding); err != nil {
			return
		}
	}

	// AWS config
	if conf.aconf, err = GetSession(context.TODO(), pConf); err != nil {
		return
	}

	var expBackoff *backoff.ExponentialBackOff
	if expBackoff, err = pConf.FieldBackOff(ssoFieldBackoff); err != nil {
		return
	}
	var maxRetries int
	if maxRetries, err = pConf.FieldInt(ssoFieldMaxRetries); err != nil {
		return
	}

	conf.backoffCtor = func() backoff.BackOff {
		boff := *expBackoff
		boff.Reset()
		return backoff.WithMaxRetries(&boff, uint64(maxRetries))
	}

	return
}

func init() {
	err := service.RegisterBatchOutput("aws_s3_stream", s3StreamOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy(ssoFieldBatching); err != nil {
				return
			}
			var wConf s3StreamConfig
			if wConf, err = s3StreamConfigFromParsed(conf); err != nil {
				return
			}
			var streamOut *s3StreamOutput
			streamOut, err = newS3StreamOutput(wConf, batchPolicy, mgr)
			out = interop.NewUnwrapInternalOutput(streamOut)
			batchPolicy = service.BatchPolicy{}
			maxInFlight = 1
			return
		})
	if err != nil {
		panic(err)
	}
}

type s3StreamOutput struct {
	conf         s3StreamConfig
	batchPolicy  service.BatchPolicy
	batcher      *service.Batcher
	log          *service.Logger
	s3Client     s3StreamingAPI
	s3ClientCtor func(s3StreamConfig) s3StreamingAPI

	// Writer pool for managing multiple partition paths
	writersMut sync.RWMutex
	writers    map[string]*S3StreamingWriter

	transactions <-chan message.Transaction
	status       atomic.Pointer[component.ConnectionStatus]
	shutSig      *shutdown.Signaller
	resources    *service.Resources
}

func newS3StreamOutput(conf s3StreamConfig, batchPolicy service.BatchPolicy, mgr *service.Resources) (*s3StreamOutput, error) {
	s := &s3StreamOutput{
		conf:        conf,
		batchPolicy: batchPolicy,
		log:         mgr.Logger(),
		writers:     make(map[string]*S3StreamingWriter),
		shutSig:     shutdown.NewSignaller(),
		resources:   mgr,
		s3ClientCtor: func(conf s3StreamConfig) s3StreamingAPI {
			return s3.NewFromConfig(conf.aconf, func(o *s3.Options) {
				o.UsePathStyle = conf.UsePathStyle
			})
		},
	}
	s.status.Store(component.ConnectionPending(interop.UnwrapManagement(mgr)))
	if !batchPolicy.IsNoop() {
		var err error
		if s.batcher, err = batchPolicy.NewBatcher(mgr); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *s3StreamOutput) Connect(ctx context.Context) error {
	s.s3Client = s.s3ClientCtor(s.conf)
	s.status.Store(component.ConnectionActive(interop.UnwrapManagement(s.resources)))
	return nil
}

func (s *s3StreamOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	return s.writeBatch(ctx, batch, func(context.Context, error) error {
		return nil
	})
}

func (s *s3StreamOutput) writeBatch(ctx context.Context, batch service.MessageBatch, ackFn s3AckFunc) error {
	// Group messages by partition key
	type partitionGroup struct {
		key  string
		path string
		msgs service.MessageBatch
	}
	partitionMap := make(map[string]*partitionGroup)

	for i, msg := range batch {
		var partitionKey string
		var fullPath string
		var err error

		if len(s.conf.PartitionBy) > 0 {
			// Use partition_by to determine routing key
			partitionParts := make([]string, len(s.conf.PartitionBy))
			for j, partExpr := range s.conf.PartitionBy {
				partitionParts[j], err = batch.TryInterpolatedString(i, partExpr)
				if err != nil {
					_ = ackFn(ctx, err)
					return fmt.Errorf("failed to evaluate partition_by[%d]: %w", j, err)
				}
			}
			partitionKey = fmt.Sprintf("%v", partitionParts)

			// Check if we already have a path for this partition
			if pg, exists := partitionMap[partitionKey]; exists {
				// Reuse existing path
				fullPath = pg.path
			} else {
				// Evaluate full path only once for this partition
				fullPath, err = batch.TryInterpolatedString(i, s.conf.Path)
				if err != nil {
					_ = ackFn(ctx, err)
					return fmt.Errorf("failed to interpolate path: %w", err)
				}
			}
		} else {
			// Backwards compatibility: evaluate path per message
			fullPath, err = batch.TryInterpolatedString(i, s.conf.Path)
			if err != nil {
				_ = ackFn(ctx, err)
				return fmt.Errorf("failed to interpolate path: %w", err)
			}
			partitionKey = fullPath
		}

		// Add message to partition group
		if pg, exists := partitionMap[partitionKey]; exists {
			pg.msgs = append(pg.msgs, msg)
		} else {
			partitionMap[partitionKey] = &partitionGroup{
				key:  partitionKey,
				path: fullPath,
				msgs: service.MessageBatch{msg},
			}
		}
	}

	if len(partitionMap) == 0 {
		return ackFn(ctx, nil)
	}

	// Write to each partition
	cAck := newS3CombinedAck(len(partitionMap), ackFn)
	for _, pg := range partitionMap {
		if err := s.writeToPartition(ctx, pg.key, pg.path, pg.msgs, cAck.Derive()); err != nil {
			return fmt.Errorf("failed to write to partition %s: %w", pg.key, err)
		}
	}
	return nil
}

func (s *s3StreamOutput) writeToPartition(ctx context.Context, partitionKey string, path string, batch service.MessageBatch, ackFn s3AckFunc) error {
	// Try to get existing writer with read lock
	s.writersMut.RLock()
	writer, exists := s.writers[partitionKey]
	s.writersMut.RUnlock()

	if !exists {
		// Need to create writer - acquire write lock
		s.writersMut.Lock()
		// Double-check that another goroutine didn't create it while we were waiting
		writer, exists = s.writers[partitionKey]
		if !exists {
			// Evaluate content type and encoding for this partition (uses first message in batch)
			var contentType string
			var contentEncoding string
			var err error

			if len(batch) > 0 {
				contentType, err = batch.TryInterpolatedString(0, s.conf.ContentType)
				if err != nil {
					s.writersMut.Unlock()
					_ = ackFn(ctx, err)
					return fmt.Errorf("failed to evaluate content_type: %w", err)
				}

				if s.conf.ContentEncoding != nil {
					contentEncoding, err = batch.TryInterpolatedString(0, s.conf.ContentEncoding)
					if err != nil {
						s.writersMut.Unlock()
						_ = ackFn(ctx, err)
						return fmt.Errorf("failed to evaluate content_encoding: %w", err)
					}
				}
			}

			newWriter, err := NewS3StreamingWriter(S3StreamingWriterConfig{
				S3Client:        s.s3Client,
				Bucket:          s.conf.Bucket,
				Key:             path,
				MaxBufferBytes:  s.conf.MaxBufferBytes,
				MaxBufferCount:  s.conf.MaxBufferCount,
				MaxBufferPeriod: s.conf.MaxBufferPeriod,
				ContentType:     contentType,
				ContentEncoding: contentEncoding,
				BackoffCtor:     s.conf.backoffCtor,
			})
			if err != nil {
				s.writersMut.Unlock()
				_ = ackFn(ctx, err)
				return fmt.Errorf("failed to create writer: %w", err)
			}

			if err := newWriter.Initialize(ctx); err != nil {
				s.writersMut.Unlock()
				_ = ackFn(ctx, err)
				return fmt.Errorf("failed to initialize writer: %w", err)
			}

			s.writers[partitionKey] = newWriter
			writer = newWriter
			s.log.Debugf("Created new streaming writer for path: %s", path)
		}
		s.writersMut.Unlock()
	}

	var partBytes []byte
	for _, msg := range batch {
		msgBytes, err := msg.AsBytes()
		if err != nil {
			_ = ackFn(ctx, err)
			return fmt.Errorf("failed to get message bytes: %w", err)
		}
		partBytes = append(partBytes, msgBytes...)
	}

	if err := writer.WriteBytes(ctx, partBytes, ackFn); err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	return nil
}

func (s *s3StreamOutput) Close(ctx context.Context) error {
	s.writersMut.Lock()
	defer s.writersMut.Unlock()

	s.log.Debugf("Closing %d active writers", len(s.writers))

	var lastErr error
	for path, writer := range s.writers {
		stats := writer.Stats()
		s.log.Debugf("Closing writer for %s (messages: %d, parts: %d, bytes: %d)",
			path, stats.TotalMessages, stats.PartsUploaded, stats.TotalBytes)

		if err := writer.Close(ctx); err != nil {
			s.log.Errorf("Failed to close writer for %s: %v", path, err)
			lastErr = err
		}
	}

	s.writers = make(map[string]*S3StreamingWriter)
	if s.batcher != nil {
		if err := s.batcher.Close(ctx); err != nil && lastErr == nil {
			lastErr = err
		}
	}
	s.status.Store(component.ConnectionClosed(interop.UnwrapManagement(s.resources)))

	return lastErr
}

func (s *s3StreamOutput) Consume(transactions <-chan message.Transaction) error {
	if s.transactions != nil {
		return component.ErrAlreadyStarted
	}
	s.transactions = transactions
	go s.loop()
	return nil
}

func (s *s3StreamOutput) loop() {
	defer s.shutSig.TriggerHasStopped()

	if err := s.Connect(context.Background()); err != nil {
		s.status.Store(component.ConnectionFailing(interop.UnwrapManagement(s.resources), err))
		return
	}

	var pendingAcks []s3AckFunc
	for {
		var nextTimedBatchChan <-chan time.Time
		if s.batcher != nil && len(pendingAcks) > 0 {
			if untilNext, exists := s.batcher.UntilNext(); exists {
				nextTimedBatchChan = time.After(untilNext)
			}
		}

		var tran message.Transaction
		var open bool
		flushTimedBatch := false
		select {
		case tran, open = <-s.transactions:
			if !open {
				if err := s.flushBatcher(context.Background(), pendingAcks); err != nil {
					s.log.Errorf("Failed to flush final S3 stream batch: %v", err)
				}
				if err := s.Close(context.Background()); err != nil {
					s.log.Errorf("Failed to close S3 stream output: %v", err)
				}
				return
			}
		case <-s.shutSig.HardStopChan():
			_ = s.Close(context.Background())
			return
		case <-nextTimedBatchChan:
			flushTimedBatch = true
		}

		if flushTimedBatch {
			if err := s.flushBatcher(context.Background(), pendingAcks); err != nil {
				s.log.Errorf("Failed to write timed S3 stream batch: %v", err)
			}
			pendingAcks = nil
			continue
		}

		if s.batcher == nil {
			batch := serviceBatchFromInternal(tran.Payload)
			if err := s.writeBatch(context.Background(), batch, tran.Ack); err != nil {
				s.log.Errorf("Failed to write S3 stream batch: %v", err)
			}
			continue
		}

		flush := false
		_ = tran.Payload.Iter(func(i int, part *message.Part) error {
			if s.batcher.Add(service.NewInternalMessage(part)) {
				flush = true
			}
			return nil
		})
		pendingAcks = append(pendingAcks, tran.Ack)
		if flush {
			if err := s.flushBatcher(context.Background(), pendingAcks); err != nil {
				s.log.Errorf("Failed to write S3 stream batch: %v", err)
			}
			pendingAcks = nil
		}
	}
}

func (s *s3StreamOutput) flushBatcher(ctx context.Context, pendingAcks []s3AckFunc) error {
	if s.batcher == nil || len(pendingAcks) == 0 {
		return nil
	}
	batch, err := s.batcher.Flush(ctx)
	if err != nil {
		_ = ackAll(ctx, pendingAcks, err)
		return err
	}
	if len(batch) == 0 {
		return nil
	}
	return s.writeBatch(ctx, batch, func(ctx context.Context, err error) error {
		return ackAll(ctx, pendingAcks, err)
	})
}

func serviceBatchFromInternal(batch message.Batch) service.MessageBatch {
	out := make(service.MessageBatch, 0, batch.Len())
	_ = batch.Iter(func(i int, part *message.Part) error {
		out = append(out, service.NewInternalMessage(part))
		return nil
	})
	return out
}

func (s *s3StreamOutput) ConnectionStatus() component.ConnectionStatuses {
	return component.ConnectionStatuses{s.status.Load()}
}

func (s *s3StreamOutput) TriggerCloseNow() {
	s.shutSig.TriggerHardStop()
}

func (s *s3StreamOutput) WaitForClose(ctx context.Context) error {
	select {
	case <-s.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// s3CombinedAck fans a single upstream transaction ack out across the multiple
// partition writers that one batch may touch. The root ack is resolved exactly
// once: either when every partition has been durably acked, or as soon as any
// single partition fails.
//
// The done guard is what makes this safe. Without it, a partial failure could
// either never resolve the root (a leaked in-flight transaction, since unvisited
// partitions never decrement the counter) or resolve it twice (e.g. a partition
// that already succeeded later fires its deferred ack after another partition has
// already nacked the batch). Firing once and then no-oping every later child call
// avoids both.
type s3CombinedAck struct {
	remaining int
	root      s3AckFunc
	done      bool
	mut       sync.Mutex
}

func newS3CombinedAck(count int, root s3AckFunc) *s3CombinedAck {
	return &s3CombinedAck{remaining: count, root: root}
}

func (c *s3CombinedAck) Derive() s3AckFunc {
	return func(ctx context.Context, err error) error {
		c.mut.Lock()
		defer c.mut.Unlock()
		if c.done {
			return nil
		}
		c.remaining--
		// Nack the whole batch on the first partition failure: a transaction can
		// only be resolved as a whole, so redelivery may duplicate the partitions
		// that already succeeded, which is acceptable. Otherwise ack only once
		// every partition is durable.
		if err != nil || c.remaining == 0 {
			c.done = true
			return c.root(ctx, err)
		}
		return nil
	}
}
