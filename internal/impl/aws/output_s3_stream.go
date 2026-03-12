package aws

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/warpstreamlabs/bento/internal/impl/aws/config"
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
)

func s3StreamOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
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
				Description("Maximum buffer size in bytes before flushing to S3. Default is 10MB.").
				Default(10*1024*1024).
				Advanced(),
			service.NewIntField(ssoFieldMaxBufferCount).
				Description("Maximum number of messages to buffer before flushing to S3.").
				Default(10000).
				Advanced(),
			service.NewDurationField(ssoFieldMaxBufferPeriod).
				Description("Maximum duration to buffer messages before flushing to S3.").
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

	aconf aws.Config
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
			out, err = newS3StreamOutput(wConf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

type s3StreamOutput struct {
	conf     s3StreamConfig
	log      *service.Logger
	s3Client *s3.Client

	// Writer pool for managing multiple partition paths
	writersMut sync.RWMutex
	writers    map[string]*S3StreamingWriter
}

func newS3StreamOutput(conf s3StreamConfig, mgr *service.Resources) (*s3StreamOutput, error) {
	return &s3StreamOutput{
		conf:    conf,
		log:     mgr.Logger(),
		writers: make(map[string]*S3StreamingWriter),
	}, nil
}

func (s *s3StreamOutput) Connect(ctx context.Context) error {
	s.s3Client = s3.NewFromConfig(s.conf.aconf, func(o *s3.Options) {
		o.UsePathStyle = s.conf.UsePathStyle
	})
	return nil
}

func (s *s3StreamOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
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
					return fmt.Errorf("failed to interpolate path: %w", err)
				}
			}
		} else {
			// Backwards compatibility: evaluate path per message
			fullPath, err = batch.TryInterpolatedString(i, s.conf.Path)
			if err != nil {
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

	// Write to each partition
	for _, pg := range partitionMap {
		if err := s.writeToPartition(ctx, pg.key, pg.path, pg.msgs); err != nil {
			return fmt.Errorf("failed to write to partition %s: %w", pg.key, err)
		}
	}

	return nil
}

func (s *s3StreamOutput) writeToPartition(ctx context.Context, partitionKey string, path string, batch service.MessageBatch) error {
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
					return fmt.Errorf("failed to evaluate content_type: %w", err)
				}

				if s.conf.ContentEncoding != nil {
					contentEncoding, err = batch.TryInterpolatedString(0, s.conf.ContentEncoding)
					if err != nil {
						s.writersMut.Unlock()
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
			})
			if err != nil {
				s.writersMut.Unlock()
				return fmt.Errorf("failed to create writer: %w", err)
			}

			if err := newWriter.Initialize(ctx); err != nil {
				s.writersMut.Unlock()
				return fmt.Errorf("failed to initialize writer: %w", err)
			}

			s.writers[partitionKey] = newWriter
			writer = newWriter
			s.log.Debugf("Created new streaming writer for path: %s", path)
		}
		s.writersMut.Unlock()
	}

	// Write messages to writer
	for _, msg := range batch {
		msgBytes, err := msg.AsBytes()
		if err != nil {
			return fmt.Errorf("failed to get message bytes: %w", err)
		}

		if err := writer.WriteBytes(ctx, msgBytes); err != nil {
			return fmt.Errorf("failed to write message: %w", err)
		}
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

	return lastErr
}
