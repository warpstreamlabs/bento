package aws

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/public/service"
)

func TestS3StreamOutputConfigParsing(t *testing.T) {
	tests := []struct {
		name        string
		config      string
		expectError bool
	}{
		{
			name: "basic config",
			config: `
bucket: test-bucket
path: 'logs/${! timestamp_unix() }.log'
`,
			expectError: false,
		},
		{
			name: "with partition_by",
			config: `
bucket: test-bucket
path: 'logs/date=${! meta("date") }/${! uuid_v4() }.log'
partition_by:
  - '${! meta("date") }'
  - '${! meta("account") }'
`,
			expectError: false,
		},
		{
			name: "with buffer settings",
			config: `
bucket: test-bucket
path: 'data/${! timestamp_unix() }.json'
max_buffer_bytes: 5242880
max_buffer_count: 5000
max_buffer_period: 5s
`,
			expectError: false,
		},
		{
			name: "with content type",
			config: `
bucket: test-bucket
path: 'logs/${! timestamp_unix() }.log'
content_type: 'application/json'
`,
			expectError: false,
		},
		{
			name: "with content encoding",
			config: `
bucket: test-bucket
path: 'logs/${! timestamp_unix() }.log.gz'
content_type: 'application/json'
content_encoding: 'gzip'
`,
			expectError: false,
		},
		{
			name: "missing bucket",
			config: `
path: 'logs/${! timestamp_unix() }.log'
`,
			expectError: true,
		},
		{
			name: "missing path",
			config: `
bucket: test-bucket
`,
			expectError: true,
		},
		{
			name: "invalid period",
			config: `
bucket: test-bucket
path: 'logs/${! timestamp_unix() }.log'
max_buffer_period: 'invalid'
`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := s3StreamOutputSpec()
			parsedConf, err := spec.ParseYAML(tt.config, nil)

			if tt.expectError && err != nil {
				// Error during parsing is acceptable for error test cases
				return
			}

			require.NoError(t, err)

			conf, err := s3StreamConfigFromParsed(parsedConf)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotEmpty(t, conf.Bucket)
			}
		})
	}
}

func TestS3StreamOutputPathInterpolation(t *testing.T) {
	configYAML := `
bucket: test-bucket
path: 'logs/date=${! meta("date") }/service=${! meta("service") }/${! uuid_v4() }.log'
partition_by:
  - '${! meta("date") }'
  - '${! meta("service") }'
`

	spec := s3StreamOutputSpec()
	parsedConf, err := spec.ParseYAML(configYAML, nil)
	require.NoError(t, err)

	conf, err := s3StreamConfigFromParsed(parsedConf)
	require.NoError(t, err)

	assert.Equal(t, "test-bucket", conf.Bucket)
	assert.NotNil(t, conf.Path)
	assert.Len(t, conf.PartitionBy, 2)

	// Test path interpolation with batch
	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"message": "test1"}`)),
		service.NewMessage([]byte(`{"message": "test2"}`)),
	}

	batch[0].MetaSet("date", "2026-01-20")
	batch[0].MetaSet("service", "api")
	batch[1].MetaSet("date", "2026-01-20")
	batch[1].MetaSet("service", "web")

	// Evaluate partition_by for first message
	part0, err := batch.TryInterpolatedString(0, conf.PartitionBy[0])
	require.NoError(t, err)
	assert.Equal(t, "2026-01-20", part0)

	part1, err := batch.TryInterpolatedString(0, conf.PartitionBy[1])
	require.NoError(t, err)
	assert.Equal(t, "api", part1)

	// Evaluate partition_by for second message
	part2, err := batch.TryInterpolatedString(1, conf.PartitionBy[0])
	require.NoError(t, err)
	assert.Equal(t, "2026-01-20", part2)

	part3, err := batch.TryInterpolatedString(1, conf.PartitionBy[1])
	require.NoError(t, err)
	assert.Equal(t, "web", part3)

	// Path should contain interpolated values
	path0, err := batch.TryInterpolatedString(0, conf.Path)
	require.NoError(t, err)
	assert.Contains(t, path0, "date=2026-01-20")
	assert.Contains(t, path0, "service=api")

	path1, err := batch.TryInterpolatedString(1, conf.Path)
	require.NoError(t, err)
	assert.Contains(t, path1, "date=2026-01-20")
	assert.Contains(t, path1, "service=web")
}

func TestS3StreamOutputPartitionGrouping(t *testing.T) {
	configYAML := `
bucket: test-bucket
path: 'logs/date=${! meta("date") }/${! uuid_v4() }.log'
partition_by:
  - '${! meta("date") }'
`

	spec := s3StreamOutputSpec()
	parsedConf, err := spec.ParseYAML(configYAML, nil)
	require.NoError(t, err)

	conf, err := s3StreamConfigFromParsed(parsedConf)
	require.NoError(t, err)

	// Create batch with 4 messages, 2 dates
	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"msg": "1"}`)),
		service.NewMessage([]byte(`{"msg": "2"}`)),
		service.NewMessage([]byte(`{"msg": "3"}`)),
		service.NewMessage([]byte(`{"msg": "4"}`)),
	}

	batch[0].MetaSet("date", "2026-01-20")
	batch[1].MetaSet("date", "2026-01-21")
	batch[2].MetaSet("date", "2026-01-20")
	batch[3].MetaSet("date", "2026-01-21")

	// Simulate partition grouping logic
	type partitionGroup struct {
		key  string
		path string
		msgs service.MessageBatch
	}
	partitionMap := make(map[string]*partitionGroup)

	for i, msg := range batch {
		partitionParts := make([]string, len(conf.PartitionBy))
		for j, partExpr := range conf.PartitionBy {
			var err error
			partitionParts[j], err = batch.TryInterpolatedString(i, partExpr)
			require.NoError(t, err)
		}
		partitionKey := partitionParts[0]

		var fullPath string
		if pg, exists := partitionMap[partitionKey]; exists {
			fullPath = pg.path
		} else {
			fullPath, err = batch.TryInterpolatedString(i, conf.Path)
			require.NoError(t, err)
		}

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

	// Verify partitioning
	assert.Len(t, partitionMap, 2, "Should have 2 partitions")

	pg1 := partitionMap["2026-01-20"]
	require.NotNil(t, pg1)
	assert.Len(t, pg1.msgs, 2, "Partition 2026-01-20 should have 2 messages")
	assert.Contains(t, pg1.path, "date=2026-01-20")

	pg2 := partitionMap["2026-01-21"]
	require.NotNil(t, pg2)
	assert.Len(t, pg2.msgs, 2, "Partition 2026-01-21 should have 2 messages")
	assert.Contains(t, pg2.path, "date=2026-01-21")

	// Verify paths are different (UUID should be different per partition)
	assert.NotEqual(t, pg1.path, pg2.path, "Paths should be different for different partitions")
}

func TestS3StreamOutputBufferSettings(t *testing.T) {
	tests := []struct {
		name              string
		config            string
		expectedBytes     int64
		expectedCount     int
		expectedPeriodStr string
	}{
		{
			name: "default values",
			config: `
bucket: test-bucket
path: 'logs/test.log'
`,
			expectedBytes:     10 * 1024 * 1024, // 10MB
			expectedCount:     10000,
			expectedPeriodStr: "10s",
		},
		{
			name: "custom values",
			config: `
bucket: test-bucket
path: 'logs/test.log'
max_buffer_bytes: 5242880
max_buffer_count: 5000
max_buffer_period: 5s
`,
			expectedBytes:     5242880,
			expectedCount:     5000,
			expectedPeriodStr: "5s",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := s3StreamOutputSpec()
			parsedConf, err := spec.ParseYAML(tt.config, nil)
			require.NoError(t, err)

			conf, err := s3StreamConfigFromParsed(parsedConf)
			require.NoError(t, err)

			assert.Equal(t, tt.expectedBytes, conf.MaxBufferBytes)
			assert.Equal(t, tt.expectedCount, conf.MaxBufferCount)
			assert.Equal(t, tt.expectedPeriodStr, conf.MaxBufferPeriod.String())
		})
	}
}

func TestS3StreamOutputContentSettings(t *testing.T) {
	configYAML := `
bucket: test-bucket
path: 'logs/test.log.gz'
content_type: 'application/json'
content_encoding: 'gzip'
`

	spec := s3StreamOutputSpec()
	parsedConf, err := spec.ParseYAML(configYAML, nil)
	require.NoError(t, err)

	conf, err := s3StreamConfigFromParsed(parsedConf)
	require.NoError(t, err)

	// Create test batch
	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"test": "data"}`)),
	}

	// Evaluate content type
	contentType, err := batch.TryInterpolatedString(0, conf.ContentType)
	require.NoError(t, err)
	assert.Equal(t, "application/json", contentType)

	// Evaluate content encoding
	require.NotNil(t, conf.ContentEncoding)
	contentEncoding, err := batch.TryInterpolatedString(0, conf.ContentEncoding)
	require.NoError(t, err)
	assert.Equal(t, "gzip", contentEncoding)
}

func TestS3StreamOutputNoPartitionBy(t *testing.T) {
	// Test backwards compatibility when partition_by is not specified
	configYAML := `
bucket: test-bucket
path: 'logs/${! meta("filename") }.log'
`

	spec := s3StreamOutputSpec()
	parsedConf, err := spec.ParseYAML(configYAML, nil)
	require.NoError(t, err)

	conf, err := s3StreamConfigFromParsed(parsedConf)
	require.NoError(t, err)

	assert.Empty(t, conf.PartitionBy, "Should have no partition_by expressions")

	// When partition_by is empty, path should be evaluated per message
	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"msg": "1"}`)),
		service.NewMessage([]byte(`{"msg": "2"}`)),
	}

	batch[0].MetaSet("filename", "file1")
	batch[1].MetaSet("filename", "file2")

	path0, err := batch.TryInterpolatedString(0, conf.Path)
	require.NoError(t, err)
	assert.Equal(t, "logs/file1.log", path0)

	path1, err := batch.TryInterpolatedString(1, conf.Path)
	require.NoError(t, err)
	assert.Equal(t, "logs/file2.log", path1)

	// Each message should create its own partition
	assert.NotEqual(t, path0, path1, "Paths should be different without partition_by")
}

func TestS3StreamOutputConsumeAppliesConfiguredBatching(t *testing.T) {
	var uploadedBodies [][]byte
	mockClient := &mockS3StreamClient{
		uploadPartFunc: func(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
			body, err := io.ReadAll(input.Body)
			require.NoError(t, err)
			uploadedBodies = append(uploadedBodies, body)
			return &s3.UploadPartOutput{
				ETag: aws.String("test-etag"),
			}, nil
		},
	}

	configYAML := `
bucket: test-bucket
path: batched.log
batching:
  count: 2
`

	parsedConf, err := s3StreamOutputSpec().ParseYAML(configYAML, nil)
	require.NoError(t, err)

	conf, err := s3StreamConfigFromParsed(parsedConf)
	require.NoError(t, err)
	batchPolicy, err := parsedConf.FieldBatchPolicy(ssoFieldBatching)
	require.NoError(t, err)

	out, err := newS3StreamOutput(conf, batchPolicy, service.MockResources())
	require.NoError(t, err)
	out.s3ClientCtor = func(s3StreamConfig) s3StreamingAPI {
		return mockClient
	}

	txChan := make(chan message.Transaction)
	require.NoError(t, out.Consume(txChan))

	ack1 := make(chan error, 1)
	ack2 := make(chan error, 1)

	txChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte("foo")}), ack1)
	select {
	case err := <-ack1:
		t.Fatalf("first transaction acked before batch flush: %v", err)
	default:
	}

	txChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte("bar")}), ack2)
	close(txChan)

	waitCtx, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	require.NoError(t, out.WaitForClose(waitCtx))

	require.Len(t, uploadedBodies, 1)
	assert.Equal(t, []byte("foobar"), uploadedBodies[0])
	require.NoError(t, <-ack1)
	require.NoError(t, <-ack2)
}

// `fallback` consumes a child output by forwarding each transaction with an
// async ack interceptor and immediately moving to the next, never blocking on a
// prior ack (output_fallback.go:221-247). That async model is what lets it work
// with our deferred-ack output, unlike `drop_on`, which blocks per-transaction
// waiting for the ack and therefore deadlocks. This verifies our real output
// behaves correctly under that contract: it makes progress without deadlock, a
// deferred part-upload failure surfaces as a nack (so a fallback could route it),
// and the writer resumes past the failure and completes the file.
func TestS3StreamOutputComposesWithFallbackStyleFeeder(t *testing.T) {
	var uploadCalls int
	var completed bool
	mockClient := &mockS3StreamClient{
		uploadPartFunc: func(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
			uploadCalls++
			if uploadCalls == 2 {
				return nil, errors.New("poison part upload")
			}
			return &s3.UploadPartOutput{ETag: aws.String("test-etag")}, nil
		},
		completeMultipartUploadFunc: func(ctx context.Context, input *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
			completed = true
			return &s3.CompleteMultipartUploadOutput{}, nil
		},
	}

	// 5 MiB buffer + max_retries:0 so each 6 MiB message seals as its own part
	// and a failed upload fails fast (no backoff stalling the loop).
	configYAML := `
bucket: test-bucket
path: static.log
max_buffer_bytes: 5242880
max_retries: 0
`
	parsedConf, err := s3StreamOutputSpec().ParseYAML(configYAML, nil)
	require.NoError(t, err)
	conf, err := s3StreamConfigFromParsed(parsedConf)
	require.NoError(t, err)
	out, err := newS3StreamOutput(conf, service.BatchPolicy{}, service.MockResources())
	require.NoError(t, err)
	out.s3ClientCtor = func(s3StreamConfig) s3StreamingAPI { return mockClient }

	txChan := make(chan message.Transaction)
	require.NoError(t, out.Consume(txChan))

	// Forward a transaction with an async interceptor ack and do NOT block on it
	// (exactly how fallback feeds its child).
	send := func() chan error {
		res := make(chan error, 1)
		ackFn := func(ctx context.Context, err error) error {
			res <- err
			return nil
		}
		txChan <- message.NewTransactionFunc(message.QuickBatch([][]byte{make([]byte, 6*1024*1024)}), ackFn)
		return res
	}

	res1 := send() // part 1 uploads, ack held (deferred)
	res2 := send() // releases res1 (acked), seals part 2, upload FAILS -> res2 nacked
	res3 := send() // resumes (reuses part 2 number), uploads, ack held until close
	close(txChan)

	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, out.WaitForClose(waitCtx), "must not deadlock under fallback-style feeding")

	getRes := func(c chan error) (error, bool) {
		select {
		case e := <-c:
			return e, true
		case <-time.After(time.Second):
			return nil, false
		}
	}
	e1, ok1 := getRes(res1)
	e2, ok2 := getRes(res2)
	e3, ok3 := getRes(res3)
	require.True(t, ok1 && ok2 && ok3, "all transactions must resolve (no deadlock/leak)")
	assert.NoError(t, e1, "durable message acked")
	assert.Error(t, e2, "poison part nacked so a fallback output could route it")
	assert.NoError(t, e3, "writer resumed past the failure; this message acked")
	assert.True(t, completed, "file completed despite the poison part")
	assert.Equal(t, 3, uploadCalls)
}

// A batch spanning multiple partition keys must resolve its upstream
// transaction exactly once even when a partition fails. Previously a partial
// failure left the combined-ack counter unable to reach zero, so the
// transaction was never acked or nacked (leaked in-flight).
func TestS3StreamOutputPartitionFailureNacksWholeBatchOnce(t *testing.T) {
	mockClient := &mockS3StreamClient{
		// Fail recovery listing so every writer's Initialize() errors.
		listMultipartUploadsFunc: func(ctx context.Context, input *s3.ListMultipartUploadsInput, opts ...func(*s3.Options)) (*s3.ListMultipartUploadsOutput, error) {
			return nil, errors.New("list failed")
		},
	}

	configYAML := `
bucket: test-bucket
path: '${! meta("file") }.log'
`
	parsedConf, err := s3StreamOutputSpec().ParseYAML(configYAML, nil)
	require.NoError(t, err)
	conf, err := s3StreamConfigFromParsed(parsedConf)
	require.NoError(t, err)

	out, err := newS3StreamOutput(conf, service.BatchPolicy{}, service.MockResources())
	require.NoError(t, err)
	out.s3ClientCtor = func(s3StreamConfig) s3StreamingAPI { return mockClient }
	require.NoError(t, out.Connect(context.Background()))

	// Two messages routed to two distinct partition keys ("a.log", "b.log").
	m0 := service.NewMessage([]byte("aaa"))
	m0.MetaSet("file", "a")
	m1 := service.NewMessage([]byte("bbb"))
	m1.MetaSet("file", "b")

	var rootCalls int
	var rootErr error
	rootAck := func(ctx context.Context, e error) error {
		rootCalls++
		rootErr = e
		return nil
	}

	err = out.writeBatch(context.Background(), service.MessageBatch{m0, m1}, rootAck)
	require.Error(t, err)
	assert.Equal(t, 1, rootCalls, "upstream transaction must be resolved exactly once")
	assert.Error(t, rootErr, "a partition failure must nack the whole batch")
}

// WriteBytes takes ownership of an ack the moment it is called, so its early
// failures (closed/uninitialized) must nack it rather than drop it.
func TestS3StreamingWriterEarlyFailureNacksAck(t *testing.T) {
	t.Run("not initialized", func(t *testing.T) {
		writer, err := NewS3StreamingWriter(S3StreamingWriterConfig{
			S3Client: &mockS3StreamClient{},
			Bucket:   "test-bucket",
			Key:      "test-key",
		})
		require.NoError(t, err)

		ack := &ackRecord{}
		err = writer.WriteBytes(context.Background(), []byte("data"), ack.fn)
		require.Error(t, err)
		assert.Equal(t, 1, ack.called)
		assert.Error(t, ack.err)
	})

	t.Run("closed", func(t *testing.T) {
		writer, err := NewS3StreamingWriter(S3StreamingWriterConfig{
			S3Client: &mockS3StreamClient{},
			Bucket:   "test-bucket",
			Key:      "test-key",
		})
		require.NoError(t, err)

		ctx := context.Background()
		require.NoError(t, writer.Initialize(ctx))
		require.NoError(t, writer.Close(ctx))

		ack := &ackRecord{}
		err = writer.WriteBytes(ctx, []byte("data"), ack.fn)
		require.Error(t, err)
		assert.Equal(t, 1, ack.called)
		assert.Error(t, ack.err)
	})
}
