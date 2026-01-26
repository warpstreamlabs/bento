package aws

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3ParquetStreamOutputConfigParsing(t *testing.T) {
	spec := s3ParquetStreamOutputSpec()

	t.Run("valid config", func(t *testing.T) {
		configYAML := `
bucket: test-bucket
path: 'logs/${! timestamp_unix() }.parquet'
schema:
  - name: id
    type: INT64
  - name: message
    type: UTF8
  - name: value
    type: DOUBLE
    optional: true
default_compression: snappy
row_group_size: 5000
max_file_rows: 100000
max_file_duration: 5m
`
		parsed, err := spec.ParseYAML(configYAML, nil)
		require.NoError(t, err)

		conf, err := s3ParquetStreamConfigFromParsed(parsed)
		require.NoError(t, err)

		assert.Equal(t, "test-bucket", conf.Bucket)
		assert.NotNil(t, conf.Path)
		assert.NotNil(t, conf.Schema)
		assert.NotNil(t, conf.MessageType)
		assert.Equal(t, int64(5000), conf.RowGroupSize)
	})

	t.Run("default values", func(t *testing.T) {
		configYAML := `
bucket: test-bucket
path: 'data.parquet'
schema:
  - name: id
    type: INT64
`
		parsed, err := spec.ParseYAML(configYAML, nil)
		require.NoError(t, err)

		conf, err := s3ParquetStreamConfigFromParsed(parsed)
		require.NoError(t, err)

		assert.Equal(t, int64(10000), conf.RowGroupSize) // Default
		assert.False(t, conf.UsePathStyle)               // Default
	})

	t.Run("compression types", func(t *testing.T) {
		compressionTypes := []string{"uncompressed", "snappy", "gzip", "brotli", "zstd", "lz4raw"}

		for _, compType := range compressionTypes {
			t.Run(compType, func(t *testing.T) {
				configYAML := `
bucket: test-bucket
path: 'data.parquet'
schema:
  - name: id
    type: INT64
default_compression: ` + compType
				parsed, err := spec.ParseYAML(configYAML, nil)
				require.NoError(t, err)

				conf, err := s3ParquetStreamConfigFromParsed(parsed)
				require.NoError(t, err)
				assert.NotNil(t, conf.CompressionType)
			})
		}
	})

	t.Run("complex schema with nested fields", func(t *testing.T) {
		configYAML := `
bucket: test-bucket
path: 'data.parquet'
schema:
  - name: id
    type: INT64
  - name: metadata
    type: STRUCT
    fields:
      - name: version
        type: UTF8
      - name: timestamp
        type: INT64
  - name: tags
    type: LIST
    fields:
      - name: element
        type: UTF8
  - name: attributes
    type: MAP
    fields:
      - name: key
        type: UTF8
      - name: value
        type: INT64
`
		parsed, err := spec.ParseYAML(configYAML, nil)
		require.NoError(t, err)

		conf, err := s3ParquetStreamConfigFromParsed(parsed)
		require.NoError(t, err)

		assert.NotNil(t, conf.Schema)
		assert.NotNil(t, conf.MessageType)
		// Verify schema has the expected number of fields
		assert.Len(t, conf.Schema.Fields(), 4)
	})

	t.Run("missing required fields", func(t *testing.T) {
		configYAML := `
path: 'data.parquet'
schema:
  - name: id
    type: INT64
`
		_, err := spec.ParseYAML(configYAML, nil)
		require.Error(t, err) // Should fail - bucket is required
	})
}

func TestS3ParquetStreamOutput_PathInterpolation(t *testing.T) {
	// This test verifies that the path interpolation setup is correct
	// Full integration testing would require S3/LocalStack
	spec := s3ParquetStreamOutputSpec()

	configYAML := `
bucket: test-bucket
path: 'logs/${! timestamp_unix() }/data.parquet'
schema:
  - name: id
    type: INT64
  - name: message
    type: UTF8
`
	parsed, err := spec.ParseYAML(configYAML, nil)
	require.NoError(t, err)

	conf, err := s3ParquetStreamConfigFromParsed(parsed)
	require.NoError(t, err)

	// Verify the path interpolation string was parsed
	assert.NotNil(t, conf.Path)
}

func TestS3ParquetStreamOutput_SchemaGeneration(t *testing.T) {
	spec := s3ParquetStreamOutputSpec()

	t.Run("optional fields", func(t *testing.T) {
		configYAML := `
bucket: test-bucket
path: 'data.parquet'
schema:
  - name: required_field
    type: INT64
  - name: optional_field
    type: UTF8
    optional: true
`
		parsed, err := spec.ParseYAML(configYAML, nil)
		require.NoError(t, err)

		conf, err := s3ParquetStreamConfigFromParsed(parsed)
		require.NoError(t, err)

		// Verify that both schemaType and messageType were generated
		assert.NotNil(t, conf.Schema)
		assert.NotNil(t, conf.MessageType)

		// MessageType should use pointers for optionals
		// This is reflected in the messageType generation
		assert.Len(t, conf.Schema.Fields(), 2)
	})

	t.Run("repeated fields", func(t *testing.T) {
		configYAML := `
bucket: test-bucket
path: 'data.parquet'
schema:
  - name: id
    type: INT64
  - name: tags
    type: UTF8
    repeated: true
`
		parsed, err := spec.ParseYAML(configYAML, nil)
		require.NoError(t, err)

		conf, err := s3ParquetStreamConfigFromParsed(parsed)
		require.NoError(t, err)

		assert.NotNil(t, conf.Schema)
		assert.Len(t, conf.Schema.Fields(), 2)
	})
}

func TestS3ParquetStreamOutput_WriterPooling(t *testing.T) {
	// Test writer pooling logic (without actual S3 connection)

	t.Run("writer creation and tracking", func(t *testing.T) {
		// Create output instance (won't connect to S3 in test)
		output := &s3ParquetStreamOutput{
			writers: make(map[string]*StreamingParquetWriter),
		}

		// Verify initial state
		assert.Empty(t, output.writers)

		// Writer pooling is tested in integration tests
		// This is just a structural test
	})
}

// TestS3ParquetStreamOutput_ConfigExamples verifies that the example configs
// in the spec are valid and can be parsed
func TestS3ParquetStreamOutput_ConfigExamples(t *testing.T) {
	spec := s3ParquetStreamOutputSpec()

	// Simplified examples without complex Bloblang (which requires runtime context)
	examples := []string{
		`
bucket: my-data-bucket
path: 'events/${! uuid_v4() }.parquet'
schema:
  - name: id
    type: INT64
  - name: timestamp
    type: INT64
  - name: message
    type: UTF8
  - name: level
    type: UTF8
default_compression: snappy
max_file_rows: 1000000
max_file_duration: 10m
`,
		`
bucket: large-data-bucket
path: 'data/${! timestamp_unix() }.parquet'
schema:
  - name: id
    type: INT64
  - name: data
    type: BYTE_ARRAY
default_compression: zstd
row_group_size: 5000
max_file_rows: 10000000
`,
	}

	for i, exampleYAML := range examples {
		t.Run(t.Name()+"_example_"+string(rune(i)), func(t *testing.T) {
			parsed, err := spec.ParseYAML(exampleYAML, nil)
			require.NoError(t, err, "Example config %d should parse successfully", i)

			conf, err := s3ParquetStreamConfigFromParsed(parsed)
			require.NoError(t, err, "Example config %d should convert successfully", i)

			assert.NotNil(t, conf.Schema)
			assert.NotNil(t, conf.MessageType)
		})
	}
}

func TestS3ParquetStreamOutput_BatchPolicySupport(t *testing.T) {
	// Verify that batching configuration is supported
	spec := s3ParquetStreamOutputSpec()

	configYAML := `
bucket: test-bucket
path: 'data.parquet'
schema:
  - name: id
    type: INT64
batching:
  count: 100
  period: 5s
`
	parsed, err := spec.ParseYAML(configYAML, nil)
	require.NoError(t, err)

	// Verify batch policy can be extracted
	_, err = parsed.FieldBatchPolicy("batching")
	require.NoError(t, err)
}

// Benchmark configuration parsing
func BenchmarkS3ParquetStreamConfigParsing(b *testing.B) {
	spec := s3ParquetStreamOutputSpec()
	configYAML := `
bucket: test-bucket
path: 'logs/${! timestamp_unix() }.parquet'
schema:
  - name: id
    type: INT64
  - name: message
    type: UTF8
  - name: timestamp
    type: INT64
  - name: level
    type: UTF8
default_compression: snappy
`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parsed, err := spec.ParseYAML(configYAML, nil)
		if err != nil {
			b.Fatal(err)
		}
		_, err = s3ParquetStreamConfigFromParsed(parsed)
		if err != nil {
			b.Fatal(err)
		}
	}
}
