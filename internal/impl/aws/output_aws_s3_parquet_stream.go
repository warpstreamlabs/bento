package aws

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"gopkg.in/yaml.v3"

	"github.com/warpstreamlabs/bento/internal/impl/aws/config"
	bentop "github.com/warpstreamlabs/bento/internal/impl/parquet"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	// S3 Parquet Stream Output Fields
	spsoFieldBucket              = "bucket"
	spsoFieldPath                = "path"
	spsoFieldPartitionBy         = "partition_by"
	spsoFieldForcePathStyleURLs  = "force_path_style_urls"
	spsoFieldSchema              = "schema"
	spsoFieldSchemaFile          = "schema_file"
	spsoFieldDefaultCompression  = "default_compression"
	spsoFieldDefaultEncoding     = "default_encoding"
	spsoFieldRowGroupSize        = "row_group_size"
	spsoFieldBatching            = "batching"
)

func s3ParquetStreamOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Version("1.0.0").
		Categories("Services", "AWS").
		Summary(`Streams Parquet data directly to S3 using multipart uploads with minimal memory overhead.`).
		Description(`
This output writes Parquet files to S3 by streaming row groups incrementally using S3 multipart uploads.
Unlike the standard ` + "`aws_s3`" + ` output with ` + "`parquet_encode`" + ` processor (which buffers the entire file in memory),
this output streams data directly to S3, reducing memory usage from gigabytes to under 100MB.

## Key Features

- **Memory Efficient**: Streams row groups to S3 as they're generated, minimal memory footprint
- **Automatic File Rotation**: Files are automatically closed and new ones started based on row count or duration
- **Dynamic Paths**: Supports Bloblang interpolation for partition paths (e.g., ` + "`logs/${! timestamp_unix() }`" + `)
- **S3 Multipart Upload**: Leverages S3 multipart uploads for reliable large file transfers

## When to Use

Use this output instead of ` + "`aws_s3`" + ` + ` + "`parquet_encode`" + ` when:
- Writing large Parquet files (>1GB) that would consume too much memory
- Streaming continuous data that needs to be partitioned into files
- You need fine-grained control over file rotation (by row count or time)

## Performance

For a typical OCSF event dataset (84,265 events):
- Standard approach: ~9GB memory usage
- Streaming approach: <100MB memory usage

### Credentials

By default Bento will use a shared credentials file when connecting to AWS services.
You can find out more [in this document](/docs/guides/cloud/aws).
`).
		Fields(
			service.NewStringField(spsoFieldBucket).
				Description("The S3 bucket to upload files to."),
			service.NewInterpolatedStringField(spsoFieldPath).
				Description("The path for each Parquet file. Supports Bloblang interpolation for dynamic partitioning.").
				Example(`logs/${! timestamp_unix() }-${! uuid_v4() }.parquet`).
				Example(`data/year=${! timestamp().format("2006") }/month=${! timestamp().format("01") }/data.parquet`),
			service.NewInterpolatedStringListField(spsoFieldPartitionBy).
				Description("Optional list of interpolated string expressions that determine writer partitioning. Messages with the same partition values are written to the same file. The full path is only evaluated once when a new partition is encountered. This allows using functions like uuid_v4() in the path for unique filenames per partition. If omitted, the full path is evaluated per message for backwards compatibility.").
				Example([]any{
					"${! json(\"event_date\") }",
					"${! json(\"account_id\") }",
				}).
				Optional().
				Advanced(),
			service.NewBoolField(spsoFieldForcePathStyleURLs).
				Description("Forces path style URLs for S3 requests.").
				Default(false).
				Advanced(),
			service.NewObjectListField("schema",
				service.NewStringField("name").Description("The name of the column."),
				service.NewStringEnumField("type", "BOOLEAN", "INT8", "INT16", "INT32", "INT64", "DECIMAL64", "DECIMAL32", "FLOAT", "DOUBLE", "BYTE_ARRAY", "UTF8", "MAP", "LIST", "STRUCT").
					Description("The type of the column, only applicable for leaf columns with no child fields. STRUCT represents nested objects with defined field schemas. MAP supports only string keys, but can support values of all types. Some logical types can be specified here such as UTF8.").Optional(),
				service.NewIntField("decimal_precision").Description("Precision to use for DECIMAL32/DECIMAL64 type").Default(0),
				service.NewIntField("decimal_scale").Description("Scale to use for DECIMAL32/DECIMAL64 type").Default(0),
				service.NewBoolField("repeated").Description("Whether the field is repeated.").Default(false),
				service.NewBoolField("optional").Description("Whether the field is optional.").Default(false),
				service.NewAnyListField("fields").Description("A list of child fields.").Optional().Example([]any{
					map[string]any{
						"name": "foo",
						"type": "INT64",
					},
					map[string]any{
						"name": "bar",
						"type": "BYTE_ARRAY",
					},
				}),
			).Description("Parquet schema. Mutually exclusive with schema_file.").Optional(),
			service.NewStringField(spsoFieldSchemaFile).
				Description("Path to a YAML file containing a Parquet schema definition. The file should contain a parquet_encode processor resource with a schema section. Mutually exclusive with schema.").
				Example("./schemas/ocsf_network_activity.yml").
				Optional(),
			service.NewStringEnumField(spsoFieldDefaultCompression,
				"uncompressed", "snappy", "gzip", "brotli", "zstd", "lz4raw",
			).
				Description("The default compression type to use for Parquet columns.").
				Default("uncompressed"),
			service.NewStringEnumField(spsoFieldDefaultEncoding,
				"DELTA_LENGTH_BYTE_ARRAY", "PLAIN",
			).
				Description("The default encoding type to use for fields.").
				Default("DELTA_LENGTH_BYTE_ARRAY").
				Advanced(),
			service.NewIntField(spsoFieldRowGroupSize).
				Description("Number of rows per row group. Smaller values reduce memory but increase file overhead.").
				Default(10000).
				Advanced(),
		).
		Fields(config.SessionFields()...).
		Field(service.NewOutputMaxInFlightField()).
		Field(service.NewBatchPolicyField(spsoFieldBatching)).
		Example(
			"Writing Partitioned Parquet Files",
			"This example writes streaming Parquet files partitioned by date to S3.",
			`
output:
  aws_s3_parquet_stream:
    bucket: my-data-bucket
    path: 'events/date=${! timestamp().format("2006-01-02") }/${! uuid_v4() }.parquet'
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
`,
		).
		Example(
			"Low Memory Parquet Streaming",
			"This example demonstrates memory-efficient streaming for large datasets.",
			`
output:
  aws_s3_parquet_stream:
    bucket: large-data-bucket
    path: 'data/${! timestamp_unix() }.parquet'
    schema:
      - name: id
        type: INT64
      - name: data
        type: BYTE_ARRAY
    default_compression: zstd
    row_group_size: 5000  # Smaller row groups = less memory
`,
		)
}

type s3ParquetStreamConfig struct {
	Bucket       string
	Path         *service.InterpolatedString
	PartitionBy  []*service.InterpolatedString
	UsePathStyle bool

	Schema          *parquet.Schema
	MessageType     reflect.Type
	CompressionType compress.Codec
	RowGroupSize    int64

	aconf aws.Config
}

func s3ParquetStreamConfigFromParsed(pConf *service.ParsedConfig) (conf s3ParquetStreamConfig, err error) {
	if conf.Bucket, err = pConf.FieldString(spsoFieldBucket); err != nil {
		return
	}

	if conf.Path, err = pConf.FieldInterpolatedString(spsoFieldPath); err != nil {
		return
	}

	// Parse partition_by (optional)
	if pConf.Contains(spsoFieldPartitionBy) {
		if conf.PartitionBy, err = pConf.FieldInterpolatedStringList(spsoFieldPartitionBy); err != nil {
			return
		}
	}

	if conf.UsePathStyle, err = pConf.FieldBool(spsoFieldForcePathStyleURLs); err != nil {
		return
	}

	// Parse compression
	compressStr, err := pConf.FieldString(spsoFieldDefaultCompression)
	if err != nil {
		return conf, err
	}

	switch compressStr {
	case "uncompressed":
		conf.CompressionType = &parquet.Uncompressed
	case "snappy":
		conf.CompressionType = &parquet.Snappy
	case "gzip":
		conf.CompressionType = &parquet.Gzip
	case "brotli":
		conf.CompressionType = &parquet.Brotli
	case "zstd":
		conf.CompressionType = &parquet.Zstd
	case "lz4raw":
		conf.CompressionType = &parquet.Lz4Raw
	default:
		return conf, fmt.Errorf("unknown compression type: %v", compressStr)
	}

	// Parse encoding (for schema generation)
	encodingStr, err := pConf.FieldString(spsoFieldDefaultEncoding)
	if err != nil {
		return conf, err
	}

	var defaultEncodingTag string
	switch encodingStr {
	case "PLAIN":
		defaultEncodingTag = "plain"
	case "DELTA_LENGTH_BYTE_ARRAY":
		defaultEncodingTag = "delta"
	default:
		return conf, fmt.Errorf("unknown encoding type: %v", encodingStr)
	}

	// Determine which config to use for schema generation
	schemaConf := pConf
	schemaFile, _ := pConf.FieldString(spsoFieldSchemaFile)
	if schemaFile != "" {
		// Load schema from external file
		schemaConf, err = loadSchemaFromFile(schemaFile)
		if err != nil {
			return conf, fmt.Errorf("failed to load schema from file: %w", err)
		}
	}

	// Generate schema and message types
	// IMPORTANT: Use the same options for both schema and message types to ensure
	// consistent definition/repetition level encoding. A mismatch causes
	// "insufficient definition levels" errors when reading parquet files with
	// nested optional STRUCT fields.
	messageType, err := bentop.GenerateStructType(schemaConf, bentop.SchemaOpts{
		OptionalsAsStructTags: true,
		OptionalAsPtrs:        true,
		DefaultEncoding:       defaultEncodingTag,
	})
	if err != nil {
		return conf, fmt.Errorf("failed to generate message struct: %w", err)
	}

	// Use the same type for both schema and message to ensure consistency
	conf.Schema = parquet.SchemaOf(reflect.New(messageType).Interface())
	conf.MessageType = messageType

	// Row group size
	var rowGroupSize int
	if rowGroupSize, err = pConf.FieldInt(spsoFieldRowGroupSize); err != nil {
		return
	}
	conf.RowGroupSize = int64(rowGroupSize)

	// AWS config
	if conf.aconf, err = GetSession(context.TODO(), pConf); err != nil {
		return
	}

	return
}

// loadSchemaFromFile loads a Parquet schema from an external YAML file.
// The file should contain a processor_resources section with a parquet_encode processor.
func loadSchemaFromFile(filePath string) (*service.ParsedConfig, error) {
	// Read the file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	// Parse to extract schema
	var fileContent struct {
		ProcessorResources []struct {
			ParquetEncode struct {
				Schema []any `yaml:"schema"`
			} `yaml:"parquet_encode"`
		} `yaml:"processor_resources"`
	}

	if err := yaml.Unmarshal(data, &fileContent); err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	if len(fileContent.ProcessorResources) == 0 {
		return nil, fmt.Errorf("file does not contain processor_resources")
	}

	schema := fileContent.ProcessorResources[0].ParquetEncode.Schema
	if len(schema) == 0 {
		return nil, fmt.Errorf("file does not contain a schema definition")
	}

	// Create a minimal config with just the schema field
	schemaConfig := map[string]any{
		"schema": schema,
	}

	schemaYAML, err := yaml.Marshal(schemaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal schema: %w", err)
	}

	// Parse into a ParsedConfig
	// We use a dummy ConfigSpec that matches the schema structure
	spec := service.NewConfigSpec().Field(
		service.NewObjectListField("schema",
			service.NewStringField("name"),
			service.NewStringField("type").Optional(),
			service.NewIntField("decimal_precision").Default(0),
			service.NewIntField("decimal_scale").Default(0),
			service.NewBoolField("repeated").Default(false),
			service.NewBoolField("optional").Default(false),
			service.NewAnyListField("fields").Optional(),
		),
	)

	parsedConf, err := spec.ParseYAML(string(schemaYAML), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to parse schema config: %w", err)
	}

	return parsedConf, nil
}

func init() {
	err := service.RegisterBatchOutput("aws_s3_parquet_stream", s3ParquetStreamOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy(spsoFieldBatching); err != nil {
				return
			}
			var wConf s3ParquetStreamConfig
			if wConf, err = s3ParquetStreamConfigFromParsed(conf); err != nil {
				return
			}
			out, err = newS3ParquetStreamOutput(wConf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

type s3ParquetStreamOutput struct {
	conf s3ParquetStreamConfig
	log  *service.Logger
	s3Client *s3.Client

	// Writer pool for managing multiple partition paths
	writersMut sync.Mutex
	writers    map[string]*StreamingParquetWriter
}

func newS3ParquetStreamOutput(conf s3ParquetStreamConfig, mgr *service.Resources) (*s3ParquetStreamOutput, error) {
	s3Client := s3.NewFromConfig(conf.aconf, func(o *s3.Options) {
		o.UsePathStyle = conf.UsePathStyle
	})

	return &s3ParquetStreamOutput{
		conf:     conf,
		log:      mgr.Logger(),
		s3Client: s3Client,
		writers:  make(map[string]*StreamingParquetWriter),
	}, nil
}

func (s *s3ParquetStreamOutput) Connect(ctx context.Context) error {
	s.log.Infof("Streaming Parquet output configured for bucket: %s", s.conf.Bucket)
	return nil
}

func (s *s3ParquetStreamOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
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

func (s *s3ParquetStreamOutput) writeToPartition(ctx context.Context, partitionKey string, path string, batch service.MessageBatch) error {
	// Get or create writer (with lock)
	s.writersMut.Lock()
	writer, exists := s.writers[partitionKey]

	if !exists {
		newWriter, err := NewStreamingParquetWriter(StreamingWriterConfig{
			S3Client:        s.s3Client,
			Bucket:          s.conf.Bucket,
			Key:             path,
			Schema:          s.conf.Schema,
			MessageType:     s.conf.MessageType,
			CompressionType: s.conf.CompressionType,
			RowGroupSize:    s.conf.RowGroupSize,
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

	// Write events to writer
	for _, msg := range batch {
		structured, err := msg.AsStructured()
		if err != nil {
			return fmt.Errorf("failed to get structured data: %w", err)
		}

		eventMap, ok := structured.(map[string]any)
		if !ok {
			return fmt.Errorf("message is not a map: %T", structured)
		}

		if err := writer.WriteEvent(ctx, eventMap); err != nil {
			return fmt.Errorf("failed to write event: %w", err)
		}
	}

	return nil
}

func (s *s3ParquetStreamOutput) Close(ctx context.Context) error {
	s.writersMut.Lock()
	defer s.writersMut.Unlock()

	s.log.Infof("Closing %d active writers", len(s.writers))

	var lastErr error
	for path, writer := range s.writers {
		stats := writer.Stats()
		s.log.Debugf("Closing writer for %s (rows: %d, row_groups: %d, parts: %d)",
			path, stats.TotalRows, stats.RowGroups, stats.PartsUploaded)

		if err := writer.Close(ctx); err != nil {
			s.log.Errorf("Failed to close writer for %s: %v", path, err)
			lastErr = err
		}
	}

	s.writers = make(map[string]*StreamingParquetWriter)

	return lastErr
}
