package parquet

import (
	"bytes"
	"context"
	"fmt"
	"reflect"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"

	"github.com/warpstreamlabs/bento/public/service"
)

func parquetEncodeProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		// Stable(). TODO
		Categories("Parsing").
		Summary("Encodes [Parquet files](https://parquet.apache.org/docs/) from a batch of structured messages.").
		Field(parquetSchemaConfig()).
		Field(service.NewStringEnumField("default_compression",
			"uncompressed", "snappy", "gzip", "brotli", "zstd", "lz4raw",
		).
			Description("The default compression type to use for fields.").
			Default("uncompressed")).
		Field(service.NewStringEnumField("default_encoding",
			"DELTA_LENGTH_BYTE_ARRAY", "PLAIN",
		).
			Description("The default encoding type to use for fields. A custom default encoding is only necessary when consuming data with libraries that do not support `DELTA_LENGTH_BYTE_ARRAY` and is therefore best left unset where possible.").
			Default("DELTA_LENGTH_BYTE_ARRAY").
			Advanced().
			Version("1.0.0")).
		Description(`
This processor uses [https://github.com/parquet-go/parquet-go](https://github.com/parquet-go/parquet-go), which is itself experimental. Therefore changes could be made into how this processor functions outside of major version releases.
`).
		Version("1.0.0").
		// TODO: Add an example that demonstrates error handling
		Example("Writing Parquet Files to AWS S3",
			"In this example we use the batching mechanism of an `aws_s3` output to collect a batch of messages in memory, which then converts it to a parquet file and uploads it.",
			`
output:
  aws_s3:
    bucket: TODO
    path: 'stuff/${! timestamp_unix() }-${! uuid_v4() }.parquet'
    batching:
      count: 1000
      period: 10s
      processors:
        - parquet_encode:
            schema:
              - name: id
                type: INT64
              - name: weight
                type: DOUBLE
              - name: content
                type: BYTE_ARRAY
              - name: attributes
                type: MAP
                fields:
                  - { name: key, type: UTF8 }
                  - { name: value, type: INT64 }
              - name: tags
                type: LIST
                fields:
                  - { name: element, type: UTF8 }
            default_compression: zstd
`).
		Example("Using STRUCT Types for Nested Objects",
			"This example shows how to use STRUCT types to store complex nested objects natively in Parquet format, avoiding JSON serialization overhead.",
			`
output:
  aws_s3:
    bucket: TODO
    path: 'events/${! timestamp_unix() }-${! uuid_v4() }.parquet'
    batching:
      count: 1000
      period: 10s
      processors:
        - parquet_encode:
            schema:
              - name: id
                type: INT64
              - name: timestamp
                type: INT64
              - name: cloud
                type: STRUCT
                optional: true
                fields:
                  - name: provider
                    type: UTF8
                  - name: region
                    type: UTF8
                    optional: true
                  - name: account
                    type: STRUCT
                    optional: true
                    fields:
                      - name: uid
                        type: UTF8
                        optional: true
              - name: metadata
                type: STRUCT
                optional: true
                fields:
                  - name: version
                    type: UTF8
                  - name: product
                    type: STRUCT
                    optional: true
                    fields:
                      - name: name
                        type: UTF8
                      - name: version
                        type: UTF8
                        optional: true
            default_compression: zstd
`)
}

func init() {
	err := service.RegisterBatchProcessor(
		"parquet_encode", parquetEncodeProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return newParquetEncodeProcessorFromConfig(conf, mgr.Logger())
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

func parquetSchemaConfig() *service.ConfigField {
	return service.NewObjectListField("schema",
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
	).Description("Parquet schema.")
}

//------------------------------------------------------------------------------

func newParquetEncodeProcessorFromConfig(
	conf *service.ParsedConfig,
	logger *service.Logger,
) (*parquetEncodeProcessor, error) {
	compressStr, err := conf.FieldString("default_compression")
	if err != nil {
		return nil, err
	}

	var compressDefault compress.Codec
	switch compressStr {
	case "uncompressed":
		compressDefault = &parquet.Uncompressed
	case "snappy":
		compressDefault = &parquet.Snappy
	case "gzip":
		compressDefault = &parquet.Gzip
	case "brotli":
		compressDefault = &parquet.Brotli
	case "zstd":
		compressDefault = &parquet.Zstd
	case "lz4raw":
		compressDefault = &parquet.Lz4Raw
	default:
		return nil, fmt.Errorf("default_compression type %v not recognised", compressStr)
	}

	encodingStr, err := conf.FieldString("default_encoding")
	if err != nil {
		return nil, err
	}

	// Note: these values are derived from the supported parquet-go schema struct tags i.e
	// plain - enables the plain encoding (no-op default)
	// delta - enables delta encoding on the parquet column (default for string types)
	// dict  - enables dictionary encoding on the parquet column (not currently unsupported)
	var defaultEncodingTag string
	switch encodingStr {
	case parquet.Plain.String():
		defaultEncodingTag = "plain"
	case parquet.DeltaLengthByteArray.String():
		defaultEncodingTag = "delta"
	default:
		return nil, fmt.Errorf("default_encoding type %v not recognised", encodingStr)
	}

	// For the schema, we don't want any of the actual values encoded as pointers. That works
	// for 99% of things, but it doesn't work for decimal types, so we use the optional struct
	// tags approach which works for 100% of things. This is fine because even thought the
	// optional struct tags approach cant represent null values, is this just defining the schema
	// so we don't care if the struct value is a concrete type or a pointer to a concrete type.
	schemaType, err := GenerateStructType(conf, schemaOpts{
		optionalsAsStructTags: true,
		optionalAsPtrs:        false,
		defaultEncoding:       defaultEncodingTag,
	})
	if err != nil {
		return nil, fmt.Errorf(
			"failed to generate struct type from parquet schema(schema): %w", err)
	}

	// For the actual *struct values* that we're going to pass to the parquet encoder, we use
	// the pointer approach. This is fine because this struct won't be passed to parquet.SchemaOf()
	// so it won't trigger the panic in that function. Ensuring the struct used to represent
	// parquet rows uses pointers for optionals ensures that we can properly represent NULL values.
	messageType, err := GenerateStructType(conf, schemaOpts{
		optionalsAsStructTags: false,
		optionalAsPtrs:        true,
		defaultEncoding:       defaultEncodingTag,
	})
	if err != nil {
		return nil, fmt.Errorf(
			"failed to generate struct type from parquet schema(message): %w", err)
	}

	schema := parquet.SchemaOf(reflect.New(schemaType).Interface())

	return newParquetEncodeProcessor(logger, schema, compressDefault, messageType)
}

type parquetEncodeProcessor struct {
	logger          *service.Logger
	schema          *parquet.Schema
	compressionType compress.Codec
	messageType     reflect.Type
}

func newParquetEncodeProcessor(
	logger *service.Logger,
	schema *parquet.Schema,
	compressionType compress.Codec,
	messageType reflect.Type,
) (*parquetEncodeProcessor, error) {
	s := &parquetEncodeProcessor{
		logger:          logger,
		schema:          schema,
		compressionType: compressionType,
		messageType:     messageType,
	}
	return s, nil
}

func writeWithoutPanic(pWtr *parquet.GenericWriter[any], rows []any) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("encoding panic: %v", r)
		}
	}()

	_, err = pWtr.Write(rows)
	return
}

func closeWithoutPanic(pWtr *parquet.GenericWriter[any]) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("encoding panic: %v", r)
		}
	}()

	err = pWtr.Close()
	return
}

func (s *parquetEncodeProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	if len(batch) == 0 {
		return nil, nil
	}

	buf := bytes.NewBuffer(nil)
	pWtr := parquet.NewGenericWriter[any](buf, s.schema, parquet.Compression(s.compressionType))

	rows := make([]any, len(batch))
	for i, m := range batch {
		ms, err := m.AsStructured()
		if err != nil {
			return nil, err
		}

		scrubbed, isObj := scrubJSONNumbers(ms).(map[string]any)
		if !isObj {
			return nil, fmt.Errorf("unable to encode message type %T as parquet row", ms)
		}

		v := reflect.New(s.messageType)

		if err := MapToStruct(scrubbed, v.Interface()); err != nil {
			return nil, fmt.Errorf("conversion to struct failed, err: %w", err)
		}

		rows[i] = v.Interface()
	}

	if err := writeWithoutPanic(pWtr, rows); err != nil {
		return nil, err
	}
	if err := closeWithoutPanic(pWtr); err != nil {
		return nil, err
	}

	outMsg := batch[0]
	outMsg.SetBytes(buf.Bytes())
	return []service.MessageBatch{{outMsg}}, nil
}

func (s *parquetEncodeProcessor) Close(ctx context.Context) error {
	return nil
}
