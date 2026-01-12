package parquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go"

	"github.com/warpstreamlabs/bento/public/service"
)

func parquetDecodeProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		// Stable(). TODO
		Categories("Parsing").
		Summary("Decodes [Parquet files](https://parquet.apache.org/docs/) into a batch of structured messages.").
		Field(service.NewBoolField("byte_array_as_string").
			Description("Whether to extract BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY values as strings rather than byte slices in all cases. Values with a logical type of UTF8 will automatically be extracted as strings irrespective of this field. Enabling this field makes serialising the data as JSON more intuitive as `[]byte` values are serialised as base64 encoded strings by default.").
			Default(false).Deprecated()).
		Field(service.NewBoolField("use_parquet_list_format").
			Description(`Whether to decode`+"`LIST`"+`type columns into their Parquet logical type format `+"`{\"list\": [{\"element\": value_1}, {\"element\": value_2}, ...]}` instead of a Go slice `[value_1, value_2, ...]`."+`

:::caution
This flag will be disabled `+"(set to `false`)"+` by default and deprecated in future versions, with the logical format being deprecated in favour of the Go slice.
:::`).Version("1.8.0").
			Default(true).
			Advanced()).
		Field(service.NewBoolField("strict_schema").
			Description("Whether to enforce strict Parquet schema validation. When set to false, allows reading files with non-standard schema structures (such as non-standard LIST formats). Disabling strict mode may reduce validation but increases compatibility.").
			Default(true).
			Advanced()).
		Description(`
This processor uses [https://github.com/parquet-go/parquet-go](https://github.com/parquet-go/parquet-go), which is itself experimental. Therefore changes could be made into how this processor functions outside of major version releases.`).
		Version("1.0.0").
		Example("Reading Parquet Files from AWS S3",
			"In this example we consume files from AWS S3 as they're written by listening onto an SQS queue for upload events. We make sure to use the `to_the_end` scanner which means files are read into memory in full, which then allows us to use a `parquet_decode` processor to expand each file into a batch of messages. Finally, we write the data out to local files as newline delimited JSON.",
			`
input:
  aws_s3:
    bucket: TODO
    prefix: foos/
    scanner:
      to_the_end: {}
    sqs:
      url: TODO
  processors:
    - parquet_decode: {}

output:
  file:
    codec: lines
    path: './foos/${! metadata("s3_key") }.jsonl'
`)
}

func init() {
	err := service.RegisterProcessor(
		"parquet_decode", parquetDecodeProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newParquetDecodeProcessorFromConfig(conf, mgr.Logger())
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

func newParquetDecodeProcessorFromConfig(conf *service.ParsedConfig, logger *service.Logger) (*parquetDecodeProcessor, error) {
	isLegacy, _ := conf.FieldBool("use_parquet_list_format")
	strictSchema, _ := conf.FieldBool("strict_schema")

	return &parquetDecodeProcessor{
		logger:              logger,
		useLegacyListFormat: isLegacy,
		strictSchema:        strictSchema,
	}, nil
}

type parquetDecodeProcessor struct {
	logger *service.Logger

	useLegacyListFormat bool
	strictSchema        bool
}

func newReaderWithoutPanic(r io.ReaderAt) (pRdr *parquet.GenericReader[any], err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("parquet read panic: %v", r)
		}
	}()

	pRdr = parquet.NewGenericReader[any](r)
	return
}

func readWithoutPanic(pRdr *parquet.GenericReader[any], rows []any) (n int, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("decoding panic: %v", r)
		}
	}()

	n, err = pRdr.Read(rows)
	return
}

func (s *parquetDecodeProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	mBytes, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}

	inFile, err := parquet.OpenFile(bytes.NewReader(mBytes), int64(len(mBytes)))
	if err != nil {
		return nil, err
	}

	rowBuf := make([]any, 10)
	var resBatch service.MessageBatch

	if s.strictSchema {
		pRdr, err := newReaderWithoutPanic(inFile)
		if err != nil {
			return nil, err
		}

		for {
			n, err := readWithoutPanic(pRdr, rowBuf)
			if err != nil && !errors.Is(err, io.EOF) {
				return nil, err
			}
			if n == 0 {
				break
			}

			for i := range n {
				newMsg := msg.Copy()

				if s.useLegacyListFormat {
					rowBuf[i] = transformDataWithSchema(rowBuf[i], pRdr.Schema().Fields()...)
				}

				newMsg.SetStructuredMut(rowBuf[i])
				resBatch = append(resBatch, newMsg)
			}
		}
	} else {
		schema := inFile.Schema()

		// Use RowGroups to avoid deprecated Reader
		for _, rg := range inFile.RowGroups() {
			rows := rg.Rows()

			for {
				n, err := readLenient(rows, schema, rowBuf)
				if err != nil && !errors.Is(err, io.EOF) {
					rows.Close()
					return nil, err
				}
				if n == 0 {
					break
				}

				for i := range n {
					newMsg := msg.Copy()

					if s.useLegacyListFormat {
						rowBuf[i] = transformDataWithSchema(rowBuf[i], schema.Fields()...)
					}

					newMsg.SetStructuredMut(rowBuf[i])
					resBatch = append(resBatch, newMsg)
				}
			}
			rows.Close()
		}
	}

	return resBatch, nil
}

func (s *parquetDecodeProcessor) Close(ctx context.Context) error {
	return nil
}
