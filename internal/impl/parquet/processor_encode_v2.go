package parquet

import (
	"bytes"
	"context"
	"fmt"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"

	"github.com/warpstreamlabs/bento/public/service"
)

func init() {
	err := service.RegisterBatchProcessor(
		"parquet_encode_v2", parquetEncodeProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return newParquetEncodeProcessorFromConfig(conf, mgr.Logger())
		})
	if err != nil {
		panic(err)
	}
}

// ------------------------------------------------------------------------------

type parquetEncodeV2Processor struct {
	logger          *service.Logger
	schema          *parquet.Schema
	compressionType compress.Codec
}

//------------------------------------------------------------------------------

func newParquetEncodeV2ProcessorFromConfig(
	conf *service.ParsedConfig,
	logger *service.Logger,
) (*parquetEncodeV2Processor, error) {

	opts, err := parseSchemaOpts(conf)
	if err != nil {
		return nil, err
	}

	fields, err := parseSchema(conf)
	if err != nil {
		return nil, err
	}

	schema, err := toParquetSchema(fields, opts)
	if err != nil {
		return nil, err
	}

	return &parquetEncodeV2Processor{
		logger: logger,
		schema: schema,
	}, nil
}

func (s *parquetEncodeV2Processor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
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

		// var row parquet.Row
		// row = s.schema.Deconstruct(row, scrubbed)

		rows[i] = scrubbed
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

func (s *parquetEncodeV2Processor) Close(ctx context.Context) error {
	return nil
}
