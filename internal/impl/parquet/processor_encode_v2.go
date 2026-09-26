package parquet

import (
	"github.com/warpstreamlabs/bento/public/service"
)

func newParquetEncodeV2ProcessorFromConfig(
	conf *service.ParsedConfig,
	logger *service.Logger,
) (*parquetEncodeProcessor, error) {

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

	enc := &nodeEncoder{
		schema: schema,
	}

	return &parquetEncodeProcessor{
		logger:          logger,
		schema:          schema,
		compressionType: opts.compression,
		encoder:         enc,
	}, nil
}
