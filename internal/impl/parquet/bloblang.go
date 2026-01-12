package parquet

import (
	"bytes"
	"errors"
	"io"

	"github.com/parquet-go/parquet-go"
	"github.com/warpstreamlabs/bento/public/bloblang"
)

func init() {
	// Note: The examples are run and tested from within
	// ./internal/bloblang/query/parsed_test.go

	parquetParseSpec := bloblang.NewPluginSpec().
		Category("Parsing").
		Description("Decodes a [Parquet file](https://parquet.apache.org/docs/) into an array of objects, one for each row within the file.").
		Param(bloblang.NewBoolParam("byte_array_as_string").
			Description("Deprecated: This parameter is no longer used.").Default(false)).
		Param(bloblang.NewBoolParam("strict_schema").
			Description("Whether to enforce strict Parquet schema validation. When set to false, allows reading files with non-standard schema structures (such as non-standard LIST formats).").Default(true)).
		Example("", `root = content().parse_parquet()`).
		Example("With lenient schema validation", `root = content().parse_parquet(strict_schema: false)`)

	if err := bloblang.RegisterMethodV2(
		"parse_parquet", parquetParseSpec,
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			strictSchema, err := args.GetBool("strict_schema")
			if err != nil {
				return nil, err
			}

			return func(v any) (any, error) {
				b, err := bloblang.ValueAsBytes(v)
				if err != nil {
					return nil, err
				}

				rdr := bytes.NewReader(b)
				rowBuf := make([]any, 10)
				var result []any

				if strictSchema {
					pRdr, err := newReaderWithoutPanic(rdr)
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
							result = append(result, rowBuf[i])
						}
					}
				} else {
					inFile, err := parquet.OpenFile(rdr, int64(len(b)))
					if err != nil {
						return nil, err
					}

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
								result = append(result, rowBuf[i])
							}
						}
						rows.Close()
					}
				}

				return result, nil
			}, nil
		},
	); err != nil {
		panic(err)
	}
}
