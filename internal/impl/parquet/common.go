package parquet

import (
	"errors"
	"io"
	"unicode/utf8"

	"github.com/parquet-go/parquet-go"
)

func readLenient(rdr parquet.RowReader, schema *parquet.Schema, rows []any) (n int, err error) {
	parquetRows := make([]parquet.Row, len(rows))

	n, err = rdr.ReadRows(parquetRows)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}

	// Convert parquet.Row to map[string]any manually
	fields := schema.Fields()
	for i := 0; i < n; i++ {
		row := make(map[string]any)
		parquetRow := parquetRows[i]

		// Group values by column index
		columnValues := make(map[int][]parquet.Value)
		for _, val := range parquetRow {
			colIdx := val.Column()
			columnValues[colIdx] = append(columnValues[colIdx], val)
		}

		// Convert each field
		for fieldIdx, field := range fields {
			vals, ok := columnValues[fieldIdx]
			if !ok || len(vals) == 0 {
				continue
			}

			row[field.Name()] = convertParquetValue(vals, field)
		}

		rows[i] = row
	}

	return n, err
}

func convertParquetValue(vals []parquet.Value, field parquet.Field) any {
	if len(vals) == 0 {
		return nil
	}

	// Handle repeated fields (arrays)
	if field.Repeated() {
		result := make([]any, 0, len(vals))
		for _, val := range vals {
			if !val.IsNull() {
				result = append(result, convertSingleValue(val))
			}
		}
		return result
	}

	// Handle single value
	val := vals[0]
	if val.IsNull() {
		return nil
	}

	return convertSingleValue(val)
}

func convertSingleValue(val parquet.Value) any {
	if val.IsNull() {
		return nil
	}

	switch val.Kind() {
	case parquet.Boolean:
		return val.Boolean()
	case parquet.Int32:
		return val.Int32()
	case parquet.Int64:
		return val.Int64()
	case parquet.Float:
		return val.Float()
	case parquet.Double:
		return val.Double()
	case parquet.ByteArray:
		b := val.ByteArray()
		if utf8.Valid(b) {
			return string(b)
		}
		return b
	case parquet.FixedLenByteArray:
		return val.ByteArray()
	default:
		return val.String()
	}
}
