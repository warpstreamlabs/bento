package parquet

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/bloblang"
)

func TestParquetParseBloblangAsStrings(t *testing.T) {
	buf := bytes.NewBuffer(nil)

	pWtr := parquet.NewGenericWriter[any](buf, parquet.NewSchema("test", parquet.Group{
		"ID": parquet.Int(64),
		"A":  parquet.Int(64),
		"B":  parquet.Int(64),
		"C":  parquet.Int(64),
		"D":  parquet.String(),
		"E":  parquet.Leaf(parquet.ByteArrayType),
	}))

	type obj map[string]any

	_, err := pWtr.Write([]any{
		obj{"ID": 1, "A": 11, "B": 21, "C": 31, "D": "first", "E": []byte("first")},
		obj{"ID": 2, "A": 12, "B": 22, "C": 32, "D": "second", "E": []byte("second")},
		obj{"ID": 3, "A": 13, "B": 23, "C": 33, "D": "third", "E": []byte("third")},
		obj{"ID": 4, "A": 14, "B": 24, "C": 34, "D": "fourth", "E": []byte("fourth")},
	})
	require.NoError(t, err)

	require.NoError(t, pWtr.Close())

	exec, err := bloblang.Parse(`root = this.parse_parquet(byte_array_as_string: true)`)
	require.NoError(t, err)

	res, err := exec.Query(buf.Bytes())
	require.NoError(t, err)

	actualDataBytes, err := json.Marshal(res)
	require.NoError(t, err)

	assert.JSONEq(t, `[
  {"ID": 1, "A": 11, "B": 21, "C": 31, "D": "first", "E": "first"},
  {"ID": 2, "A": 12, "B": 22, "C": 32, "D": "second", "E": "second"},
  {"ID": 3, "A": 13, "B": 23, "C": 33, "D": "third", "E": "third"},
  {"ID": 4, "A": 14, "B": 24, "C": 34, "D": "fourth", "E": "fourth"}
]`, string(actualDataBytes))
}

func TestParquetParseBloblangPanicInit(t *testing.T) {
	exec, err := bloblang.Parse(`root = this.parse_parquet()`)
	require.NoError(t, err)

	_, err = exec.Query([]byte(`hello world lol`))
	require.Error(t, err)
}

func TestParquetParseBloblangLenientSchema(t *testing.T) {
	// Create a non-standard parquet file with repeated field (non-standard LIST)
	nonStandardSchema := parquet.NewSchema("test", parquet.Group{
		"id":     parquet.Int(64),
		"name":   parquet.String(),
		"values": parquet.Repeated(parquet.Int(64)),
	})

	buf := bytes.NewBuffer(nil)
	pWtr := parquet.NewGenericWriter[any](buf, nonStandardSchema)

	type obj map[string]any
	type arr []any

	_, err := pWtr.Write([]any{
		obj{"id": int64(1), "name": "first", "values": arr{int64(10), int64(20)}},
		obj{"id": int64(2), "name": "second", "values": arr{int64(30), int64(40), int64(50)}},
	})
	require.NoError(t, err)
	require.NoError(t, pWtr.Close())

	// Test with strict_schema: false
	exec, err := bloblang.Parse(`root = this.parse_parquet(strict_schema: false)`)
	require.NoError(t, err)

	res, err := exec.Query(buf.Bytes())
	require.NoError(t, err)

	actualDataBytes, err := json.Marshal(res)
	require.NoError(t, err)

	assert.JSONEq(t, `[
		{"id": 1, "name": "first", "values": [10, 20]},
		{"id": 2, "name": "second", "values": [30, 40, 50]}
	]`, string(actualDataBytes))
}
