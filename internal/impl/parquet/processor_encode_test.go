package parquet

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

// Before we changed this, the library used to mix and match behavior where sometimes it would quietly
// downscale the value, and other times it would not. Now we always just do a straight cast.
func TestParquetEncodeDoesNotPanic(t *testing.T) {
	// We assume that converting integers to floats and vice versa
	// that its "obvious" that there is some lossiness and that is
	// acceptable.
	encodeConf, err := parquetEncodeProcessorConfig().ParseYAML(`
schema:
  - { name: id, type: FLOAT }
  - { name: name, type: UTF8 }
`, nil)
	require.NoError(t, err)

	encodeProc, err := newParquetEncodeProcessorFromConfig(encodeConf, nil)
	require.NoError(t, err)

	tctx := context.Background()
	_, err = encodeProc.ProcessBatch(tctx, service.MessageBatch{
		service.NewMessage([]byte(`{"id":1e99,"name":"foo"}`)),
	})
	require.NoError(t, err)

	// But underflowing/overflowing integers is not acceptable and we return
	// an error.
	encodeConf, err = parquetEncodeProcessorConfig().ParseYAML(`
schema:
  - { name: id, type: INT32 }
  - { name: name, type: UTF8 }
`, nil)
	require.NoError(t, err)

	encodeProc, err = newParquetEncodeProcessorFromConfig(encodeConf, nil)
	require.NoError(t, err)

	_, err = encodeProc.ProcessBatch(tctx, service.MessageBatch{
		service.NewMessage([]byte(`{"id":1e10,"name":"foo"}`)),
	})
	require.Error(t, err)
}

func TestParquetEncodeDefaultEncodingPlain(t *testing.T) {
	encodeConf, err := parquetEncodeProcessorConfig().ParseYAML(`
schema:
  - { name: float, type: FLOAT }
  - { name: utf8, type: UTF8 }
  - { name: byte_array, type: BYTE_ARRAY }
default_encoding: PLAIN
`, nil)
	require.NoError(t, err)

	encodeProc, err := newParquetEncodeProcessorFromConfig(encodeConf, nil)
	require.NoError(t, err)

	for _, field := range encodeProc.schema.Fields() {
		if field.Name() != "float" {
			require.IsType(t, &parquet.Plain, field.Encoding())
		}

		if field.Name() == "float" {
			require.Nil(t, field.Encoding())
		}
	}

	tctx := context.Background()
	_, err = encodeProc.ProcessBatch(tctx, service.MessageBatch{
		service.NewMessage([]byte(`{"float":1e99,"utf8":"foo","byte_array":"bar"}`)),
	})
	require.NoError(t, err)
}

func TestParquetEncodeDecodeRoundTrip(t *testing.T) {
	encodeConf, err := parquetEncodeProcessorConfig().ParseYAML(`
schema:
  - { name: id, type: INT64 }
  - { name: as, type: DOUBLE, repeated: true }
  - { name: b, type: BYTE_ARRAY }
  - { name: c, type: DOUBLE }
  - { name: d, type: BOOLEAN }
  - { name: e, type: INT64, optional: true }
  - { name: f, type: INT64 }
  - { name: g, type: UTF8 }
  - name: nested_stuff
    optional: true
    fields:
      - { name: a_stuff, type: BYTE_ARRAY }
      - { name: b_stuff, type: BYTE_ARRAY }
  - { name: h, type: DECIMAL32, decimal_precision: 3, optional: True}
  - name: ob
    fields:
      - name: ob_name
        type: UTF8
      - name: bidValue
        type: FLOAT
    optional: true
`, nil)
	require.NoError(t, err)

	encodeProc, err := newParquetEncodeProcessorFromConfig(encodeConf, nil)
	require.NoError(t, err)

	decodeConf, err := parquetDecodeProcessorConfig().ParseYAML(`
byte_array_as_string: true
`, nil)
	require.NoError(t, err)

	decodeProc, err := newParquetDecodeProcessorFromConfig(decodeConf, nil)
	require.NoError(t, err)

	testParquetEncodeDecodeRoundTrip(t, encodeProc, decodeProc)
}

func TestParquetEncodeDecodeRoundTripMapList(t *testing.T) {
	encodeConf, err := parquetEncodeProcessorConfig().ParseYAML(`
default_encoding: PLAIN
schema:
  - { name: id, type: INT64 }
  - name: mymap
    type: MAP
    optional: true
    fields:
      - { name: key, type: UTF8 }
      - { name: value, type: UTF8 }
  - name: mylist
    type: LIST
    fields:
      - { name: element, type: INT64 }
`, nil)
	require.NoError(t, err)

	encodeProc, err := newParquetEncodeProcessorFromConfig(encodeConf, nil)
	require.NoError(t, err)

	decodeConf, err := parquetDecodeProcessorConfig().ParseYAML(`
byte_array_as_string: true
`, nil)
	require.NoError(t, err)

	decodeProc, err := newParquetDecodeProcessorFromConfig(decodeConf, nil)
	require.NoError(t, err)

	testParquetEncodeDecodeRoundTripMapList(t, encodeProc, decodeProc)
}

func TestParquetEncodeDecodeRoundTripPlainEncoding(t *testing.T) {
	encodeConf, err := parquetEncodeProcessorConfig().ParseYAML(`
default_encoding: PLAIN
schema:
  - { name: id, type: INT64 }
  - { name: as, type: DOUBLE, repeated: true }
  - { name: b, type: BYTE_ARRAY }
  - { name: c, type: DOUBLE }
  - { name: d, type: BOOLEAN }
  - { name: e, type: INT64, optional: true }
  - { name: f, type: INT64 }
  - { name: g, type: UTF8 }
  - name: nested_stuff
    optional: true
    fields:
      - { name: a_stuff, type: BYTE_ARRAY }
      - { name: b_stuff, type: BYTE_ARRAY }
  - { name: h, type: DECIMAL32, decimal_precision: 3, optional: True}
  - name: ob
    fields:
      - name: ob_name
        type: UTF8
      - name: bidValue
        type: FLOAT
    optional: true
`, nil)
	require.NoError(t, err)

	encodeProc, err := newParquetEncodeProcessorFromConfig(encodeConf, nil)
	require.NoError(t, err)

	decodeConf, err := parquetDecodeProcessorConfig().ParseYAML(`
byte_array_as_string: true
`, nil)
	require.NoError(t, err)

	decodeProc, err := newParquetDecodeProcessorFromConfig(decodeConf, nil)
	require.NoError(t, err)

	testParquetEncodeDecodeRoundTrip(t, encodeProc, decodeProc)
}

func testParquetEncodeDecodeRoundTripMapList(t *testing.T, encodeProc *parquetEncodeProcessor, decodeProc *parquetDecodeProcessor) {
	tctx := context.Background()

	for _, test := range []struct {
		name      string
		input     string
		encodeErr string
		output    string
		decodeErr string
	}{
		{
			name: "basic values",
			input: `{
  "id": 3,
  "mymap": {"a":"b","c":"d"},
  "mylist": [1,2,3]
}`,
			output: `{
  "id": 3,
  "mymap": {"a":"Yg==","c":"ZA=="},
  "mylist": {"list":[{"element":1},{"element":2},{"element":3}]}
}`,
		},
		{
			name: "miss all optionals",
			input: `{
  "id": 3
}`,
			output: `{
  "id": 3,
  "mymap":null,
  "mylist":{"list":[]}
}`,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			inBatch := service.MessageBatch{
				service.NewMessage([]byte(test.input)),
			}

			encodedBatches, err := encodeProc.ProcessBatch(tctx, inBatch)
			if test.encodeErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.encodeErr)
				return
			}
			require.NoError(t, err)
			require.Len(t, encodedBatches, 1)
			require.Len(t, encodedBatches[0], 1)

			encodedBytes, err := encodedBatches[0][0].AsBytes()
			require.NoError(t, err)

			decodedBatch, err := decodeProc.Process(tctx, service.NewMessage(encodedBytes))
			if test.encodeErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.encodeErr)
				return
			}
			require.NoError(t, err)
			require.Len(t, decodedBatch, 1)

			decodedBytes, err := decodedBatch[0].AsBytes()
			require.NoError(t, err)

			assert.JSONEq(t, test.output, string(decodedBytes))
		})
	}
}

func testParquetEncodeDecodeRoundTrip(t *testing.T, encodeProc *parquetEncodeProcessor, decodeProc *parquetDecodeProcessor) {
	tctx := context.Background()

	for _, test := range []struct {
		name      string
		input     string
		encodeErr string
		output    string
		decodeErr string
	}{
		{
			name: "basic values",
			input: `{
  "id": 3,
  "as": [ 0.1, 0.2, 0.3, 0.4 ],
  "b": "hello world basic values",
  "c": 0.5,
  "d": true,
  "e": 6,
  "f": 7,
  "g": "logical string represent",
  "nested_stuff": {
    "a_stuff": "a value",
    "b_stuff": "b value"
  },
  "canary":"not in schema",
  "h": 1.0,
  "ob":{"ob_name":"test","bidValue":0.15}
}`,
			output: `{
  "id": 3,
  "as": [ 0.1, 0.2, 0.3, 0.4 ],
  "b": "hello world basic values",
  "c": 0.5,
  "d": true,
  "e": 6,
  "f": 7,
  "g": "logical string represent",
  "nested_stuff": {
    "a_stuff": "a value",
    "b_stuff": "b value"
  },
  "h": 1.0,
  "ob":{"ob_name":"test","bidValue":0.15}
}`,
		},
		{
			name: "miss all optionals",
			input: `{
  "id": 3,
  "b": "hello world basic values",
  "c": 0.5,
  "d": true,
  "f": 7,
  "g": "logical string represent",
  "canary":"not in schema"
}`,
			output: `{
  "id": 3,
  "as": [],
  "b": "hello world basic values",
  "c": 0.5,
  "d": true,
  "e": null,
  "f": 7,
  "g": "logical string represent",
  "nested_stuff": null,
  "h": null,
  "ob": null
}`,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			inBatch := service.MessageBatch{
				service.NewMessage([]byte(test.input)),
			}

			encodedBatches, err := encodeProc.ProcessBatch(tctx, inBatch)
			if test.encodeErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.encodeErr)
				return
			}
			require.NoError(t, err)
			require.Len(t, encodedBatches, 1)
			require.Len(t, encodedBatches[0], 1)

			encodedBytes, err := encodedBatches[0][0].AsBytes()
			require.NoError(t, err)

			decodedBatch, err := decodeProc.Process(tctx, service.NewMessage(encodedBytes))
			if test.encodeErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.encodeErr)
				return
			}
			require.NoError(t, err)
			require.Len(t, decodedBatch, 1)

			decodedBytes, err := decodedBatch[0].AsBytes()
			require.NoError(t, err)

			assert.JSONEq(t, test.output, string(decodedBytes))
		})
	}
}

func TestParquetDecodeListFormat(t *testing.T) {
	tctx := context.Background()

	encodeConf, err := parquetEncodeProcessorConfig().ParseYAML(`
schema:
  - { name: id, type: INT64 }
  - name: tags
    type: LIST
    fields:
      - { name: element, type: UTF8 }
  - name: scores
    type: LIST
    fields:
      - { name: element, type: FLOAT }
  - name: nested
    fields:
      - name: items
        type: LIST
        fields:
          - { name: element, type: INT32 }
  - { name: regular_array, type: INT32, repeated: true }
`, nil)
	require.NoError(t, err)

	encodeProc, err := newParquetEncodeProcessorFromConfig(encodeConf, nil)
	require.NoError(t, err)

	input := `{
  "id": 123,
  "tags": ["foo", "bar", "baz"],
  "scores": [1.5, 2.5, 3.5],
  "nested": {
    "items": [10, 20, 30]
  },
  "regular_array": [100, 200, 300]
}`

	encodedBatches, err := encodeProc.ProcessBatch(tctx, service.MessageBatch{
		service.NewMessage([]byte(input)),
	})
	require.NoError(t, err)
	require.Len(t, encodedBatches, 1)
	require.Len(t, encodedBatches[0], 1)

	encodedBytes, err := encodedBatches[0][0].AsBytes()
	require.NoError(t, err)

	t.Run("with legacy format", func(t *testing.T) {
		decodeConf, err := parquetDecodeProcessorConfig().ParseYAML(`
use_parquet_list_format: true
`, nil)
		require.NoError(t, err)

		decodeProc, err := newParquetDecodeProcessorFromConfig(decodeConf, nil)
		require.NoError(t, err)

		decodedBatch, err := decodeProc.Process(tctx, service.NewMessage(encodedBytes))
		require.NoError(t, err)
		require.Len(t, decodedBatch, 1)

		decodedBytes, err := decodedBatch[0].AsBytes()
		require.NoError(t, err)

		expected := `{
  "id": 123,
  "tags": {
    "list": [
      {"element": "foo"},
      {"element": "bar"},
      {"element": "baz"}
    ]
  },
  "scores": {
    "list": [
      {"element": 1.5},
      {"element": 2.5},
      {"element": 3.5}
    ]
  },
  "nested": {
    "items": {
      "list": [
        {"element": 10},
        {"element": 20},
        {"element": 30}
      ]
    }
  },
  "regular_array": [100, 200, 300]
}`

		assert.JSONEq(t, expected, string(decodedBytes))
	})

	t.Run("without legacy format", func(t *testing.T) {
		decodeConf, err := parquetDecodeProcessorConfig().ParseYAML(`
use_parquet_list_format: false
`, nil)
		require.NoError(t, err)

		decodeProc, err := newParquetDecodeProcessorFromConfig(decodeConf, nil)
		require.NoError(t, err)

		decodedBatch, err := decodeProc.Process(tctx, service.NewMessage(encodedBytes))
		require.NoError(t, err)
		require.Len(t, decodedBatch, 1)

		decodedBytes, err := decodedBatch[0].AsBytes()
		require.NoError(t, err)

		expected := `{
  "id": 123,
  "tags": ["foo", "bar", "baz"],
  "scores": [1.5, 2.5, 3.5],
  "nested": {
    "items": [10, 20, 30]
  },
  "regular_array": [100, 200, 300]
}`

		assert.JSONEq(t, expected, string(decodedBytes))
	})
}

func TestParquetDecodeListFormatEdgeCases(t *testing.T) {
	// FIXME: Close https://github.com/warpstreamlabs/bento/issues/360 when 2D array support added to parquet-go.
	t.Skip("2D slices are not currently supported by parquet encoder.")

	tctx := context.Background()

	encodeConf, err := parquetEncodeProcessorConfig().ParseYAML(`
schema:
  - name: normal_list
    type: LIST
    fields:
      - { name: element, type: UTF8 }
  - name: nullable_list
    type: LIST
    optional: true
    fields:
      - { name: element, type: INT32 }
  - name: list_of_lists
    type: LIST
    fields:
      - name: element
        type: LIST
        fields:
          - { name: element, type: FLOAT }
`, nil)
	require.NoError(t, err)

	encodeProc, err := newParquetEncodeProcessorFromConfig(encodeConf, nil)
	require.NoError(t, err)

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name: "empty lists",
			input: `{
  "normal_list": [],
  "nullable_list": null,
  "list_of_lists": []
}`,
			expected: `{
  "normal_list": {"list": []},
  "nullable_list": null,
  "list_of_lists": {"list": []}
}`,
		},
		{
			name: "nested lists",
			input: `{
  "normal_list": ["a"],
  "nullable_list": [1, 2],
  "list_of_lists": [[1.1, 2.2], [3.3]]
}`,
			expected: `{
  "normal_list": {"list": [{"element": "a"}]},
  "nullable_list": {"list": [{"element": 1}, {"element": 2}]},
  "list_of_lists": {
    "list": [
      {"element": {"list": [{"element": 1.1}, {"element": 2.2}]}},
      {"element": {"list": [{"element": 3.3}]}}
    ]
  }
}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encodedBatches, err := encodeProc.ProcessBatch(tctx, service.MessageBatch{
				service.NewMessage([]byte(test.input)),
			})
			require.NoError(t, err)

			encodedBytes, err := encodedBatches[0][0].AsBytes()
			require.NoError(t, err)

			decodeConf, err := parquetDecodeProcessorConfig().ParseYAML(`
use_parquet_list_format: true
`, nil)
			require.NoError(t, err)

			decodeProc, err := newParquetDecodeProcessorFromConfig(decodeConf, nil)
			require.NoError(t, err)

			decodedBatch, err := decodeProc.Process(tctx, service.NewMessage(encodedBytes))
			require.NoError(t, err)

			decodedBytes, err := decodedBatch[0].AsBytes()
			require.NoError(t, err)

			assert.JSONEq(t, test.expected, string(decodedBytes))
		})
	}
}

func TestParquetEncodeEmptyBatch(t *testing.T) {
	tctx := context.Background()

	encodeConf, err := parquetEncodeProcessorConfig().ParseYAML(`
default_encoding: PLAIN
schema:
  - { name: id, type: INT64 }
`, nil)
	require.NoError(t, err)

	encodeProc, err := newParquetEncodeProcessorFromConfig(encodeConf, nil)
	require.NoError(t, err)

	inBatch := service.MessageBatch{}
	_, err = encodeProc.ProcessBatch(tctx, inBatch)
	require.NoError(t, err)
}

func TestParquetEncodeProcessor(t *testing.T) {
	type obj map[string]any
	type arr []any

	tests := []struct {
		name  string
		input any
	}{
		{
			name: "Empty values",
			input: obj{
				"ID": 0,
				"A":  0,
				"Foo": obj{
					"First":  nil,
					"Second": nil,
					"Third":  nil,
				},
				"Bar": obj{
					"Meows":      arr{},
					"NestedFoos": arr{},
				},
			},
		},
		{
			name: "Basic values",
			input: obj{
				"ID": 1,
				"Foo": obj{
					"First":  21,
					"Second": nil,
					"Third":  22,
				},
				"A": 2,
				"Bar": obj{
					"Meows": arr{41, 42},
					"NestedFoos": arr{
						obj{"First": 27, "Second": nil, "Third": nil},
						obj{"First": nil, "Second": 28, "Third": 29},
					},
				},
			},
		},
		{
			name: "Empty array trickery",
			input: obj{
				"ID": 0,
				"A":  0,
				"Foo": obj{
					"First":  nil,
					"Second": nil,
					"Third":  nil,
				},
				"Bar": obj{
					"Meows": arr{},
					"NestedFoos": arr{
						obj{"First": nil, "Second": nil, "Third": nil},
						obj{"First": nil, "Second": 28, "Third": 29},
					},
				},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			expectedDataBytes, err := json.Marshal(test.input)
			require.NoError(t, err)

			schema := parquet.SchemaOf(&PMType{})

			reader, err := newParquetEncodeProcessor(
				nil,
				schema,
				&parquet.Uncompressed,
				reflect.TypeOf(PMType{}),
			)
			require.NoError(t, err)

			readerResBatches, err := reader.ProcessBatch(context.Background(), service.MessageBatch{
				service.NewMessage(expectedDataBytes),
			})
			require.NoError(t, err)

			require.Len(t, readerResBatches, 1)
			require.Len(t, readerResBatches[0], 1)

			pqDataBytes, err := readerResBatches[0][0].AsBytes()
			require.NoError(t, err)

			pRdr := parquet.NewGenericReader[any](bytes.NewReader(pqDataBytes), testPMSchema())

			outRows := make([]any, 1)
			_, err = pRdr.Read(outRows)
			if err != nil {
				require.ErrorIs(t, err, io.EOF)
			}

			require.NoError(t, pRdr.Close())

			actualDataBytes, err := json.Marshal(outRows[0])
			require.NoError(t, err)

			assert.JSONEq(t, string(expectedDataBytes), string(actualDataBytes))
		})
	}

	t.Run("all together", func(t *testing.T) {
		var expected []any

		var inBatch service.MessageBatch
		for _, test := range tests {
			expected = append(expected, test.input)

			dataBytes, err := json.Marshal(test.input)
			require.NoError(t, err)

			inBatch = append(inBatch, service.NewMessage(dataBytes))
		}

		schema := parquet.SchemaOf(&PMType{})

		reader, err := newParquetEncodeProcessor(
			nil,
			schema,
			&parquet.Uncompressed,
			reflect.TypeOf(PMType{}),
		)
		require.NoError(t, err)

		readerResBatches, err := reader.ProcessBatch(context.Background(), inBatch)
		require.NoError(t, err)

		require.Len(t, readerResBatches, 1)
		require.Len(t, readerResBatches[0], 1)

		pqDataBytes, err := readerResBatches[0][0].AsBytes()
		require.NoError(t, err)

		pRdr := parquet.NewGenericReader[any](bytes.NewReader(pqDataBytes), testPMSchema())
		require.NoError(t, err)

		var outRows []any
		for {
			outRowsTmp := make([]any, 3)
			_, err := pRdr.Read(outRowsTmp)
			outRows = append(outRows, outRowsTmp...)
			if err != nil {
				require.ErrorIs(t, err, io.EOF)
				break
			}
		}
		require.NoError(t, pRdr.Close())

		expectedBytes, err := json.Marshal(expected)
		require.NoError(t, err)
		actualBytes, err := json.Marshal(outRows)
		require.NoError(t, err)

		assert.JSONEq(t, string(expectedBytes), string(actualBytes))
	})
}

func TestEncodingFromConfig(t *testing.T) {
	tests := []struct {
		config   string
		expected parquet.Node
	}{
		{
			config: `
schema:
  - name: map
    type: MAP
    fields:
      - { name: key, type: UTF8 }
      - { name: value, type: FLOAT }
`,
			expected: parquet.Group{
				"map": parquet.Map(
					parquet.String(),
					parquet.Leaf(parquet.FloatType),
				),
			},
		},
		{
			config: `
schema:
  - name: map
    type: MAP
    fields:
      - { name: key, type: UTF8 }
      - name: value
        type: MAP
        fields:
          - { name: key, type: INT64 }
          - { name: value, type: BYTE_ARRAY }

`,
			expected: parquet.Group{
				"map": parquet.Map(
					parquet.String(),
					parquet.Map(
						parquet.Int(64),
						parquet.Leaf(parquet.ByteArrayType),
					),
				),
			},
		},
	}

	for _, tt := range tests {
		encodeConf, err := parquetEncodeProcessorConfig().ParseYAML(tt.config, nil)
		require.NoError(t, err)

		encodeProc, err := newParquetEncodeProcessorFromConfig(encodeConf, nil)
		require.NoError(t, err)

		schema := parquet.NewSchema("", tt.expected)
		require.Equal(t, schema.String(), encodeProc.schema.String())
	}
}

func TestEncodeDecimalOptional(t *testing.T) {
	tests := []struct {
		config   string
		expected parquet.Node
	}{
		{
			config: `
    schema:
      - name: floor_value
        optional: true
        type: DECIMAL32
        decimal_scale: 4
        decimal_precision: 8
`,
			expected: parquet.Group{
				"floor_value": parquet.Optional(parquet.Decimal(4, 8, parquet.Int32Type)),
			},
		},
	}

	for _, tt := range tests {
		encodeConf, err := parquetEncodeProcessorConfig().ParseYAML(tt.config, nil)
		require.NoError(t, err)

		encodeProc, err := newParquetEncodeProcessorFromConfig(encodeConf, nil)
		require.NoError(t, err)

		schema := parquet.NewSchema("", tt.expected)
		require.Equal(t, schema.String(), encodeProc.schema.String())
	}

}
