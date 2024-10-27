package parquet

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

func TestParquetEncodePanic(t *testing.T) {
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
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot represent 1e+99 as float32")

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
	assert.Contains(t, err.Error(), "cannot represent 1e+10 as int32")
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

			fmt.Printf("decodedBytes:%s\n", string(decodedBytes))

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
  "mymap": {"a":"b","c":"d"},
  "nested_stuff": {
    "a_stuff": "a value",
    "b_stuff": "b value"
  },
  "canary":"not in schema"
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
  "mymap": {"e":"f","g":"h"},
  "nested_stuff": {
    "a_stuff": "a value",
    "b_stuff": "b value"
  }
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
  "mymap": {"i":"j","k":"l"},
  "nested_stuff": null
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
			require.NoError(t, err)

			outRows := make([]any, 1)
			_, err = pRdr.Read(outRows)
			require.NoError(t, err)
			require.NoError(t, err)

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
			outRowsTmp := make([]any, 1)
			_, err := pRdr.Read(outRowsTmp)
			if err != nil {
				require.ErrorIs(t, err, io.EOF)
				break
			}
			outRows = append(outRows, outRowsTmp[0])
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

// ConvertMapToParquetFormat converts a map[string]any to the parquet list format
// where slices are represented as []map[string]any with "element" keys
// and strings in nested maps are base64 encoded
func ConvertMapToParquetFormat(input map[string]any) map[string]any {
	return convertMapLevel(input, true)
}

// convertMapLevel handles map conversion with awareness of nesting level
func convertMapLevel(input map[string]any, isTopLevel bool) map[string]any {
	result := make(map[string]any)
	for key, value := range input {
		result[key] = convertValue(value, isTopLevel)
	}
	return result
}

// convertValue handles the conversion of individual values
func convertValue(value any, isTopLevel bool) any {
	if value == nil {
		return nil
	}

	// Handle maps
	if m, ok := value.(map[string]any); ok {
		return convertMapLevel(m, false)
	}

	// Handle []any specifically
	if slice, ok := value.([]any); ok {
		result := make([]any, len(slice))
		for i, elem := range slice {
			if m, ok := elem.(map[string]any); ok {
				result[i] = convertMapLevel(m, false)
			} else {
				converted := convertValue(elem, false)
				result[i] = map[string]any{"element": converted}
			}
		}
		return result
	}

	// Handle strings
	if str, ok := value.(string); ok {
		if !isTopLevel {
			return base64.StdEncoding.EncodeToString([]byte(str))
		}
		return str
	}

	// Handle other slice types using reflection
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Slice {
		length := v.Len()
		result := make([]any, length)
		for i := 0; i < length; i++ {
			elem := v.Index(i).Interface()
			converted := convertValue(elem, false)
			result[i] = map[string]any{"element": converted}
		}
		return result
	}

	return value
}
