package docs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/warpstreamlabs/bento/internal/docs/specpb"
)

func ptr[T any](v T) *any {
	var a any = v
	return &a
}

func TestFieldSpecRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		spec FieldSpec
	}{
		{
			name: "string field with default",
			spec: FieldSpec{
				Name:    "hostname",
				Type:    FieldTypeString,
				Kind:    KindScalar,
				Default: ptr("localhost"),
			},
		},
		{
			name: "int field with default",
			spec: FieldSpec{
				Name:    "port",
				Type:    FieldTypeInt,
				Kind:    KindScalar,
				Default: ptr(int32(8080)),
			},
		},
		{
			name: "float field with default",
			spec: FieldSpec{
				Name:    "timeout",
				Type:    FieldTypeFloat,
				Kind:    KindScalar,
				Default: ptr(float32(30.5)),
			},
		},
		{
			name: "bool field with default",
			spec: FieldSpec{
				Name:    "enabled",
				Type:    FieldTypeBool,
				Kind:    KindScalar,
				Default: ptr(true),
			},
		},
		{
			name: "optional secret field",
			spec: FieldSpec{
				Name:       "api_key",
				Type:       FieldTypeString,
				Kind:       KindScalar,
				IsOptional: true,
				IsSecret:   true,
			},
		},
		{
			name: "interpolated field",
			spec: FieldSpec{
				Name:         "topic",
				Type:         FieldTypeString,
				Kind:         KindScalar,
				Interpolated: true,
			},
		},
		{
			name: "bloblang field",
			spec: FieldSpec{
				Name:     "mapping",
				Type:     FieldTypeString,
				Kind:     KindScalar,
				Bloblang: true,
			},
		},
		{
			name: "field with options",
			spec: FieldSpec{
				Name:    "compression",
				Type:    FieldTypeString,
				Kind:    KindScalar,
				Options: []string{"none", "gzip", "snappy"},
			},
		},
		{
			name: "field with linter",
			spec: FieldSpec{
				Name:   "batch_size",
				Type:   FieldTypeInt,
				Kind:   KindScalar,
				Linter: "root = if this < 1 { [ \"must be positive\" ] }",
			},
		},
		{
			name: "object field",
			spec: FieldSpec{
				Type:    FieldTypeObject,
				Kind:    KindMap,
				Default: ptr(map[string]any{"foo": "bar"})},
		},

		{
			name: "array field",
			spec: FieldSpec{
				Name: "brokers",
				Type: FieldTypeString,
				Kind: KindArray,
			},
		},
		{
			name: "map field",
			spec: FieldSpec{
				Name: "metadata",
				Type: FieldTypeString,
				Kind: KindMap,
			},
		},
		{
			name: "2d array field",
			spec: FieldSpec{
				Name: "matrix",
				Type: FieldTypeInt,
				Kind: Kind2DArray,
			},
		},
		{
			name: "object with children",
			spec: FieldSpec{
				Name: "tls",
				Type: FieldTypeObject,
				Kind: KindScalar,
				Children: FieldSpecs{
					{
						Name:    "enabled",
						Type:    FieldTypeBool,
						Kind:    KindScalar,
						Default: ptr(false),
					},
					{
						Name:       "skip_verify",
						Type:       FieldTypeBool,
						Kind:       KindScalar,
						IsOptional: true,
					},
				},
			},
		},
		{
			name: "nested object multiple levels",
			spec: FieldSpec{
				Name: "kafka",
				Type: FieldTypeObject,
				Kind: KindScalar,
				Children: FieldSpecs{
					{
						Name: "brokers",
						Type: FieldTypeString,
						Kind: KindArray,
					},
					{
						Name: "tls",
						Type: FieldTypeObject,
						Kind: KindScalar,
						Children: FieldSpecs{
							{
								Name:    "enabled",
								Type:    FieldTypeBool,
								Kind:    KindScalar,
								Default: ptr(false),
							},
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			proto, err := FromFieldSpec(test.spec)
			require.NoError(t, err)

			result := ToFieldSpec(proto)
			assert.Equal(t, test.spec, result)
		})
	}
}

func TestFieldValueConversion(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected any
	}{

		{"bool true", true, true},
		{"bool false", false, false},
		{"string", "hello world", "hello world"},
		{"int32", int32(42), int32(42)},
		{"float32", float32(3.14), float32(3.14)},
		{"negative int32", int32(-100), int32(-100)},
		{"zero int32", int32(0), int32(0)},

		{"int to int32", int(42), int32(42)},
		{"int64 to int32", int64(1000), int32(1000)},
		{"uint to int32", uint(50), int32(50)},
		{"uint32 to int32", uint32(200), int32(200)},

		{"float64 to float32", float64(2.71828), float32(2.71828)},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			proto, err := toFieldValue(test.input)
			require.NoError(t, err)
			result := fromFieldValue(proto)
			assert.Equal(t, test.expected, result)
		})
	}
}

func TestFieldSpecComponentTypes(t *testing.T) {
	tests := []struct {
		name      string
		fieldType FieldType
		protoType pb.FieldType
	}{
		{"input", FieldTypeInput, pb.FieldType_FIELD_TYPE_INPUT},
		{"buffer", FieldTypeBuffer, pb.FieldType_FIELD_TYPE_BUFFER},
		{"cache", FieldTypeCache, pb.FieldType_FIELD_TYPE_CACHE},
		{"processor", FieldTypeProcessor, pb.FieldType_FIELD_TYPE_PROCESSOR},
		{"rate_limit", FieldTypeRateLimit, pb.FieldType_FIELD_TYPE_RATE_LIMIT},
		{"output", FieldTypeOutput, pb.FieldType_FIELD_TYPE_OUTPUT},
		{"metrics", FieldTypeMetrics, pb.FieldType_FIELD_TYPE_METRICS},
		{"tracer", FieldTypeTracer, pb.FieldType_FIELD_TYPE_TRACER},
		{"scanner", FieldTypeScanner, pb.FieldType_FIELD_TYPE_SCANNER},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			spec := FieldSpec{
				Name: "test",
				Type: test.fieldType,
				Kind: KindScalar,
			}
			proto, err := FromFieldSpec(spec)
			require.NoError(t, err)
			assert.Equal(t, test.protoType, proto.FieldType)

			result := ToFieldSpec(proto)
			assert.Equal(t, test.fieldType, result.Type)
		})
	}
}

func TestFieldSpecRoundTripFull(t *testing.T) {
	tests := []struct {
		name string
		spec FieldSpec
	}{
		{
			name: "all together",
			spec: FieldSpec{
				Name: "root", Type: FieldTypeObject, Kind: KindScalar,
				Children: FieldSpecs{
					{Name: "string_field", Type: FieldTypeString, Kind: KindScalar, Default: ptr("default_value")},
					{Name: "int_field", Type: FieldTypeInt, Kind: KindScalar, Default: ptr(int32(42))},
					{Name: "float_field", Type: FieldTypeFloat, Kind: KindScalar, Default: ptr(float32(3.14))},
					{Name: "bool_field", Type: FieldTypeBool, Kind: KindScalar, Default: ptr(true)},

					{Name: "object_field", Type: FieldTypeObject, Kind: KindMap, Default: ptr(map[string]any{"foo": "bar"})},

					{Name: "array_field", Type: FieldTypeString, Kind: KindArray},
					{Name: "2darray_field", Type: FieldTypeInt, Kind: Kind2DArray},
					{Name: "map_field", Type: FieldTypeString, Kind: KindMap},

					{Name: "optional_field", Type: FieldTypeString, Kind: KindScalar, IsOptional: true},
					{Name: "secret_field", Type: FieldTypeString, Kind: KindScalar, IsSecret: true},
					{Name: "interpolated_field", Type: FieldTypeString, Kind: KindScalar, Interpolated: true},
					{Name: "bloblang_field", Type: FieldTypeString, Kind: KindScalar, Bloblang: true},
					{Name: "options_field", Type: FieldTypeString, Kind: KindScalar, Options: []string{"option1", "option2", "option3"}},
					{Name: "linter_field", Type: FieldTypeInt, Kind: KindScalar, Linter: "root = if this < 1 { [ \"must be positive\" ] }"},

					{Name: "input_field", Type: FieldTypeInput, Kind: KindScalar},
					{Name: "buffer_field", Type: FieldTypeBuffer, Kind: KindScalar},
					{Name: "cache_field", Type: FieldTypeCache, Kind: KindScalar},
					{Name: "processor_field", Type: FieldTypeProcessor, Kind: KindArray},
					{Name: "rate_limit_field", Type: FieldTypeRateLimit, Kind: KindScalar},
					{Name: "output_field", Type: FieldTypeOutput, Kind: KindScalar},
					{Name: "metrics_field", Type: FieldTypeMetrics, Kind: KindScalar},
					{Name: "tracer_field", Type: FieldTypeTracer, Kind: KindScalar},
					{Name: "scanner_field", Type: FieldTypeScanner, Kind: KindScalar},

					{
						Name: "nested_object",
						Type: FieldTypeObject, Kind: KindScalar,
						Children: FieldSpecs{
							{Name: "nested_string", Type: FieldTypeString, Kind: KindScalar},
							{Name: "nested_int", Type: FieldTypeInt, Kind: KindScalar, Default: ptr(int32(99))},
							{
								Name: "deeply_nested",
								Type: FieldTypeObject, Kind: KindScalar,
								Children: FieldSpecs{
									{Name: "deep_bool", Type: FieldTypeBool, Kind: KindScalar, Default: ptr(false)},
								},
							},
						},
					},
					{Name: "unknown_field", Type: FieldTypeUnknown, Kind: KindScalar},
				},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			proto, err := FromFieldSpec(test.spec)
			require.NoError(t, err)

			result := ToFieldSpec(proto)
			assert.Equal(t, test.spec, result)
		})
	}
}
