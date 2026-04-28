package docs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/warpstreamlabs/bento/internal/docs/specpb"
)

func TestComponentSpecRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		spec ComponentSpec
	}{
		{
			name: "all component types and statuses",
			spec: ComponentSpec{
				Name:   "comprehensive_component",
				Type:   TypeProcessor,
				Status: StatusStable,
				Config: FieldSpec{
					Name: "config",
					Type: FieldTypeObject,
					Kind: KindScalar,
					Children: FieldSpecs{
						{
							Name: "input_component",
							Type: FieldTypeInput,
							Kind: KindScalar,
						},
						{
							Name: "buffer_component",
							Type: FieldTypeBuffer,
							Kind: KindScalar,
						},
						{
							Name: "cache_component",
							Type: FieldTypeCache,
							Kind: KindScalar,
						},
						{
							Name: "processor_components",
							Type: FieldTypeProcessor,
							Kind: KindArray,
						},
						{
							Name: "rate_limit_component",
							Type: FieldTypeRateLimit,
							Kind: KindScalar,
						},
						{
							Name: "output_component",
							Type: FieldTypeOutput,
							Kind: KindScalar,
						},
						{
							Name: "metrics_component",
							Type: FieldTypeMetrics,
							Kind: KindScalar,
						},
						{
							Name: "tracer_component",
							Type: FieldTypeTracer,
							Kind: KindScalar,
						},
						{
							Name: "scanner_component",
							Type: FieldTypeScanner,
							Kind: KindScalar,
						},
						{
							Name:    "enabled",
							Type:    FieldTypeBool,
							Kind:    KindScalar,
							Default: ptr(true),
						},
						{
							Name:    "timeout",
							Type:    FieldTypeString,
							Kind:    KindScalar,
							Default: ptr("5s"),
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			proto, err := FromComponentSpec(test.spec)
			require.NoError(t, err)
			result := ToComponentSpec(proto)
			assert.Equal(t, test.spec, result)
		})
	}
}

func TestComponentTypeConversion(t *testing.T) {
	tests := []struct {
		name      string
		goType    Type
		protoType pb.ComponentType
	}{
		{"buffer", TypeBuffer, pb.ComponentType_COMPONENT_TYPE_BUFFER},
		{"cache", TypeCache, pb.ComponentType_COMPONENT_TYPE_CACHE},
		{"input", TypeInput, pb.ComponentType_COMPONENT_TYPE_INPUT},
		{"metrics", TypeMetrics, pb.ComponentType_COMPONENT_TYPE_METRICS},
		{"output", TypeOutput, pb.ComponentType_COMPONENT_TYPE_OUTPUT},
		{"processor", TypeProcessor, pb.ComponentType_COMPONENT_TYPE_PROCESSOR},
		{"rate_limit", TypeRateLimit, pb.ComponentType_COMPONENT_TYPE_RATE_LIMIT},
		{"tracer", TypeTracer, pb.ComponentType_COMPONENT_TYPE_TRACER},
		{"scanner", TypeScanner, pb.ComponentType_COMPONENT_TYPE_SCANNER},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			spec := ComponentSpec{
				Name: "test",
				Type: test.goType,
				Config: FieldSpec{
					Name: "config",
					Type: FieldTypeObject,
					Kind: KindScalar,
				},
			}
			proto, err := FromComponentSpec(spec)
			require.NoError(t, err)
			assert.Equal(t, test.protoType, proto.ComponentType)

			protoSpec := &pb.ComponentSpec{
				Name:          "test",
				ComponentType: test.protoType,
				Config: &pb.FieldSpec{
					Name:      "config",
					FieldType: pb.FieldType_FIELD_TYPE_OBJECT,
					FieldKind: pb.FieldKind_FIELD_KIND_SCALAR,
				},
			}
			goSpec := ToComponentSpec(protoSpec)
			assert.Equal(t, test.goType, goSpec.Type)
		})
	}
}

func TestComponentStatusConversion(t *testing.T) {
	tests := []struct {
		name        string
		goStatus    Status
		protoStatus pb.ComponentStatus
	}{
		{"stable", StatusStable, pb.ComponentStatus_COMPONENT_STATUS_STABLE},
		{"beta", StatusBeta, pb.ComponentStatus_COMPONENT_STATUS_BETA},
		{"experimental", StatusExperimental, pb.ComponentStatus_COMPONENT_STATUS_EXPERIMENTAL},
		{"deprecated", StatusDeprecated, pb.ComponentStatus_COMPONENT_STATUS_DEPRECATED},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			spec := ComponentSpec{
				Name:   "test",
				Type:   TypeProcessor,
				Status: test.goStatus,
				Config: FieldSpec{
					Name: "config",
					Type: FieldTypeObject,
					Kind: KindScalar,
				},
			}
			proto, err := FromComponentSpec(spec)
			require.NoError(t, err)
			assert.Equal(t, test.protoStatus, proto.ComponentStatus)

			protoSpec := &pb.ComponentSpec{
				Name:            "test",
				ComponentType:   pb.ComponentType_COMPONENT_TYPE_PROCESSOR,
				ComponentStatus: test.protoStatus,
				Config: &pb.FieldSpec{
					Name:      "config",
					FieldType: pb.FieldType_FIELD_TYPE_OBJECT,
					FieldKind: pb.FieldKind_FIELD_KIND_SCALAR,
				},
			}
			goSpec := ToComponentSpec(protoSpec)
			assert.Equal(t, test.goStatus, goSpec.Status)
		})
	}
}
