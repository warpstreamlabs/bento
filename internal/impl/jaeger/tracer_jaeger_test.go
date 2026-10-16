package jaeger

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger" //nolint:staticcheck
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.opentelemetry.io/otel/trace"
)

func TestGetAgentOps(t *testing.T) {
	tests := []struct {
		name         string
		agentAddress string
		want         []jaeger.AgentEndpointOption
	}{
		{
			name:         "address with port",
			agentAddress: "localhost:5775",
			want: []jaeger.AgentEndpointOption{
				jaeger.WithAgentHost("localhost"),
				jaeger.WithAgentPort("5775"),
			},
		},
		{
			name:         "address without port",
			agentAddress: "jaeger",
			want: []jaeger.AgentEndpointOption{
				jaeger.WithAgentHost("jaeger"),
			},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			opts, err := getAgentOpts(testCase.agentAddress)

			// We can't check for equality because they are functions, so we just check that the length is the same
			assert.Len(t, opts, len(testCase.want))
			assert.NoError(t, err)
		})
	}
}

func TestNewJaeger(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	exporterInitFn = func(_ jaeger.EndpointOption) (tracesdk.SpanExporter, error) {
		return exporter, nil
	}

	dummyVersion := "v1.0"

	tests := []struct {
		Name           string
		ServiceName    string
		ServiceVersion string
		Tags           map[string]string
	}{
		{
			Name:           "no tags",
			ServiceName:    "bento",
			ServiceVersion: dummyVersion,
		},
		{
			Name:           "tags can overwrite service name and version",
			ServiceName:    "foobar",
			ServiceVersion: "6.6.6",
			Tags: map[string]string{
				string(semconv.ServiceNameKey):    "foobar",
				string(semconv.ServiceVersionKey): "6.6.6",
			},
		},
		{
			Name: "supports extra arbitrary tags",
			Tags: map[string]string{
				"foo": "bar",
			},
		},
	}

	for _, test := range tests {
		exporter.Reset()

		jaegerProvider, err := NewJaeger(jaegerConfig{
			engineVersion: dummyVersion,
			Tags:          test.Tags,
		})
		require.NoError(t, err, test.Name)

		// Add a span and flush it
		_, span := jaegerProvider.Tracer("testProvider").Start(context.Background(), "testSpan")
		span.AddEvent("testEvent")
		span.End()
		jaegerProvider.(*tracesdk.TracerProvider).ForceFlush(context.Background())

		snapshots := exporter.GetSpans().Snapshots()
		require.Len(t, snapshots, 1, test.Name)
		resource := snapshots[0].Resource()
		require.NotNil(t, resource, test.Name)
		attrs := resource.Attributes()

		if len(test.Tags) != 1 {
			require.Len(t, attrs, 2, test.Name)
			require.Equal(t, semconv.ServiceNameKey.String(test.ServiceName), attrs[0], test.Name)
			require.Equal(t, semconv.ServiceVersionKey.String(test.ServiceVersion), attrs[1], test.Name)
		} else {
			require.Len(t, attrs, 3, test.Name)
			require.Equal(t, attribute.Key("foo").String("bar"), attrs[0], test.Name)
		}
	}
}

// TestConstSamplerRespectsSampledParent verifies that, with the "const"
// sampler type, the sampler param is only applied to root spans and that a
// span with a sampled parent (such as one extracted from an incoming
// traceparent header) is always sampled regardless of the configured ratio.
func TestConstSamplerRespectsSampledParent(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	exporterInitFn = func(_ jaeger.EndpointOption) (tracesdk.SpanExporter, error) {
		return exporter, nil
	}

	jaegerProvider, err := NewJaeger(jaegerConfig{
		engineVersion: "v1.0",
		SamplerType:   "const",
		SamplerParam:  0,
	})
	require.NoError(t, err)

	traceID, err := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	require.NoError(t, err)
	spanID, err := trace.SpanIDFromHex("00f067aa0ba902b7")
	require.NoError(t, err)

	sampledParent := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	ctx := trace.ContextWithRemoteSpanContext(context.Background(), sampledParent)

	_, childSpan := jaegerProvider.Tracer("test").Start(ctx, "child")
	defer childSpan.End()
	require.True(t, childSpan.SpanContext().IsSampled(), "child of a sampled parent should be sampled even with a ratio of 0")

	_, rootSpan := jaegerProvider.Tracer("test").Start(context.Background(), "root")
	defer rootSpan.End()
	require.False(t, rootSpan.SpanContext().IsSampled(), "root span should follow the configured ratio of 0")
}
