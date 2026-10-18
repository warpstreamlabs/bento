package otlp

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

// TestSamplingRespectsSampledParent verifies that the configured ratio only
// applies to root spans, and that spans with a sampled parent (such as one
// extracted from an incoming traceparent header) are always sampled
// regardless of the ratio.
func TestSamplingRespectsSampledParent(t *testing.T) {
	provider, err := newOtlp(&otlp{
		sampling: sampleConfig{enabled: true, ratio: 0},
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

	_, childSpan := provider.Tracer("test").Start(ctx, "child")
	defer childSpan.End()

	require.True(t, childSpan.SpanContext().IsSampled(), "child of a sampled parent should be sampled even with ratio 0")

	// A root span (no parent) should respect the configured ratio of 0 and
	// not be sampled.
	_, rootSpan := provider.Tracer("test").Start(context.Background(), "root")
	defer rootSpan.End()

	require.False(t, rootSpan.SpanContext().IsSampled(), "root span should follow the configured ratio of 0")
}
