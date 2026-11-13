package tracing

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestSpanErrorSetsStatusAndRecordsException(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))

	ctx, otSpan := tp.Tracer("test").Start(context.Background(), "test-span")
	span := OtelSpan(ctx, otSpan)

	testErr := errors.New("boom")
	span.Error(testErr)
	otSpan.End()

	ended := sr.Ended()
	require.Len(t, ended, 1)

	status := ended[0].Status()
	assert.Equal(t, codes.Error, status.Code)
	assert.Equal(t, testErr.Error(), status.Description)

	events := ended[0].Events()
	require.Len(t, events, 1)
	assert.Equal(t, "exception", events[0].Name)
}

func TestSpanErrorNilSafe(t *testing.T) {
	var span *Span
	assert.NotPanics(t, func() {
		span.Error(errors.New("boom"))
	})

	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	ctx, otSpan := tp.Tracer("test").Start(context.Background(), "test-span")
	span = OtelSpan(ctx, otSpan)

	assert.NotPanics(t, func() {
		span.Error(nil)
	})
	otSpan.End()

	status := sr.Ended()[0].Status()
	assert.Equal(t, codes.Unset, status.Code)
}
