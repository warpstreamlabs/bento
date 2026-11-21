package processor

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/tracing"
)

func TestMarkErrSetsSpanErrorStatus(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))

	ctx, otSpan := tp.Tracer("test").Start(context.Background(), "test-span")
	span := tracing.OtelSpan(ctx, otSpan)

	part := message.NewPart([]byte("hello"))
	testErr := errors.New("failed to process")

	MarkErr(part, span, testErr)
	otSpan.End()

	require.Error(t, part.ErrorGet())
	assert.Equal(t, testErr.Error(), part.ErrorGet().Error())

	ended := sr.Ended()
	require.Len(t, ended, 1)

	status := ended[0].Status()
	assert.Equal(t, codes.Error, status.Code)
	assert.Equal(t, testErr.Error(), status.Description)

	var sawException bool
	for _, e := range ended[0].Events() {
		if e.Name == "exception" {
			sawException = true
		}
	}
	assert.True(t, sawException, "expected an exception event to be recorded")
}

func TestMarkErrNilErrIsNoop(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))

	ctx, otSpan := tp.Tracer("test").Start(context.Background(), "test-span")
	span := tracing.OtelSpan(ctx, otSpan)

	part := message.NewPart([]byte("hello"))
	MarkErr(part, span, nil)
	otSpan.End()

	assert.NoError(t, part.ErrorGet())

	status := sr.Ended()[0].Status()
	assert.Equal(t, codes.Unset, status.Code)
}
