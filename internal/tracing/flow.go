package tracing

import (
	"context"

	"github.com/google/uuid"
	"github.com/warpstreamlabs/bento/internal/message"
)

const EmptyTraceID = "00000000000000000000000000000000"

type flowTraceKey struct{}

// WithFlowID returns a context with the flow ID stored in it
func WithFlowID(ctx context.Context, flowID string) context.Context {
	return context.WithValue(ctx, flowTraceKey{}, flowID)
}

// GetFlowID retrieves the flow ID from a context
func GetFlowID(ctx context.Context) string {
	if id, ok := ctx.Value(flowTraceKey{}).(string); ok {
		return id
	}
	return ""
}

// EnsureFlowID ensures a flow ID exists on the message part, creating one if necessary.
// This should be called at the input layer to initialize flow IDs for all messages.
func EnsureFlowID(part *message.Part) *message.Part {
	ctx := message.GetContext(part)

	if flowID := GetFlowID(ctx); flowID != "" {
		return part
	}

	if traceID := GetTraceID(part); traceID != "" && traceID != EmptyTraceID {
		ctx = WithFlowID(ctx, traceID)
		return part.WithContext(ctx)
	}

	flowID, _ := uuid.NewV7()
	flowIDStr := flowID.String()
	ctx = WithFlowID(ctx, flowIDStr)
	return part.WithContext(ctx)
}
