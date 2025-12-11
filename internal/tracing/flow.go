package tracing

import "context"

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
