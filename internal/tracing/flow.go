package tracing

import "context"

// flowTraceKey is an internal struct used as a context key for flow IDs to avoid collisions
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
