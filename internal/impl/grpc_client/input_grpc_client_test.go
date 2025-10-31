package grpc_client

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
)

func TestInput_injectMetadataIntoContext(t *testing.T) {
	cfg := &Config{
		BearerToken: "t",
		AuthHeaders: map[string]string{"a": "1", "b": "2"},
	}
	ctx := context.Background()
	ctx = injectMetadataIntoContext(ctx, cfg)
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("expected outgoing metadata in context")
	}
	if v := md.Get("authorization"); len(v) == 0 || v[0] != "Bearer t" {
		t.Fatalf("authorization = %v", v)
	}
	if v := md.Get("a"); len(v) == 0 || v[0] != "1" {
		t.Fatalf("a = %v", v)
	}
	if v := md.Get("b"); len(v) == 0 || v[0] != "2" {
		t.Fatalf("b = %v", v)
	}
}

func TestInput_enhanceCallContext(t *testing.T) {
	g := &genericInput{cfg: &Config{}}
	parent, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ctx := g.enhanceCallContext(parent)
	// No-op now; ensure it doesn't strip the context
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}
}
