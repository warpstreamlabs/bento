package grpc_client

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
)

func TestOutput_injectMetadataIntoContext(t *testing.T) {
	cfg := &Config{
		BearerToken: "secret",
		AuthHeaders: map[string]string{"x-api-key": "k", "foo": "bar"},
	}
	ctx := context.Background()
	ctx = injectMetadataIntoContext(ctx, cfg)
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("expected outgoing metadata in context")
	}
	assertMD := func(k, want string) {
		vals := md.Get(k)
		if len(vals) == 0 || vals[0] != want {
			t.Fatalf("metadata %s = %v, want %s", k, vals, want)
		}
	}
	assertMD("authorization", "Bearer secret")
	assertMD("x-api-key", "k")
	assertMD("foo", "bar")
}

func TestOutput_enhanceCallContext_NoOp(t *testing.T) {
	u := &UnifiedOutput{cfg: &Config{}}
	parent, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ctx := u.enhanceCallContext(parent)
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}
}
