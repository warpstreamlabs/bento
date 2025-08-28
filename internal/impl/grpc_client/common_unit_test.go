package grpc_client

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func Test_injectMetadataIntoContext_Base64Bin(t *testing.T) {
	ctx := context.Background()
	cfg := &Config{
		AuthHeaders: map[string]string{
			"foo":     "bar",
			"bin-key": "YmFy", // will be treated as plain in simplified path
		},
		BearerToken: "tok",
	}
	ctx = injectMetadataIntoContext(ctx, cfg)
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatalf("expected metadata in context")
	}
	if got := md.Get("foo"); len(got) != 1 || got[0] != "bar" {
		t.Fatalf("expected foo=bar, got %v", got)
	}
	if got := md.Get("authorization"); len(got) != 1 || got[0] != "Bearer tok" {
		t.Fatalf("expected authorization bearer, got %v", got)
	}
}

func Test_parseMethodName(t *testing.T) {
	svc, m, err := parseMethodName("/echo.Echo/Stream")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if svc != "echo.Echo" || m != "Stream" {
		t.Fatalf("unexpected parsed: %s %s", svc, m)
	}

	if _, _, err := parseMethodName("bad"); err == nil {
		t.Fatalf("expected error for invalid method format")
	}
}

func Test_MethodResolver_FromProtoFiles(t *testing.T) {
	mr := NewMethodResolver()
	// Build include path relative to this file
	_, file, _, _ := runtime.Caller(0)
	dir := filepath.Dir(file)
	include := filepath.Clean(filepath.Join(dir, "../../../cmd/tools/grpc_test_server/pb"))

	cfg := &Config{
		Method:       "/echo.Echo/Stream",
		ProtoFiles:   []string{"echo.proto", "google/protobuf/struct.proto"},
		IncludePaths: []string{include},
	}
	method, err := mr.ResolveMethod(context.Background(), nil, cfg)
	if err != nil {
		t.Fatalf("ResolveMethod failed: %v", err)
	}
	if method == nil || !method.IsServerStreaming() || method.IsClientStreaming() {
		t.Fatalf("unexpected method streaming flags")
	}
}

func Test_CircuitBreaker_Transitions(t *testing.T) {
	cb := NewCircuitBreaker(2, 10*time.Millisecond)
	if !cb.CanExecute() {
		t.Fatalf("expected can execute in closed state")
	}
	cb.RecordFailure()
	if !cb.CanExecute() {
		t.Fatalf("should still execute after first failure")
	}
	cb.RecordFailure()
	if cb.CanExecute() {
		t.Fatalf("should not execute when open")
	}
	time.Sleep(15 * time.Millisecond)
	if !cb.CanExecute() {
		t.Fatalf("expected half-open allowing limited requests")
	}
	cb.RecordSuccess()
	cb.RecordSuccess()
	cb.RecordSuccess() // should transition to closed
	if cb.GetState() != CircuitBreakerClosed {
		t.Fatalf("expected closed after successes, got %v", cb.GetState())
	}
}

func Test_WithContextRetry_Attempts(t *testing.T) {
	cfg := RetryConfig{InitialBackoff: 1 * time.Millisecond, MaxBackoff: 2 * time.Millisecond, MaxRetries: 3}
	attempts := 0
	err := WithContextRetry(context.Background(), cfg, func() error {
		attempts++
		if attempts < 4 {
			return status.Error(codes.Unavailable, "retry me")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if attempts != 4 {
		t.Fatalf("expected 4 attempts, got %d", attempts)
	}
}

func Test_ClassifyGrpcError_Status(t *testing.T) {
	st := status.Error(codes.Unavailable, "svc unavailable")
	e := classifyGrpcError("/svc/m", st)
	if e == nil || !e.IsRetryable() || e.Type != ErrorTypeUnavailable {
		t.Fatalf("unexpected classification: %+v", e)
	}
}

func Test_headerCreds_GetRequestMetadata(t *testing.T) {
	h := headerCreds{
		token: "abc",
		headers: map[string]string{
			"X-Foo": "bar",
			"bad":   "line\r\ninjection",
		},
	}
	md, err := h.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if md["authorization"] != "Bearer abc" {
		t.Fatalf("expected auth bearer, got %v", md["authorization"])
	}
	if md["x-foo"] != "bar" {
		t.Fatalf("expected x-foo=bar, got %v", md["x-foo"])
	}
	if _, ok := md["bad"]; ok {
		t.Fatalf("expected invalid header to be skipped")
	}
}

func Test_deadlineUnaryInterceptor_SetsDefault(t *testing.T) {
	var hadDeadline bool
	inv := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		_, hadDeadline = ctx.Deadline()
		return nil
	}
	_ = deadlineUnaryInterceptor(context.Background(), "/svc/Method", nil, nil, nil, inv)
	if !hadDeadline {
		t.Fatalf("expected deadline to be set by interceptor")
	}
}

func Test_deadlineUnaryInterceptor_PreservesExisting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	var seen time.Time
	inv := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		seen, _ = ctx.Deadline()
		return nil
	}
	_ = deadlineUnaryInterceptor(ctx, "/svc/Method", nil, nil, nil, inv)
	if time.Until(seen) <= 0 {
		t.Fatalf("expected existing deadline to be preserved")
	}
}
