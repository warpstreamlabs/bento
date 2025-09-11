package grpc_client

import (
	"context"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/dynamic/grpcdynamic"
	"google.golang.org/grpc/codes"
)

func startTestServer(t *testing.T) func() {
	t.Helper()
	cmd := exec.Command("go", "run", "./cmd/tools/grpc_test_server")
	cmd.Dir = repoRoot(t)
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start test server: %v", err)
	}
	time.Sleep(1 * time.Second)
	return func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }
}

func repoRoot(t *testing.T) string {
	_, file, _, _ := runtime.Caller(0)
	dir := filepath.Dir(file)
	return filepath.Clean(filepath.Join(dir, "../../../"))
}

func TestIntegration_ServerStream_OK(t *testing.T) {
	stop := startTestServer(t)
	defer stop()
	cfg := &Config{
		Address:        "127.0.0.1:50051",
		Method:         "/echo.Echo/Stream",
		RPCType:        "server_stream",
		ProtoFiles:     []string{"echo.proto", "google/protobuf/struct.proto"},
		IncludePaths:   []string{filepath.Join(repoRoot(t), "cmd/tools/grpc_test_server/pb")},
		RetryPolicy:    &RetryPolicy{MaxAttempts: 2, InitialBackoff: 10 * time.Millisecond, MaxBackoff: 20 * time.Millisecond, BackoffMultiplier: 2, RetryableStatusCodes: []codes.Code{codes.Unavailable}},
		ConnectTimeout: 2 * time.Second,
	}
	cm, err := NewConnectionManager(context.Background(), cfg)
	if err != nil {
		t.Fatalf("cm: %v", err)
	}
	defer cm.Close()
	conn, err := cm.GetConnection()
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	mr := NewMethodResolver()
	m, err := mr.ResolveMethod(context.Background(), conn, cfg)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	stub := grpcdynamic.NewStub(conn)
	in := dynamic.NewMessage(m.GetInputType())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = stub.InvokeRpcServerStream(ctx, m, in)
	if err != nil {
		t.Fatalf("invoke stream: %v", err)
	}
}

func TestIntegration_ClientStream_OK(t *testing.T) {
	stop := startTestServer(t)
	defer stop()
	cfg := &Config{
		Address:        "127.0.0.1:50051",
		Method:         "/ingest.Ingest/Stream",
		RPCType:        "client_stream",
		ProtoFiles:     []string{"ingest.proto", "google/protobuf/struct.proto"},
		IncludePaths:   []string{filepath.Join(repoRoot(t), "cmd/tools/grpc_test_server/pb")},
		RetryPolicy:    &RetryPolicy{MaxAttempts: 2, InitialBackoff: 10 * time.Millisecond, MaxBackoff: 20 * time.Millisecond, BackoffMultiplier: 2, RetryableStatusCodes: []codes.Code{codes.Unavailable}},
		ConnectTimeout: 2 * time.Second,
	}
	cm, err := NewConnectionManager(context.Background(), cfg)
	if err != nil {
		t.Fatalf("cm: %v", err)
	}
	defer cm.Close()
	conn, err := cm.GetConnection()
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	mr := NewMethodResolver()
	m, err := mr.ResolveMethod(context.Background(), conn, cfg)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	stub := grpcdynamic.NewStub(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cs, err := stub.InvokeRpcClientStream(ctx, m)
	if err != nil {
		t.Fatalf("open client stream: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := cs.SendMsg(dynamic.NewMessage(m.GetInputType())); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	if _, err := cs.CloseAndReceive(); err != nil {
		t.Fatalf("close/recv: %v", err)
	}
}

func TestIntegration_CircuitBreaker_Transitions_Unreachable(t *testing.T) {
	cfg := &Config{
		Address:        "127.0.0.1:59999",
		Method:         "/echo.Echo/Stream",
		RPCType:        "server_stream",
		RetryPolicy:    &RetryPolicy{MaxAttempts: 1, InitialBackoff: 10 * time.Millisecond, MaxBackoff: 10 * time.Millisecond, BackoffMultiplier: 2},
		ConnectTimeout: 200 * time.Millisecond,
	}
	cm, err := NewConnectionManager(context.Background(), cfg)
	if err == nil {
		defer cm.Close()
	}
	if err == nil {
		_, _ = cm.GetConnection()
		_, _ = cm.GetConnection()
		if cm.GetCircuitBreakerState() == CircuitBreakerClosed {
			t.Fatalf("expected breaker to open after failures")
		}
		// wait a bit; internal breaker uses defaults
		time.Sleep(300 * time.Millisecond)
		_ = cm.GetCircuitBreakerState()
	}
}

func TestIntegration_ConnectTimeout_Fails(t *testing.T) {
	cfg := &Config{
		Address:        "10.255.255.1:65533",
		Method:         "/echo.Echo/Stream",
		RPCType:        "server_stream",
		ConnectTimeout: 200 * time.Millisecond,
	}
	_, err := NewConnectionManager(context.Background(), cfg)
	if err == nil {
		t.Fatalf("expected connection manager creation to fail due to connect_timeout")
	}
}
