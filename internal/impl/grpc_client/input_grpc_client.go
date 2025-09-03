package grpc_client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/dynamic/grpcdynamic"
	"google.golang.org/protobuf/encoding/protojson"
	structpb "google.golang.org/protobuf/types/known/structpb"

	"github.com/warpstreamlabs/bento/public/service"
)

func genericInputSpec() *service.ConfigSpec {
	return createBaseConfigSpec().
		Summary("Call an arbitrary gRPC method (unary or server-stream) using reflection to resolve types with enhanced security and performance").
		Field(service.NewStringField(fieldRPCType).Default("server_stream")).
		Field(service.NewStringField(fieldRequestJSON).Default("{}").Description("JSON request body used for unary or initial server-stream request"))
}

// genericInput handles both unary and server-streaming gRPC input
type genericInput struct {
	cfg            *Config
	connMgr        *ConnectionManager
	methodResolver *MethodResolver
	reqIS          *service.InterpolatedString
	method         *desc.MethodDescriptor

	// Server streaming state with proper cleanup
	mu           sync.Mutex
	streamCtx    context.Context
	streamCancel context.CancelFunc
	stream       *grpcdynamic.ServerStream
	streamOpen   bool
	shutdown     bool
	retryConfig  RetryConfig
}

func newGenericInput(conf *service.ParsedConfig, res *service.Resources) (service.Input, error) {
	cfg, err := ParseConfigFromService(conf)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Attach logger for common code
	cfg.Logger = res.Logger()

	reqIS, err := service.NewInterpolatedString(cfg.RequestJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to create interpolated string: %w", err)
	}

	connMgr, err := NewConnectionManager(context.Background(), cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection manager: %w", err)
	}

	methodResolver := NewMethodResolver()

	conn, err := connMgr.GetConnection()
	if err != nil {
		connMgr.Close()
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}

	method, err := methodResolver.ResolveMethod(context.Background(), conn, cfg)
	if err != nil {
		connMgr.Close()
		return nil, fmt.Errorf("failed to resolve method: %w", err)
	}

	return &genericInput{
		cfg:            cfg,
		connMgr:        connMgr,
		methodResolver: methodResolver,
		reqIS:          reqIS,
		method:         method,
		retryConfig: func() RetryConfig {
			r := DefaultRetryConfig()
			if cfg.RetryPolicy != nil {
				r.InitialBackoff = cfg.RetryPolicy.InitialBackoff
				r.MaxBackoff = cfg.RetryPolicy.MaxBackoff
				// Approximate: MaxRetries = MaxAttempts-1
				if cfg.RetryPolicy.MaxAttempts > 0 {
					r.MaxRetries = cfg.RetryPolicy.MaxAttempts - 1
				}
			}
			return r
		}(),
	}, nil
}

func (g *genericInput) Connect(_ context.Context) error {
	return nil
}

func (g *genericInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	g.mu.Lock()
	if g.shutdown {
		g.mu.Unlock()
		return nil, nil, service.ErrNotConnected
	}
	g.mu.Unlock()

	if g.method == nil {
		return nil, nil, service.ErrNotConnected
	}

	// Build request message from JSON with optional pooling
	var requestMsg *dynamic.Message
	var shouldReturnToPool bool

	// Use message pool if enabled for better performance
	if inputPool, _ := g.methodResolver.GetMessagePools(g.method.GetFullyQualifiedName()); inputPool != nil {
		requestMsg = inputPool.Get()
		shouldReturnToPool = true
	} else {
		requestMsg = dynamic.NewMessage(g.method.GetInputType())
	}

	// Ensure message is returned to pool when done
	if shouldReturnToPool {
		defer func() {
			if inputPool, _ := g.methodResolver.GetMessagePools(g.method.GetFullyQualifiedName()); inputPool != nil {
				inputPool.Put(requestMsg)
			}
		}()
	}

	reqJSON, rerr := g.reqIS.TryString(service.NewMessage(nil))
	if rerr != nil {
		return nil, nil, fmt.Errorf("failed to render request_json: %w", rerr)
	}
	if reqJSON == "" {
		reqJSON = "{}"
	}
	if uerr := requestMsg.UnmarshalJSON([]byte(reqJSON)); uerr != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal request JSON: %w", uerr)
	}

	switch g.cfg.RPCType {
	case "unary":
		return g.handleUnaryCall(ctx, requestMsg)
	case "server_stream":
		return g.handleServerStreamCall(ctx, requestMsg, reqJSON)
	default:
		return nil, nil, fmt.Errorf("unsupported rpc_type for input: %s", g.cfg.RPCType)
	}
}

func (g *genericInput) handleUnaryCall(ctx context.Context, requestMsg *dynamic.Message) (*service.Message, service.AckFunc, error) {
	// Validate method type
	if g.method.IsServerStreaming() || g.method.IsClientStreaming() {
		return nil, nil, fmt.Errorf("method %s is not unary", g.method.GetFullyQualifiedName())
	}

	conn, err := g.connMgr.GetConnection()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get connection: %w", err)
	}

	stub := grpcdynamic.NewStub(conn)

	// Enhanced context handling with proper deadline propagation
	callCtx := g.enhanceCallContext(ctx)
	callCtx = context.WithValue(callCtx, ctxKeyConnMgr, g.connMgr)
	var cancel context.CancelFunc

	if g.cfg.CallTimeout > 0 {
		callCtx, cancel = context.WithTimeout(callCtx, g.cfg.CallTimeout)
		defer cancel()
	} else if _, hasDeadline := callCtx.Deadline(); !hasDeadline {
		// Apply default timeout for unary input calls
		callCtx, cancel = context.WithTimeout(callCtx, 30*time.Second)
		defer cancel()
	}

	resp, err := stub.InvokeRpc(callCtx, g.method, requestMsg)
	if err != nil {
		return nil, nil, formatGrpcError("grpc_client unary call failed", g.method.GetFullyQualifiedName(), err)
	}

	// Handle different response types
	var respBytes []byte
	switch v := resp.(type) {
	case *dynamic.Message:
		respBytes, err = v.MarshalJSON()
	case *structpb.Struct:
		m := protojson.MarshalOptions{EmitUnpopulated: g.cfg.JSONEmitDefaults, UseProtoNames: g.cfg.JSONUseProtoNames, AllowPartial: true, Multiline: false, Indent: ""}
		respBytes, err = m.Marshal(v)
	default:
		return nil, nil, fmt.Errorf("unexpected response type from unary call: %T", resp)
	}

	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal response: %w", err)
	}

	msg := service.NewMessage(respBytes)
	return msg, func(context.Context, error) error { return nil }, nil
}

func (g *genericInput) handleServerStreamCall(ctx context.Context, requestMsg *dynamic.Message, reqJSON string) (*service.Message, service.AckFunc, error) {
	// Validate method type
	if !g.method.IsServerStreaming() || g.method.IsClientStreaming() {
		return nil, nil, fmt.Errorf("method %s is not server-streaming", g.method.GetFullyQualifiedName())
	}

	// Ensure stream is open
	if err := g.ensureStreamOpen(ctx, requestMsg); err != nil {
		return nil, nil, fmt.Errorf("failed to open stream: %w", err)
	}

	for {
		g.mu.Lock()
		if g.shutdown || g.stream == nil {
			g.mu.Unlock()
			return nil, nil, service.ErrNotConnected
		}
		stream := g.stream
		g.mu.Unlock()

		resp, err := stream.RecvMsg()
		if err == nil {
			// Handle different response types
			var respBytes []byte
			var marshalErr error

			switch v := resp.(type) {
			case *dynamic.Message:
				// Direct dynamic message
				respBytes, marshalErr = v.MarshalJSON()
			case *structpb.Struct:
				// google.protobuf.Struct
				m := protojson.MarshalOptions{EmitUnpopulated: g.cfg.JSONEmitDefaults, UseProtoNames: g.cfg.JSONUseProtoNames, AllowPartial: true, Multiline: false, Indent: ""}
				respBytes, marshalErr = m.Marshal(v)
			default:
				return nil, nil, fmt.Errorf("unexpected stream response type: %T", resp)
			}

			if marshalErr != nil {
				return nil, nil, fmt.Errorf("failed to marshal stream response: %w", marshalErr)
			}

			msg := service.NewMessage(respBytes)
			return msg, func(context.Context, error) error { return nil }, nil
		}

		if errors.Is(err, io.EOF) {
			return nil, nil, service.ErrEndOfInput
		}

		// Stream failed, attempt to reopen with retry
		if reopenErr := g.reopenStreamWithRetry(ctx, reqJSON); reopenErr != nil {
			return nil, nil, formatGrpcError("grpc_client failed to reopen server stream", g.method.GetFullyQualifiedName(), reopenErr)
		}
	}
}

func (g *genericInput) ensureStreamOpen(ctx context.Context, requestMsg *dynamic.Message) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.streamOpen && g.stream != nil {
		return nil
	}

	return g.openStreamLocked(ctx, requestMsg)
}

func (g *genericInput) openStreamLocked(ctx context.Context, requestMsg *dynamic.Message) error {
	// Close existing stream if any
	g.closeStreamLocked()

	conn, err := g.connMgr.GetConnection()
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}

	stub := grpcdynamic.NewStub(conn)

	// Enhanced context handling for server streaming
	streamCtx := g.enhanceCallContext(ctx)
	streamCtx = context.WithValue(streamCtx, ctxKeyConnMgr, g.connMgr)
	var cancel context.CancelFunc

	if g.cfg.CallTimeout > 0 {
		streamCtx, cancel = context.WithTimeout(streamCtx, g.cfg.CallTimeout)
	} else {
		// Apply default timeout for server streaming
		defaultStreamTimeout := 15 * time.Minute
		streamCtx, cancel = context.WithTimeout(streamCtx, defaultStreamTimeout)
	}

	stream, err := stub.InvokeRpcServerStream(streamCtx, g.method, requestMsg)
	if err != nil {
		cancel()
		return fmt.Errorf("failed to invoke server stream: %w", err)
	}

	g.streamCtx = streamCtx
	g.streamCancel = cancel
	g.stream = stream
	g.streamOpen = true

	return nil
}

// enhanceCallContext enhances the context for gRPC calls with proper deadline and metadata handling
func (g *genericInput) enhanceCallContext(ctx context.Context) context.Context {
	return enhanceCallContext(ctx, g.cfg, func(c context.Context) context.Context {
		return injectMetadataIntoContext(c, g.cfg)
	})
}

func (g *genericInput) reopenStreamWithRetry(ctx context.Context, reqJSON string) error {
	// Use message pool if enabled for better performance
	var requestMsg *dynamic.Message
	var shouldReturnToPool bool

	if inputPool, _ := g.methodResolver.GetMessagePools(g.method.GetFullyQualifiedName()); inputPool != nil {
		requestMsg = inputPool.Get()
		shouldReturnToPool = true
	} else {
		requestMsg = dynamic.NewMessage(g.method.GetInputType())
	}

	// Ensure message is returned to pool when done
	if shouldReturnToPool {
		defer func() {
			if inputPool, _ := g.methodResolver.GetMessagePools(g.method.GetFullyQualifiedName()); inputPool != nil {
				inputPool.Put(requestMsg)
			}
		}()
	}

	if err := requestMsg.UnmarshalJSON([]byte(reqJSON)); err != nil {
		return fmt.Errorf("failed to unmarshal request for retry: %w", err)
	}

	return WithContextRetry(ctx, g.retryConfig, func() error {
		g.mu.Lock()
		defer g.mu.Unlock()

		if g.shutdown {
			return errors.New("input is shutting down")
		}

		return g.openStreamLocked(ctx, requestMsg)
	})
}

func (g *genericInput) closeStreamLocked() {
	if g.streamCancel != nil {
		g.streamCancel()
		g.streamCancel = nil
	}
	g.stream = nil
	g.streamOpen = false
}

func (g *genericInput) Close(ctx context.Context) error {
	g.mu.Lock()
	g.shutdown = true
	g.closeStreamLocked()
	g.mu.Unlock()

	if g.connMgr != nil {
		return g.connMgr.Close()
	}
	return nil
}

func init() {
	_ = service.RegisterInput("grpc_client", genericInputSpec(), func(conf *service.ParsedConfig, res *service.Resources) (service.Input, error) {
		return newGenericInput(conf, res)
	})
}
