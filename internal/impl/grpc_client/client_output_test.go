package grpc_client

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	test_server "github.com/warpstreamlabs/bento/internal/impl/grpc_client/grpc_test_server"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/transaction"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	_ "github.com/warpstreamlabs/bento/public/components/io"
	_ "github.com/warpstreamlabs/bento/public/components/pure"
	"github.com/warpstreamlabs/bento/public/service"
)

//------------------------------------------------------------------------------

type testServer struct {
	test_server.UnimplementedGreeterServer

	reflection   bool
	tls          bool
	oauth2       bool
	healthCheck  bool
	returnErrors bool

	oauthAddress string

	mu                           sync.Mutex
	SayHelloInvocations          int
	SayMultiHellosInvocations    int
	SayHelloHowAreYouInvocations int
	SayHelloBidiInvocations      int
	port                         int
}

func startGRPCServer(t *testing.T, opts ...testServerOpt) *testServer {
	t.Helper()

	testServer := &testServer{}

	for _, o := range opts {
		o(testServer)
	}

	lis, err := net.Listen("tcp", "localhost:0")
	assert.NoError(t, err)

	testServer.port = lis.Addr().(*net.TCPAddr).Port

	serverOpts := []grpc.ServerOption{}

	if testServer.tls {
		serverCert, err := tls.LoadX509KeyPair(
			"./grpc_test_server/certs/server.pem",
			"./grpc_test_server/certs/server.key",
		)
		if err != nil {
			t.Fatal(err)
		}

		caCert, err := os.ReadFile("./grpc_test_server/certs/ca.pem")
		if err != nil {
			t.Fatal(err)
		}

		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(caCert) {
			t.Fatal("")
		}

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{serverCert},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    certPool,
		}

		creds := credentials.NewTLS(tlsConfig)
		serverOpts = append(serverOpts, grpc.Creds(creds))
	}

	if testServer.oauth2 {
		tsOAuth2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			username, password, ok := r.BasicAuth()
			require.True(t, ok)
			require.Equal(t, "fookey", username)
			require.Equal(t, "foosecret", password)

			b, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			assert.Equal(t, "grant_type=client_credentials", string(b))

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			resp := `{"access_token":"some-secret-token","token_type":"Bearer","expires_in":3600}`
			_, _ = w.Write([]byte(resp))
		}))

		testServer.oauthAddress = tsOAuth2.URL
		serverOpts = append(serverOpts, grpc.UnaryInterceptor(ensureValidToken))
	}

	s := grpc.NewServer(serverOpts...)

	if testServer.healthCheck {
		hc := health.NewServer()
		healthgrpc.RegisterHealthServer(s, hc)
		hc.SetServingStatus("", healthgrpc.HealthCheckResponse_SERVING)
	}

	if testServer.reflection {
		reflection.Register(s)
	}

	test_server.RegisterGreeterServer(s, testServer)
	go func() {
		err := s.Serve(lis)
		require.NoError(t, err)
	}()
	return testServer
}

//------------------------------------------------------------------------------

func (s *testServer) SayHello(_ context.Context, in *test_server.HelloRequest) (*test_server.HelloReply, error) {
	s.mu.Lock()
	s.SayHelloInvocations++
	s.mu.Unlock()
	if s.returnErrors {
		return nil, errors.New("ERROR :( ")
	}
	return &test_server.HelloReply{Message: "Hello " + in.GetName()}, nil
}

func (s *testServer) SayMultipleHellos(stream test_server.Greeter_SayMultipleHellosServer) error {
	s.mu.Lock()
	s.SayMultiHellosInvocations++
	s.mu.Unlock()
	names := []string{}

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			if s.returnErrors {
				return errors.New("ERROR :( ")
			}

			return stream.SendAndClose(&test_server.HelloReply{
				Message: "Hello " + strings.Join(names, ", "),
			})
		}
		if err != nil {
			return err
		}
		names = append(names, in.Name)
	}
}

func (s *testServer) SayHelloHowAreYou(in *test_server.HelloRequest, stream test_server.Greeter_SayHelloHowAreYouServer) error {
	s.mu.Lock()
	s.SayHelloHowAreYouInvocations++
	s.mu.Unlock()

	if s.returnErrors {
		return errors.New("ERROR :( ")
	}

	helloMsg := &test_server.HelloReply{Message: "Hello " + in.GetName()}
	err := stream.Send(helloMsg)
	if err != nil {
		return err
	}
	howAreYouMsg := &test_server.HelloReply{Message: "How are you, " + in.GetName() + "?"}
	err = stream.Send(howAreYouMsg)
	if err != nil {
		return err
	}
	return nil
}

func (s *testServer) SayHelloBidi(grpc.BidiStreamingServer[test_server.HelloRequest, test_server.HelloReply]) error {
	s.mu.Lock()
	s.SayHelloBidiInvocations++
	s.mu.Unlock()

	if s.returnErrors {
		return errors.New("ERROR :( ")
	}

	return nil
}

//------------------------------------------------------------------------------

var (
	errMissingMetadata = status.Errorf(codes.InvalidArgument, "missing metadata")
	errInvalidToken    = status.Errorf(codes.Unauthenticated, "invalid token")
)

func valid(authorization []string) bool {
	if len(authorization) < 1 {
		return false
	}
	token := strings.TrimPrefix(authorization[0], "Bearer ")

	return token == "some-secret-token"
}

func ensureValidToken(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errMissingMetadata
	}

	if !valid(md["authorization"]) {
		return nil, errInvalidToken
	}

	return handler(ctx, req)
}

//------------------------------------------------------------------------------

type testServerOpt func(*testServer)

func withReflection() testServerOpt {
	return func(ts *testServer) {
		ts.reflection = true
	}
}

func withTLS() testServerOpt {
	return func(ts *testServer) {
		ts.tls = true
	}
}

func withHealthCheck() testServerOpt {
	return func(ts *testServer) {
		ts.healthCheck = true
	}
}

func withOAuth2() testServerOpt {
	return func(ts *testServer) {
		ts.oauth2 = true
	}
}

func withReturnErrors() testServerOpt {
	return func(ts *testServer) {
		ts.returnErrors = true
	}
}

//------------------------------------------------------------------------------

var namesInputTestData = []string{`{"name":"Alice"}`, `{"name":"Bob"}`, `{"name":"Carol"}`, `{"name":"Dan"}`}
var names = []string{"Alice", "Bob", "Carol", "Dan"}

func TestGrpcClientWriter(t *testing.T) {
	tests := map[string]struct {
		grpcServerOpts   []testServerOpt
		confFormatString string
		formatArgs       func(*testServer) []any
		invocations      func(*testServer) int
		expInvocations   int
	}{
		"Unary Reflection": {
			grpcServerOpts: []testServerOpt{withReflection()},
			confFormatString: `
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  reflection: true
`,
			formatArgs: func(ts *testServer) []any {
				return []any{ts.port}
			},
			invocations: func(ts *testServer) int {
				ts.mu.Lock()
				defer ts.mu.Unlock()
				return ts.SayHelloInvocations
			},
			expInvocations: 4,
		},
		"Unary Proto File": {
			grpcServerOpts: []testServerOpt{},
			confFormatString: `
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  proto_files: 
    - "./grpc_test_server/helloworld.proto"
`,
			formatArgs: func(ts *testServer) []any {
				return []any{ts.port}
			},
			invocations: func(ts *testServer) int {
				ts.mu.Lock()
				defer ts.mu.Unlock()
				return ts.SayHelloInvocations
			},
			expInvocations: 4,
		},
		"Client Stream Reflection": {
			grpcServerOpts: []testServerOpt{withReflection()},
			confFormatString: `
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayMultipleHellos
  reflection: true
  rpc_type: client_stream
  batching:
    count: 4
`,
			formatArgs: func(ts *testServer) []any {
				return []any{ts.port}
			},
			invocations: func(ts *testServer) int {
				ts.mu.Lock()
				defer ts.mu.Unlock()
				return ts.SayMultiHellosInvocations
			},
			expInvocations: 1,
		},
		"Server Stream Reflection": {
			grpcServerOpts: []testServerOpt{withReflection()},
			confFormatString: `
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHelloHowAreYou
  reflection: true
  rpc_type: server_stream
`,
			formatArgs: func(ts *testServer) []any {
				return []any{ts.port}
			},
			invocations: func(ts *testServer) int {
				ts.mu.Lock()
				defer ts.mu.Unlock()
				return ts.SayHelloHowAreYouInvocations
			},
			expInvocations: 4,
		},
		"Bidirectional Reflection": {
			grpcServerOpts: []testServerOpt{withReflection()},
			confFormatString: `
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHelloBidi
  reflection: true
  rpc_type: bidi
  batching:
    count: 4
`,
			formatArgs: func(ts *testServer) []any {
				return []any{ts.port}
			},
			invocations: func(ts *testServer) int {
				ts.mu.Lock()
				defer ts.mu.Unlock()
				return ts.SayHelloBidiInvocations
			},
			expInvocations: 1,
		},
		"TLS": {
			grpcServerOpts: []testServerOpt{withReflection(), withTLS()},
			confFormatString: `
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  reflection: true
  tls:
    enabled: true
    root_cas_file: ./grpc_test_server/certs/ca.pem
    client_certs:
      - cert_file: ./grpc_test_server/certs/client.pem
        key_file: ./grpc_test_server/certs/client.key
    skip_cert_verify: false
`,
			formatArgs: func(ts *testServer) []any {
				return []any{ts.port}
			},
			invocations: func(ts *testServer) int {
				ts.mu.Lock()
				defer ts.mu.Unlock()
				return ts.SayHelloInvocations
			},
			expInvocations: 4,
		},
		"TLS and oAuth2": {
			grpcServerOpts: []testServerOpt{withReflection(), withTLS(), withOAuth2()},
			confFormatString: `
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  reflection: true
  oauth2:
    enabled: true
    token_url: %v
    client_key: fookey
    client_secret: foosecret
  tls:
    enabled: true
    root_cas_file: ./grpc_test_server/certs/ca.pem
    client_certs:
      - cert_file: ./grpc_test_server/certs/client.pem
        key_file: ./grpc_test_server/certs/client.key
    skip_cert_verify: false
`,
			formatArgs: func(ts *testServer) []any {
				return []any{ts.port, ts.oauthAddress}
			},
			invocations: func(ts *testServer) int {
				ts.mu.Lock()
				defer ts.mu.Unlock()
				return ts.SayHelloInvocations
			},
			expInvocations: 4,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			testServer := startGRPCServer(t, test.grpcServerOpts...)

			yamlConf := fmt.Sprintf(test.confFormatString, test.formatArgs(testServer)...)

			sendChan, receiveChan, err := startGrpcClientOutput(t, yamlConf)
			require.NoError(t, err)

			for _, input := range namesInputTestData {
				testMsg := message.QuickBatch([][]byte{[]byte(input)})
				select {
				case sendChan <- message.NewTransaction(testMsg, receiveChan):
				case <-time.After(time.Second * 20):
					t.Fatal("Send timed out")
				}
			}

			for i := range namesInputTestData {
				select {
				case err := <-receiveChan:
					require.NoError(t, err)
				case <-time.After(time.Second * 20):
					t.Fatalf("Response %d timed out", i)
				}
			}

			assert.Eventually(t, func() bool {
				return test.expInvocations == test.invocations(testServer)
			}, time.Second*10, time.Millisecond*20)
		})
	}
}

func TestGrpcClientWriterSyncResponse(t *testing.T) {
	tests := map[string]struct {
		grpcServerOpts   []testServerOpt
		confFormatString string
		invocations      func(*testServer) int
		expInvocations   int
		inputs           []string
		waitForBatch     bool
	}{
		"Unary Sync Response": {
			grpcServerOpts: []testServerOpt{withReflection()},
			confFormatString: `
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  propagate_response: true
  reflection: true
`,
			invocations: func(ts *testServer) int {
				ts.mu.Lock()
				defer ts.mu.Unlock()
				return ts.SayHelloInvocations
			},
			expInvocations: 4,
			inputs:         namesInputTestData,
		},
		"Unary Sync Response Protofile": {
			grpcServerOpts: []testServerOpt{},
			confFormatString: `
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  propagate_response: true
  proto_files:
    - "./grpc_test_server/helloworld.proto"
`,
			invocations: func(ts *testServer) int {
				ts.mu.Lock()
				defer ts.mu.Unlock()
				return ts.SayHelloInvocations
			},
			expInvocations: 4,
			inputs:         namesInputTestData,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			testServer := startGRPCServer(t, test.grpcServerOpts...)

			yamlConf := fmt.Sprintf(test.confFormatString, testServer.port)

			sendChan, receiveChan, err := startGrpcClientOutput(t, yamlConf)
			require.NoError(t, err)

			for i, input := range test.inputs {
				testMsg := message.QuickBatch([][]byte{[]byte(input)})
				resultStore := transaction.NewResultStore()
				transaction.AddResultStore(testMsg, resultStore)

				select {
				case sendChan <- message.NewTransaction(testMsg, receiveChan):
				case <-time.After(time.Minute):
					t.Fatal("send timed out")
				}

				select {
				case res := <-receiveChan:
					assert.NoError(t, res)
					resMsgs := resultStore.Get()
					resMsg := resMsgs[0]
					assert.Equal(t, `{"message":"Hello `+names[i]+`"}`, string(resMsg.Get(0).AsBytes()))
				case <-time.After(time.Minute):
					t.Fatal("receive timed out")
				}
			}

			assert.Eventually(t, func() bool {
				return test.expInvocations == test.invocations(testServer)
			}, time.Second*10, time.Millisecond*20)
		})
	}
}

func TestGrpcClientWriterClientStreamSyncResponse(t *testing.T) {
	testServer := startGRPCServer(t, withReflection())

	yamlConf := fmt.Sprintf(`
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayMultipleHellos
  propagate_response: true
  reflection: true
  rpc_type: client_stream
  batching:
    count: 4
`, testServer.port)

	sendChan, receiveChan, err := startGrpcClientOutput(t, yamlConf)
	require.NoError(t, err)

	msgBatch := [][]byte{
		[]byte(`{"name":"Alice"}`),
		[]byte(`{"name":"Bob"}`),
		[]byte(`{"name":"Carol"}`),
		[]byte(`{"name":"Dan"}`),
	}

	names := "Alice, Bob, Carol, Dan"

	testMsg := message.QuickBatch(msgBatch)
	resultStore := transaction.NewResultStore()
	transaction.AddResultStore(testMsg, resultStore)

	select {
	case sendChan <- message.NewTransaction(testMsg, receiveChan):
	case <-time.After(time.Minute):
		t.Fatal("send timed out")
	}

	select {
	case res := <-receiveChan:
		assert.NoError(t, res)
		resBatch := resultStore.Get()
		assert.Equal(t, `{"message":"Hello `+names+`"}`, string(resBatch[0].Get(0).AsBytes()))
		assert.Len(t, resBatch, 1)
	case <-time.After(time.Minute):
		t.Fatal("receive timed out")
	}

	assert.Eventually(t, func() bool {
		return testServer.SayMultiHellosInvocations == 1
	}, time.Second*10, time.Second)
}

func TestGrpcClientWriterServerStreamSyncResponse(t *testing.T) {
	testServer := startGRPCServer(t, withReflection())

	yamlConf := fmt.Sprintf(`
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHelloHowAreYou
  reflection: true
  rpc_type: server_stream
  propagate_response: true
`, testServer.port)

	sendChan, receiveChan, err := startGrpcClientOutput(t, yamlConf)
	require.NoError(t, err)

	inputs := []string{
		`{"name":"Alice"}`, `{"name":"Bob"}`, `{"name":"Carol"}`, `{"name":"Dan"}`,
	}

	names := []string{"Alice", "Bob", "Carol", "Dan"}

	for i, input := range inputs {
		testMsg := message.QuickBatch([][]byte{[]byte(input)})
		resultStore := transaction.NewResultStore()
		transaction.AddResultStore(testMsg, resultStore)

		select {
		case sendChan <- message.NewTransaction(testMsg, receiveChan):
		case <-time.After(time.Minute):
			t.Fatal("send timed out")
		}

		select {
		case res := <-receiveChan:
			assert.NoError(t, res)
			resMsgs := resultStore.Get()
			assert.Equal(t, `{"message":"Hello `+names[i]+`"}`, string(resMsgs[0].Get(0).AsBytes()))
			assert.Equal(t, `{"message":"How are you, `+names[i]+`?"}`, string(resMsgs[0].Get(1).AsBytes()))
		case <-time.After(time.Minute):
			t.Fatal("receive timed out")
		}
	}
	assert.Equal(t, 4, testServer.SayHelloHowAreYouInvocations)
}

func TestGrpcClientWriterHealthCheck(t *testing.T) {
	testServer := startGRPCServer(t, withReflection(), withHealthCheck())

	yamlConf := fmt.Sprintf(`
address: localhost:%v
service: helloworld.Greeter
method: SayHello
reflection: true
health_check:
  enabled: true
  service: ""
`, testServer.port)

	pConf, err := grcpClientOutputSpec().ParseYAML(yamlConf, nil)
	require.NoError(t, err)

	foo, err := newGrpcClientWriterFromParsed(pConf, nil)
	require.NoError(t, err)

	ctx := context.Background()
	err = foo.Connect(ctx)
	assert.NoError(t, err)
}

func TestGrpcClientWriterUnableToFindMethodErr(t *testing.T) {
	testServer := startGRPCServer(t, withReflection())

	sb := service.NewStreamBuilder()

	err := sb.SetYAML(fmt.Sprintf(`
input:
  generate:
    mapping: root.name = "Alice"
    count: 1

output:
  grpc_client:
    address: localhost:%v
    service: helloworld.Greeter
    method: DoesNotExist
    reflection: true
`, testServer.port))
	require.NoError(t, err)

	buffer := new(bytes.Buffer)
	logger := slog.New(slog.NewTextHandler(buffer, nil))

	sb.SetLogger(logger)

	stream, err := sb.Build()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	streamErrChan := make(chan error, 1)

	go func() {
		streamErrChan <- stream.Run(ctx)
	}()
	err = <-streamErrChan
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		require.NoError(t, err)
	}

	err = stream.Stop(ctx)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		require.NoError(t, err)
	}

	assert.Contains(t, buffer.String(), "method: DoesNotExist not found")
}

func TestGrpcClientWriterBrokenProtoFile(t *testing.T) {
	testServer := startGRPCServer(t, withReflection())

	sb := service.NewStreamBuilder()

	err := sb.SetYAML(fmt.Sprintf(`
input:
  generate:
    mapping: root.name = "Alice"
    count: 1

output:
  grpc_client:
    address: localhost:%v
    service: helloworld.Greeter
    method: SayHello
    proto_files: 
      - "./grpc_test_server/fail_parse.proto"
`, testServer.port))
	require.NoError(t, err)

	buffer := new(bytes.Buffer)
	logger := slog.New(slog.NewTextHandler(buffer, nil))

	sb.SetLogger(logger)

	stream, err := sb.Build()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	streamErrChan := make(chan error, 1)

	go func() {
		streamErrChan <- stream.Run(ctx)
	}()
	err = <-streamErrChan
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		require.NoError(t, err)
	}

	err = stream.Stop(ctx)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		require.NoError(t, err)
	}

	assert.Contains(t, buffer.String(), "syntax error: unexpected '='")
}

func TestGrpcClientWriterLints(t *testing.T) {

	tests := map[string]struct {
		config          string
		expErrMessage   string
		noErrorExpected bool
	}{"Bidi and Propagate Response Lint Check": {
		config: `
grpc_client:
  address: localhost:55001
  service: helloworld.Greeter
  method: Foo
  rpc_type: bidi
  propagate_response: true
`,
		expErrMessage: "cannot set propagate_response to true when rpc_type is bidi",
	},
		"Reflection or Proto Files Required": {
			config: `
grpc_client:
  address: localhost:55001
  service: helloworld.Greeter
  method: Foo
  reflection: false
  proto_files: []
`,
			expErrMessage: "reflection must be true or proto_files must be populated",
		},
		"Reflection or Proto Files Required Default": {
			config: `
grpc_client:
  address: localhost:55001
  service: helloworld.Greeter
  method: Foo
`,
			expErrMessage: "reflection must be true or proto_files must be populated",
		},
		"Reflection or Proto Files Required Proto Files": {
			config: `
grpc_client:
  address: localhost:55001
  service: helloworld.Greeter
  method: Foo
  proto_files: ["./grpc_test_server/helloworld.proto"]
`,
			noErrorExpected: true,
		},
		"Reflection or Proto Files Required Reflection": {
			config: `
grpc_client:
  address: localhost:55001
  service: helloworld.Greeter
  method: Foo
  reflection: true
`,
			noErrorExpected: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := service.NewStreamBuilder().AddOutputYAML(test.config)
			if test.noErrorExpected {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, test.expErrMessage)
			}
		})
	}
}

func TestGrpcClientWriterBatchErrors(t *testing.T) {
	tests := map[string]struct {
		grpcServerOpts   []testServerOpt
		confFormatString string
		formatArgs       func(*testServer) []any
		invocations      func(*testServer) int
	}{
		"Unary Reflection Errors": {
			grpcServerOpts: []testServerOpt{withReflection(), withReturnErrors()},
			confFormatString: `
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  reflection: true
`,
			formatArgs: func(ts *testServer) []any {
				return []any{ts.port}
			},
			invocations: func(ts *testServer) int {
				ts.mu.Lock()
				defer ts.mu.Unlock()
				return ts.SayHelloInvocations
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			testServer := startGRPCServer(t, test.grpcServerOpts...)

			yamlConf := fmt.Sprintf(test.confFormatString, test.formatArgs(testServer)...)

			sendChan, receiveChan, err := startGrpcClientOutput(t, yamlConf)
			require.NoError(t, err)

			for _, input := range namesInputTestData {
				testMsg := message.QuickBatch([][]byte{[]byte(input)})
				select {
				case sendChan <- message.NewTransaction(testMsg, receiveChan):
				case <-time.After(time.Second * 20):
					t.Fatal("Send timed out")
				}
			}

			for i := range namesInputTestData {
				select {
				case err := <-receiveChan:
					assert.ErrorContains(t, err, "ERROR :( ")
				case <-time.After(time.Second * 20):
					t.Fatalf("Response %d timed out", i)
				}
			}
		})
	}
}

//------------------------------------------------------------------------------

func startGrpcClientOutput(t *testing.T, yamlConf string) (
	sendChan chan message.Transaction,
	receiveChan chan error,
	err error,
) {
	t.Helper()

	conf, err := testutil.OutputFromYAML(yamlConf)
	if err != nil {
		return
	}

	s, err := mock.NewManager().NewOutput(conf)
	if err != nil {
		return
	}

	sendChan = make(chan message.Transaction)
	receiveChan = make(chan error)

	err = s.Consume(sendChan)
	if err != nil {
		return
	}
	t.Cleanup(s.TriggerCloseNow)

	return sendChan, receiveChan, nil
}
