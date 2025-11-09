package grpc

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
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
	test_server "github.com/warpstreamlabs/bento/internal/impl/grpc/grpc_test_server"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/transaction"
	"github.com/warpstreamlabs/bento/public/service"
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
)

//------------------------------------------------------------------------------

type testServer struct {
	test_server.UnimplementedGreeterServer

	reflection  bool
	tls         bool
	oauth2      bool
	healthCheck bool

	mu                           sync.Mutex
	sayHelloInvocations          int
	sayMultiHellosInvocations    int
	sayHelloHowAreYouInvocations int
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
	go s.Serve(lis)
	return testServer
}

//------------------------------------------------------------------------------

func (s *testServer) SayHello(_ context.Context, in *test_server.HelloRequest) (*test_server.HelloReply, error) {
	s.mu.Lock()
	s.sayHelloInvocations++
	s.mu.Unlock()
	return &test_server.HelloReply{Message: "Hello " + in.GetName()}, nil
}

func (s *testServer) SayMultipleHellos(stream test_server.Greeter_SayMultipleHellosServer) error {
	s.mu.Lock()
	s.sayMultiHellosInvocations++
	s.mu.Unlock()
	names := []string{}

	for {
		in, err := stream.Recv()
		if err == io.EOF {
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
	s.sayHelloHowAreYouInvocations++
	s.mu.Unlock()

	helloMsg := &test_server.HelloReply{Message: "Hello " + in.GetName()}
	stream.Send(helloMsg)
	howAreYouMsg := &test_server.HelloReply{Message: "How are you, " + in.GetName() + "?"}
	stream.Send(howAreYouMsg)
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

//------------------------------------------------------------------------------

func TestGrpcClientWriterBasicReflection(t *testing.T) {
	testServer := startGRPCServer(t, withReflection())

	yamlConf := fmt.Sprintf(`
grpc_client_jem:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  reflection: true
`, testServer.port)

	sendChan, receiveChan, err := startGrpcClientOutput(t, yamlConf)
	assert.NoError(t, err)

	inputs := []string{
		`{"name":"Alice"}`, `{"name":"Bob"}`, `{"name":"Carol"}`, `{"name":"Dan"}`,
	}

	for _, input := range inputs {
		testMsg := message.QuickBatch([][]byte{[]byte(input)})
		select {
		case sendChan <- message.NewTransaction(testMsg, receiveChan):
		case <-time.After(time.Minute):
			t.Fatal("Action timed out")
		}

		select {
		case res := <-receiveChan:
			assert.NoError(t, res)
		case <-time.After(time.Minute):
			t.Fatal("Action timed out")
		}
	}

	assert.Equal(t, 4, testServer.sayHelloInvocations)
}

func TestGrpcClientWriterSyncResponseReflection(t *testing.T) {
	testServer := startGRPCServer(t, withReflection())

	yamlConf := fmt.Sprintf(`
grpc_client_jem:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  propagate_response: true
  reflection: true
`, testServer.port)

	sendChan, receiveChan, err := startGrpcClientOutput(t, yamlConf)
	assert.NoError(t, err)

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
			t.Fatal("Action timed out")
		}

		select {
		case res := <-receiveChan:
			assert.NoError(t, res)
			resMsgs := resultStore.Get()
			resMsg := resMsgs[0]
			assert.Equal(t, `{"message":"Hello `+names[i]+`"}`, string(resMsg.Get(0).AsBytes()))
		case <-time.After(time.Minute):
			t.Fatal("Action timed out")
		}
	}

	assert.Equal(t, 4, testServer.sayHelloInvocations)
}

func TestGrpcClientWriterTLS(t *testing.T) {
	testServer := startGRPCServer(t, withReflection(), withTLS())

	yamlConf := fmt.Sprintf(`
grpc_client_jem:
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
`, testServer.port)

	sendChan, receiveChan, err := startGrpcClientOutput(t, yamlConf)
	assert.NoError(t, err)

	inputs := []string{
		`{"name":"Alice"}`, `{"name":"Bob"}`, `{"name":"Carol"}`, `{"name":"Dan"}`,
	}

	for _, input := range inputs {
		testMsg := message.QuickBatch([][]byte{[]byte(input)})
		select {
		case sendChan <- message.NewTransaction(testMsg, receiveChan):
		case <-time.After(time.Minute):
			t.Fatal("Action timed out")
		}

		select {
		case res := <-receiveChan:
			assert.NoError(t, res)
		case <-time.After(time.Minute):
			t.Fatal("Action timed out")
		}
	}

	assert.Equal(t, 4, testServer.sayHelloInvocations)
}

func TestGrpcClientWriterBasicProtoFile(t *testing.T) {
	testServer := startGRPCServer(t)

	yamlConf := fmt.Sprintf(`
grpc_client_jem:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  proto_files: 
    - "./grpc_test_server/helloworld.proto"
`, testServer.port)

	sendChan, receiveChan, err := startGrpcClientOutput(t, yamlConf)
	assert.NoError(t, err)

	inputs := []string{
		`{"name":"Alice"}`, `{"name":"Bob"}`, `{"name":"Carol"}`, `{"name":"Dan"}`,
	}

	for _, input := range inputs {
		testMsg := message.QuickBatch([][]byte{[]byte(input)})
		select {
		case sendChan <- message.NewTransaction(testMsg, receiveChan):
		case <-time.After(time.Minute):
			t.Fatal("Action timed out")
		}

		select {
		case res := <-receiveChan:
			assert.NoError(t, res)
		case <-time.After(time.Minute):
			t.Fatal("Action timed out")
		}
	}

	assert.Equal(t, 4, testServer.sayHelloInvocations)
}

func TestGrpcClientWriterBasicProtoFileSyncResponse(t *testing.T) {
	testServer := startGRPCServer(t)

	yamlConf := fmt.Sprintf(`
grpc_client_jem:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  proto_files: 
    - "./grpc_test_server/helloworld.proto"
  propagate_response: true
`, testServer.port)

	sendChan, receiveChan, err := startGrpcClientOutput(t, yamlConf)
	assert.NoError(t, err)

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
			t.Fatal("Action timed out")
		}

		select {
		case res := <-receiveChan:
			assert.NoError(t, res)
			resMsgs := resultStore.Get()
			resMsg := resMsgs[0]
			assert.Equal(t, `{"message":"Hello `+names[i]+`"}`, string(resMsg.Get(0).AsBytes()))
		case <-time.After(time.Minute):
			t.Fatal("Action timed out")
		}
	}

	assert.Equal(t, 4, testServer.sayHelloInvocations)
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
	require.NoError(t, err)
}

func TestGrpcClientWriterUnableToFindMethodErr(t *testing.T) {
	testServer := startGRPCServer(t, withReflection())

	sb := service.NewStreamBuilder()

	sb.SetYAML(fmt.Sprintf(`
input:
  generate:
    mapping: root.name = "Alice"
    count: 1

output:
  grpc_client_jem:
    address: localhost:%v
    service: helloworld.Greeter
    method: DoesNotExist
    reflection: true
`, testServer.port))

	buffer := new(bytes.Buffer)
	logger := slog.New(slog.NewTextHandler(buffer, nil))

	sb.SetLogger(logger)

	stream, err := sb.Build()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	go stream.Run(ctx)

	assert.Eventually(t, func() bool {
		return strings.Contains(buffer.String(), "method: DoesNotExist not found")
	}, time.Second*10, time.Second)
}

func TestGrpcClientWriterClientSideStream(t *testing.T) {
	testServer := startGRPCServer(t, withReflection())

	yamlConf := fmt.Sprintf(`
grpc_client_jem:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayMultipleHellos
  reflection: true
  rpc_type: client_stream
  batching:
    count: 4
`, testServer.port)

	sendChan, receiveChan, err := startGrpcClientOutput(t, yamlConf)
	assert.NoError(t, err)

	msgBatch := [][]byte{
		[]byte(`{"name":"Alice"}`),
		[]byte(`{"name":"Bob"}`),
		[]byte(`{"name":"Carol"}`),
		[]byte(`{"name":"Dan"}`),
	}

	testMsg := message.QuickBatch(msgBatch)
	resultStore := transaction.NewResultStore()
	transaction.AddResultStore(testMsg, resultStore)

	select {
	case sendChan <- message.NewTransaction(testMsg, receiveChan):
	case <-time.After(time.Minute):
		t.Fatal("Action timed out")
	}

	select {
	case res := <-receiveChan:
		assert.NoError(t, res)
	case <-time.After(time.Minute):
		t.Fatal("Action timed out")
	}

	assert.Eventually(t, func() bool {
		return testServer.sayMultiHellosInvocations == 1
	}, time.Second*10, time.Second)
}

func TestGrpcClientWriterClientStreamSyncResponse(t *testing.T) {
	testServer := startGRPCServer(t, withReflection())

	yamlConf := fmt.Sprintf(`
grpc_client_jem:
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
	assert.NoError(t, err)

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
		t.Fatal("Action timed out")
	}

	select {
	case res := <-receiveChan:
		assert.NoError(t, res)
		resMsgs := resultStore.Get()
		resMsg := resMsgs[0]
		assert.Equal(t, `{"message":"Hello `+names+`"}`, string(resMsg.Get(0).AsBytes()))
	case <-time.After(time.Minute):
		t.Fatal("Action timed out")
	}

	assert.Eventually(t, func() bool {
		return testServer.sayMultiHellosInvocations == 1
	}, time.Second*10, time.Second)
}

func TestGrpcClientWriterServerStream(t *testing.T) {
	testServer := startGRPCServer(t, withReflection())

	yamlConf := fmt.Sprintf(`
grpc_client_jem:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHelloHowAreYou
  reflection: true
  rpc_type: server_stream
`, testServer.port)

	sendChan, receiveChan, err := startGrpcClientOutput(t, yamlConf)
	assert.NoError(t, err)

	inputs := []string{
		`{"name":"Alice"}`, `{"name":"Bob"}`, `{"name":"Carol"}`, `{"name":"Dan"}`,
	}

	for _, input := range inputs {
		testMsg := message.QuickBatch([][]byte{[]byte(input)})
		select {
		case sendChan <- message.NewTransaction(testMsg, receiveChan):
		case <-time.After(time.Minute):
			t.Fatal("Action timed out")
		}

		select {
		case res := <-receiveChan:
			assert.NoError(t, res)
		case <-time.After(time.Minute):
			t.Fatal("Action timed out")
		}
	}

	assert.Equal(t, 4, testServer.sayHelloHowAreYouInvocations)
}

func TestGrpcClientWriterServerStreamSyncResponse(t *testing.T) {
	testServer := startGRPCServer(t, withReflection())

	yamlConf := fmt.Sprintf(`
grpc_client_jem:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHelloHowAreYou
  reflection: true
  rpc_type: server_stream
  propagate_response: true
`, testServer.port)

	sendChan, receiveChan, err := startGrpcClientOutput(t, yamlConf)
	assert.NoError(t, err)

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
			t.Fatal("Action timed out")
		}

		select {
		case res := <-receiveChan:
			assert.NoError(t, res)
			resMsgs := resultStore.Get()
			assert.Equal(t, `{"message":"Hello `+names[i]+`"}`, string(resMsgs[0].Get(0).AsBytes()))
			assert.Equal(t, `{"message":"How are you, `+names[i]+`?"}`, string(resMsgs[0].Get(1).AsBytes()))
		case <-time.After(time.Minute):
			t.Fatal("Action timed out")
		}
	}
	assert.Equal(t, 4, testServer.sayHelloHowAreYouInvocations)
}

func TestGrpcClientWriterOAuthTLS(t *testing.T) {
	tsOAuth2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		username, password, ok := r.BasicAuth()
		assert.True(t, ok)
		assert.Equal(t, "fookey", username)
		assert.Equal(t, "foosecret", password)

		b, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		assert.Equal(t, "grant_type=client_credentials", string(b))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		resp := `{"access_token":"some-secret-token","token_type":"Bearer","expires_in":3600}`
		_, _ = w.Write([]byte(resp))
	}))
	defer tsOAuth2.Close()

	testServer := startGRPCServer(t, withReflection(), withTLS(), withOAuth2())

	yamlConf := fmt.Sprintf(`
grpc_client_jem:
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
`, testServer.port, tsOAuth2.URL)

	sendChan, receiveChan, err := startGrpcClientOutput(t, yamlConf)
	assert.NoError(t, err)

	inputs := []string{
		`{"name":"Alice"}`, `{"name":"Bob"}`, `{"name":"Carol"}`, `{"name":"Dan"}`,
	}

	for _, input := range inputs {
		testMsg := message.QuickBatch([][]byte{[]byte(input)})
		select {
		case sendChan <- message.NewTransaction(testMsg, receiveChan):
		case <-time.After(time.Minute):
			t.Fatal("Action timed out")
		}

		select {
		case res := <-receiveChan:
			assert.NoError(t, res)
		case <-time.After(time.Minute):
			t.Fatal("Action timed out")
		}
	}

	assert.Equal(t, 4, testServer.sayHelloInvocations)
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
