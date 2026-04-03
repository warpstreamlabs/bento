package test_server

import (
	context "context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	sync "sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	test_server "github.com/warpstreamlabs/bento/internal/impl/grpc/grpc_test_server/helloworld"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	status "google.golang.org/grpc/status"
)

type TestServer struct {
	test_server.UnimplementedGreeterServer

	reflection   bool
	tls          bool
	oauth2       bool
	healthCheck  bool
	returnErrors bool

	OAuthAddress string

	Mu                           sync.Mutex
	SayHelloInvocations          int
	SayMultiHellosInvocations    int
	SayHelloHowAreYouInvocations int
	SayHelloBidiInvocations      int
	receivedMetadata             []metadata.MD
	Port                         int
}

func StartGRPCServer(t *testing.T, opts ...TestServerOpt) *TestServer {
	t.Helper()

	testServer := &TestServer{}

	for _, o := range opts {
		o(testServer)
	}

	lis, err := net.Listen("tcp", "localhost:0")
	assert.NoError(t, err)

	testServer.Port = lis.Addr().(*net.TCPAddr).Port

	serverOpts := []grpc.ServerOption{}

	if testServer.tls {
		serverCert, err := tls.LoadX509KeyPair(
			"./grpc_test_server/helloworld/certs/server.pem",
			"./grpc_test_server/helloworld/certs/server.key",
		)
		if err != nil {
			t.Fatal(err)
		}

		caCert, err := os.ReadFile("./grpc_test_server/helloworld/certs/ca.pem")
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

		testServer.OAuthAddress = tsOAuth2.URL
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

func (s *TestServer) SayHello(ctx context.Context, in *test_server.HelloRequest) (*test_server.HelloReply, error) {
	s.Mu.Lock()
	s.SayHelloInvocations++
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.receivedMetadata = append(s.receivedMetadata, md)
	}
	s.Mu.Unlock()
	if s.returnErrors {
		return nil, errors.New("ERROR :( ")
	}
	return &test_server.HelloReply{Message: "Hello " + in.GetName()}, nil
}

func (s *TestServer) SayMultipleHellos(stream test_server.Greeter_SayMultipleHellosServer) error {
	s.Mu.Lock()
	s.SayMultiHellosInvocations++
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		s.receivedMetadata = append(s.receivedMetadata, md)
	}
	s.Mu.Unlock()
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

func (s *TestServer) SayHelloHowAreYou(in *test_server.HelloRequest, stream test_server.Greeter_SayHelloHowAreYouServer) error {
	s.Mu.Lock()
	s.SayHelloHowAreYouInvocations++
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		s.receivedMetadata = append(s.receivedMetadata, md)
	}
	s.Mu.Unlock()

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

func (s *TestServer) SayHelloBidi(stream grpc.BidiStreamingServer[test_server.HelloRequest, test_server.HelloReply]) error {
	s.Mu.Lock()
	s.SayHelloBidiInvocations++
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		s.receivedMetadata = append(s.receivedMetadata, md)
	}
	s.Mu.Unlock()

	if s.returnErrors {
		return errors.New("ERROR :( ")
	}

	return nil
}

func (s *TestServer) GetReceivedMetadata() []metadata.MD {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	cp := make([]metadata.MD, len(s.receivedMetadata))
	copy(cp, s.receivedMetadata)
	return cp
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

type TestServerOpt func(*TestServer)

func WithReflection() TestServerOpt {
	return func(ts *TestServer) {
		ts.reflection = true
	}
}

func WithTLS() TestServerOpt {
	return func(ts *TestServer) {
		ts.tls = true
	}
}

func WithHealthCheck() TestServerOpt {
	return func(ts *TestServer) {
		ts.healthCheck = true
	}
}

func WithOAuth2() TestServerOpt {
	return func(ts *TestServer) {
		ts.oauth2 = true
	}
}

func WithReturnErrors() TestServerOpt {
	return func(ts *TestServer) {
		ts.returnErrors = true
	}
}
