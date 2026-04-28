package grpc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/public/service"

	test_server "github.com/warpstreamlabs/bento/internal/impl/grpc/grpc_test_server/test_server"
)

func TestGrpcClientInput(t *testing.T) {
	tests := map[string]struct {
		grpcServerOpts   []test_server.TestServerOpt
		confFormatString string
		formatArgs       func(*test_server.TestServer) []any
		expected         [][]byte
	}{
		"Unary Reflection": {
			grpcServerOpts: []test_server.TestServerOpt{test_server.WithReflection()},
			confFormatString: `
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  rpc_type: unary
  reflection: true
  payload: ${! {"name":"Alice"} }
`,
			formatArgs: func(ts *test_server.TestServer) []any {
				return []any{ts.Port}
			},
			expected: [][]byte{
				[]byte(`{"message":"Hello Alice"}`),
			},
		},
		"Unary Proto File": {
			grpcServerOpts: []test_server.TestServerOpt{},
			confFormatString: `
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  rpc_type: unary
  proto_files: 
    - "./grpc_test_server/helloworld/helloworld.proto"
  payload: ${! {"name":"Alice"} }
`,
			formatArgs: func(ts *test_server.TestServer) []any {
				return []any{ts.Port}
			},
			expected: [][]byte{
				[]byte(`{"message":"Hello Alice"}`),
			},
		},
		"Server Stream Reflection": {
			grpcServerOpts: []test_server.TestServerOpt{test_server.WithReflection()},
			confFormatString: `
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHelloHowAreYou
  rpc_type: server_stream
  reflection: true
  payload: ${! {"name":"Alice"} }
`,
			formatArgs: func(ts *test_server.TestServer) []any {
				return []any{ts.Port}
			},
			expected: [][]byte{
				[]byte(`{"message":"Hello Alice"}`),
				[]byte(`{"message":"How are you, Alice?"}`),
			},
		},
		"Server Stream Proto File": {
			grpcServerOpts: []test_server.TestServerOpt{test_server.WithReflection()},
			confFormatString: `
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHelloHowAreYou
  rpc_type: server_stream
  proto_files: 
    - "./grpc_test_server/helloworld/helloworld.proto" 
  payload: ${! {"name":"Alice"} }
`,
			formatArgs: func(ts *test_server.TestServer) []any {
				return []any{ts.Port}
			},
			expected: [][]byte{
				[]byte(`{"message":"Hello Alice"}`),
				[]byte(`{"message":"How are you, Alice?"}`),
			},
		},
		"TLS": {
			grpcServerOpts: []test_server.TestServerOpt{test_server.WithReflection(), test_server.WithTLS()},
			confFormatString: `
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  rpc_type: unary
  reflection: true
  payload: ${! {"name":"Alice"} }
  tls:
    enabled: true
    root_cas_file: ./grpc_test_server/helloworld/certs/ca.pem
    client_certs:
      - cert_file: ./grpc_test_server/helloworld/certs/client.pem
        key_file: ./grpc_test_server/helloworld/certs/client.key
    skip_cert_verify: false
`,
			formatArgs: func(ts *test_server.TestServer) []any {
				return []any{ts.Port}
			},
			expected: [][]byte{
				[]byte(`{"message":"Hello Alice"}`),
			},
		},
		"TLS and oAuth2": {
			grpcServerOpts: []test_server.TestServerOpt{test_server.WithReflection(), test_server.WithTLS(), test_server.WithOAuth2()},
			confFormatString: `
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  rpc_type: unary
  reflection: true
  payload: ${! {"name":"Alice"} }
  oauth2:
    enabled: true
    token_url: %v
    client_key: fookey
    client_secret: foosecret
  tls:
    enabled: true
    root_cas_file: ./grpc_test_server/helloworld/certs/ca.pem
    client_certs:
      - cert_file: ./grpc_test_server/helloworld/certs/client.pem
        key_file: ./grpc_test_server/helloworld/certs/client.key
    skip_cert_verify: false
`,
			formatArgs: func(ts *test_server.TestServer) []any {
				return []any{ts.Port, ts.OAuthAddress}
			},
			expected: [][]byte{
				[]byte(`{"message":"Hello Alice"}`),
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			testServer := test_server.StartGRPCServer(t, test.grpcServerOpts...)

			yamlConf := fmt.Sprintf(test.confFormatString, test.formatArgs(testServer)...)

			msgCh := startGrpcClientInput(t, yamlConf)

			var msg message.Transaction
			select {
			case msg = <-msgCh:
			case <-time.After(time.Second * 10):
				t.FailNow()
			}

			err := msg.Payload.Iter(func(i int, p *message.Part) error {
				assert.Equal(t, test.expected[i], p.AsBytes())
				return nil
			})
			require.NoError(t, err)
		})
	}
}

func TestGrpcClientInputClientStream(t *testing.T) {
	testServer := test_server.StartGRPCServer(t, test_server.WithReflection())

	yamlConf := fmt.Sprintf(`grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayMultipleHellos
  rpc_type: client_stream
  reflection: true
  payload: ${! [{"name":"Alice"}, {"name":"Bob"}] }`, testServer.Port)

	msgCh := startGrpcClientInput(t, yamlConf)

	var msg message.Transaction
	select {
	case msg = <-msgCh:
	case <-time.After(10 * time.Second):
		t.FailNow()
	}

	expected := []byte(`{"message":"Hello Alice, Bob"}`)

	err := msg.Payload.Iter(func(i int, p *message.Part) error {
		assert.Equal(t, string(expected), string(p.AsBytes()))
		return nil
	})
	require.NoError(t, err)

}

func TestGrpcClientInputBidi(t *testing.T) {
	testServer := test_server.StartGRPCServer(t, test_server.WithReflection())

	yamlConf := fmt.Sprintf(`grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHelloBidi
  rpc_type: bidi
  reflection: true`, testServer.Port)

	msgCh := startGrpcClientInput(t, yamlConf)

	var msg message.Transaction
	select {
	case msg = <-msgCh:
	case <-time.After(10 * time.Second):
		t.FailNow()
	}

	expected := []byte(`{"message":"Hello"}`)

	err := msg.Payload.Iter(func(i int, p *message.Part) error {
		assert.Equal(t, string(expected), string(p.AsBytes()))
		return nil
	})
	require.NoError(t, err)
}

func TestGrpcClientInputRateLimit(t *testing.T) {
	testServer := test_server.StartGRPCServer(t, test_server.WithReflection())

	sb := service.NewStreamBuilder()

	err := sb.SetYAML(fmt.Sprintf(`
input:
  grpc_client:
    address: localhost:%v
    service: helloworld.Greeter
    method: SayHello
    rpc_type: unary
    reflection: true
    payload: ${! {"name":"Alice"} }
    rate_limit: basic

rate_limit_resources:
  - label: basic
    local:
      count: 1
      interval: 24h
`, testServer.Port))
	require.NoError(t, err)

	ch := make(chan []byte)

	err = sb.AddConsumerFunc(func(c context.Context, m *service.Message) error {
		msgBytes, err := m.AsBytes()
		if err != nil {
			return err
		}

		ch <- msgBytes
		return err
	})
	require.NoError(t, err)

	stream, err := sb.Build()
	require.NoError(t, err)

	go func() {
		err = stream.Run(context.Background())
		require.NoError(t, err)
	}()

	var msgReceived [][]byte
	for {
		select {
		case <-time.After(time.Second * 2):
			assert.Len(t, msgReceived, 1)
			assert.Equal(t, `{"message":"Hello Alice"}`, string(msgReceived[0]))
			return
		case msg := <-ch:
			msgReceived = append(msgReceived, msg)
		}
	}
}

func TestGrpcClientInputHealthCheck(t *testing.T) {
	testServer := test_server.StartGRPCServer(t, test_server.WithReflection(), test_server.WithHealthCheck())

	yamlConf := fmt.Sprintf(`
address: localhost:%v
service: helloworld.Greeter
method: SayHello
reflection: true
payload: ${! {"name":"Alice"} }
health_check:
  enabled: true
  service: ""
`, testServer.Port)

	pConf, err := grpcClientInputSpec().ParseYAML(yamlConf, nil)
	require.NoError(t, err)

	input, err := newGrpcClientInputFromParsed(pConf, service.MockResources())
	require.NoError(t, err)

	ctx := context.Background()
	err = input.Connect(ctx)
	assert.NoError(t, err)
}

func startGrpcClientInput(t *testing.T, yamlConf string) (ch <-chan (message.Transaction)) {
	t.Helper()

	conf, err := testutil.InputFromYAML(yamlConf)
	require.NoError(t, err)

	s, err := mock.NewManager().NewInput(conf)
	require.NoError(t, err)

	t.Cleanup(s.TriggerCloseNow)

	return s.TransactionChan()
}

func TestGrpcClientInputLints(t *testing.T) {

	tests := map[string]struct {
		config          string
		expErrMessage   string
		noErrorExpected bool
	}{
		"Reflection or Proto Files Required": {
			config: `
grpc_client:
  address: localhost:55001
  service: helloworld.Greeter
  method: Foo
  reflection: false
  proto_files: []
  rpc_type: unary
  payload: |
      {"foo":"bar"}
`,
			expErrMessage: "reflection must be true or proto_files must be populated",
		},
		"Reflection or Proto Files Required Default": {
			config: `
grpc_client:
  address: localhost:55001
  service: helloworld.Greeter
  method: Foo
  rpc_type: unary
  payload: |
      {"foo":"bar"}
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
  rpc_type: unary
  payload: |
      {"foo":"bar"}
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
  rpc_type: unary
  payload: |
      {"foo":"bar"}
`,
			noErrorExpected: true,
		},
		"Payload required for unary": {
			config: `
grpc_client:
  address: localhost:55001
  service: helloworld.Greeter
  rpc_type: unary
  method: Foo
  reflection: true
`,
			expErrMessage: "payload must be set for rpc_types: unary, client_stream, server_stream",
		},
		"Payload required for server_stream": {
			config: `
grpc_client:
  address: localhost:55001
  service: helloworld.Greeter
  rpc_type: server_stream
  method: Foo
  reflection: true
`,
			expErrMessage: "payload must be set for rpc_types: unary, client_stream, server_stream",
		},
		"Payload required for client_stream": {
			config: `
grpc_client:
  address: localhost:55001
  service: helloworld.Greeter
  rpc_type: client_stream
  method: Foo
  reflection: true
`,
			expErrMessage: "payload must be set for rpc_types: unary, client_stream, server_stream",
		},
		"Payload not required for bidi": {
			config: `
grpc_client:
  address: localhost:55001
  service: helloworld.Greeter
  rpc_type: bidi
  method: Foo
  reflection: true
`,
			noErrorExpected: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := service.NewStreamBuilder().AddInputYAML(test.config)
			if test.noErrorExpected {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, test.expErrMessage)
			}
		})
	}
}
