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
)

func TestGrpcClientInput(t *testing.T) {
	tests := map[string]struct {
		grpcServerOpts   []testServerOpt
		confFormatString string
		formatArgs       func(*testServer) []any
		expected         [][]byte
	}{
		"Unary Reflection": {
			grpcServerOpts: []testServerOpt{withReflection()},
			confFormatString: `
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  rpc_type: unary
  reflection: true
  payload: ${! {"name":"Alice"} }
`,
			formatArgs: func(ts *testServer) []any {
				return []any{ts.port}
			},
			expected: [][]byte{
				[]byte(`{"message":"Hello Alice"}`),
			},
		},
		"Unary Proto File": {
			grpcServerOpts: []testServerOpt{},
			confFormatString: `
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  rpc_type: unary
  proto_files: 
    - "./grpc_test_server/helloworld.proto"
  payload: ${! {"name":"Alice"} }
`,
			formatArgs: func(ts *testServer) []any {
				return []any{ts.port}
			},
			expected: [][]byte{
				[]byte(`{"message":"Hello Alice"}`),
			},
		},
		"Server Stream Reflection": {
			grpcServerOpts: []testServerOpt{withReflection()},
			confFormatString: `
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHelloHowAreYou
  rpc_type: server_stream
  reflection: true
  payload: ${! {"name":"Alice"} }
`,
			formatArgs: func(ts *testServer) []any {
				return []any{ts.port}
			},
			expected: [][]byte{
				[]byte(`{"message":"Hello Alice"}`),
				[]byte(`{"message":"How are you, Alice?"}`),
			},
		},
		"Server Stream Proto File": {
			grpcServerOpts: []testServerOpt{withReflection()},
			confFormatString: `
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHelloHowAreYou
  rpc_type: server_stream
  proto_files: 
    - "./grpc_test_server/helloworld.proto" 
  payload: ${! {"name":"Alice"} }
`,
			formatArgs: func(ts *testServer) []any {
				return []any{ts.port}
			},
			expected: [][]byte{
				[]byte(`{"message":"Hello Alice"}`),
				[]byte(`{"message":"How are you, Alice?"}`),
			},
		},
		"TLS": {
			grpcServerOpts: []testServerOpt{withReflection(), withTLS()},
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
    root_cas_file: ./grpc_test_server/certs/ca.pem
    client_certs:
      - cert_file: ./grpc_test_server/certs/client.pem
        key_file: ./grpc_test_server/certs/client.key
    skip_cert_verify: false
`,
			formatArgs: func(ts *testServer) []any {
				return []any{ts.port}
			},
			expected: [][]byte{
				[]byte(`{"message":"Hello Alice"}`),
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			testServer := startGRPCServer(t, test.grpcServerOpts...)

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

func TestGrpcClientInputRateLimit(t *testing.T) {
	testServer := startGRPCServer(t, withReflection())

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
`, testServer.port))
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

func startGrpcClientInput(t *testing.T, yamlConf string) (ch <-chan (message.Transaction)) {
	t.Helper()

	conf, err := testutil.InputFromYAML(yamlConf)
	require.NoError(t, err)

	s, err := mock.NewManager().NewInput(conf)
	require.NoError(t, err)

	t.Cleanup(s.TriggerCloseNow)

	return s.TransactionChan()
}
