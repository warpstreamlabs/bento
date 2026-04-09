package grpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/transaction"

	_ "github.com/warpstreamlabs/bento/public/components/io"
	_ "github.com/warpstreamlabs/bento/public/components/pure"
	"github.com/warpstreamlabs/bento/public/service"

	test_server "github.com/warpstreamlabs/bento/internal/impl/grpc/grpc_test_server/test_server"
)

//------------------------------------------------------------------------------

var namesInputTestData = []string{`{"name":"Alice"}`, `{"name":"Bob"}`, `{"name":"Carol"}`, `{"name":"Dan"}`}
var names = []string{"Alice", "Bob", "Carol", "Dan"}

func TestGrpcClientOutput(t *testing.T) {
	tests := map[string]struct {
		grpcServerOpts   []test_server.TestServerOpt
		confFormatString string
		formatArgs       func(*test_server.TestServer) []any
		invocations      func(*test_server.TestServer) int
		expInvocations   int
	}{
		"Unary Reflection": {
			grpcServerOpts: []test_server.TestServerOpt{test_server.WithReflection()},
			confFormatString: `
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  reflection: true
`,
			formatArgs: func(ts *test_server.TestServer) []any {
				return []any{ts.Port}
			},
			invocations: func(ts *test_server.TestServer) int {
				ts.Mu.Lock()
				defer ts.Mu.Unlock()
				return ts.SayHelloInvocations
			},
			expInvocations: 4,
		},
		"Unary Proto File": {
			grpcServerOpts: []test_server.TestServerOpt{},
			confFormatString: `
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  proto_files: 
    - "./grpc_test_server/helloworld/helloworld.proto"
`,
			formatArgs: func(ts *test_server.TestServer) []any {
				return []any{ts.Port}
			},
			invocations: func(ts *test_server.TestServer) int {
				ts.Mu.Lock()
				defer ts.Mu.Unlock()
				return ts.SayHelloInvocations
			},
			expInvocations: 4,
		},
		"Client Stream Reflection": {
			grpcServerOpts: []test_server.TestServerOpt{test_server.WithReflection()},
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
			formatArgs: func(ts *test_server.TestServer) []any {
				return []any{ts.Port}
			},
			invocations: func(ts *test_server.TestServer) int {
				ts.Mu.Lock()
				defer ts.Mu.Unlock()
				return ts.SayMultiHellosInvocations
			},
			expInvocations: 1,
		},
		"Server Stream Reflection": {
			grpcServerOpts: []test_server.TestServerOpt{test_server.WithReflection()},
			confFormatString: `
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHelloHowAreYou
  reflection: true
  rpc_type: server_stream
`,
			formatArgs: func(ts *test_server.TestServer) []any {
				return []any{ts.Port}
			},
			invocations: func(ts *test_server.TestServer) int {
				ts.Mu.Lock()
				defer ts.Mu.Unlock()
				return ts.SayHelloHowAreYouInvocations
			},
			expInvocations: 4,
		},
		"Bidirectional Reflection": {
			grpcServerOpts: []test_server.TestServerOpt{test_server.WithReflection()},
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
			formatArgs: func(ts *test_server.TestServer) []any {
				return []any{ts.Port}
			},
			invocations: func(ts *test_server.TestServer) int {
				ts.Mu.Lock()
				defer ts.Mu.Unlock()
				return ts.SayHelloBidiInvocations
			},
			expInvocations: 1,
		},
		"TLS": {
			grpcServerOpts: []test_server.TestServerOpt{test_server.WithReflection(), test_server.WithTLS()},
			confFormatString: `
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  reflection: true
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
			invocations: func(ts *test_server.TestServer) int {
				ts.Mu.Lock()
				defer ts.Mu.Unlock()
				return ts.SayHelloInvocations
			},
			expInvocations: 4,
		},
		"TLS and oAuth2": {
			grpcServerOpts: []test_server.TestServerOpt{test_server.WithReflection(), test_server.WithTLS(), test_server.WithOAuth2()},
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
    root_cas_file: ./grpc_test_server/helloworld/certs/ca.pem
    client_certs:
      - cert_file: ./grpc_test_server/helloworld/certs/client.pem
        key_file: ./grpc_test_server/helloworld/certs/client.key
    skip_cert_verify: false
`,
			formatArgs: func(ts *test_server.TestServer) []any {
				return []any{ts.Port, ts.OAuthAddress}
			},
			invocations: func(ts *test_server.TestServer) int {
				ts.Mu.Lock()
				defer ts.Mu.Unlock()
				return ts.SayHelloInvocations
			},
			expInvocations: 4,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			testServer := test_server.StartGRPCServer(t, test.grpcServerOpts...)

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

func TestGrpcClientOutputSyncResponse(t *testing.T) {
	tests := map[string]struct {
		grpcServerOpts   []test_server.TestServerOpt
		confFormatString string
		invocations      func(*test_server.TestServer) int
		expInvocations   int
		inputs           []string
		waitForBatch     bool
	}{
		"Unary Sync Response": {
			grpcServerOpts: []test_server.TestServerOpt{test_server.WithReflection()},
			confFormatString: `
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  propagate_response: true
  reflection: true
`,
			invocations: func(ts *test_server.TestServer) int {
				ts.Mu.Lock()
				defer ts.Mu.Unlock()
				return ts.SayHelloInvocations
			},
			expInvocations: 4,
			inputs:         namesInputTestData,
		},
		"Unary Sync Response Protofile": {
			grpcServerOpts: []test_server.TestServerOpt{},
			confFormatString: `
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  propagate_response: true
  proto_files:
    - "./grpc_test_server/helloworld/helloworld.proto"
`,
			invocations: func(ts *test_server.TestServer) int {
				ts.Mu.Lock()
				defer ts.Mu.Unlock()
				return ts.SayHelloInvocations
			},
			expInvocations: 4,
			inputs:         namesInputTestData,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			testServer := test_server.StartGRPCServer(t, test.grpcServerOpts...)

			yamlConf := fmt.Sprintf(test.confFormatString, testServer.Port)

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

func TestGrpcClientOutputClientStreamSyncResponse(t *testing.T) {
	testServer := test_server.StartGRPCServer(t, test_server.WithReflection())

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
`, testServer.Port)

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

func TestGrpcClientOutputServerStreamSyncResponse(t *testing.T) {
	testServer := test_server.StartGRPCServer(t, test_server.WithReflection())

	yamlConf := fmt.Sprintf(`
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHelloHowAreYou
  reflection: true
  rpc_type: server_stream
  propagate_response: true
`, testServer.Port)

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

func TestGrpcClientOutputHealthCheck(t *testing.T) {
	testServer := test_server.StartGRPCServer(t, test_server.WithReflection(), test_server.WithHealthCheck())

	yamlConf := fmt.Sprintf(`
address: localhost:%v
service: helloworld.Greeter
method: SayHello
reflection: true
health_check:
  enabled: true
  service: ""
`, testServer.Port)

	pConf, err := grpcClientOutputSpec().ParseYAML(yamlConf, nil)
	require.NoError(t, err)

	output, err := newGrpcClientOutputFromParsed(pConf, nil)
	require.NoError(t, err)

	ctx := context.Background()
	err = output.Connect(ctx)
	assert.NoError(t, err)
}

func TestGrpcClientOutputUnableToFindMethodErr(t *testing.T) {
	testServer := test_server.StartGRPCServer(t, test_server.WithReflection())

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
`, testServer.Port))
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

func TestGrpcClientOutputBrokenProtoFile(t *testing.T) {
	testServer := test_server.StartGRPCServer(t, test_server.WithReflection())

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
      - "./grpc_test_server/helloworld/fail_parse.proto"
`, testServer.Port))
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

func TestGrpcClientOutputLints(t *testing.T) {

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

func TestGrpcClientOutputBatchErrors(t *testing.T) {
	tests := map[string]struct {
		grpcServerOpts   []test_server.TestServerOpt
		confFormatString string
		formatArgs       func(*test_server.TestServer) []any
		invocations      func(*test_server.TestServer) int
	}{
		"Unary Reflection Errors": {
			grpcServerOpts: []test_server.TestServerOpt{test_server.WithReflection(), test_server.WithReturnErrors()},
			confFormatString: `
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  reflection: true
`,
			formatArgs: func(ts *test_server.TestServer) []any {
				return []any{ts.Port}
			},
			invocations: func(ts *test_server.TestServer) int {
				ts.Mu.Lock()
				defer ts.Mu.Unlock()
				return ts.SayHelloInvocations
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			testServer := test_server.StartGRPCServer(t, test.grpcServerOpts...)

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

func TestGrpcClientWriterMetadataUnary(t *testing.T) {
	ts := test_server.StartGRPCServer(t, test_server.WithReflection())

	yamlConf := fmt.Sprintf(`
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  reflection: true
  metadata:
    x-custom-key: custom-value
    x-request-name: fixed-name
`, ts.Port)

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
		return ts.SayHelloInvocations == len(namesInputTestData)
	}, time.Second*10, time.Millisecond*20)

	received := ts.GetReceivedMetadata()
	require.Len(t, received, len(namesInputTestData))
	for _, md := range received {
		assert.Equal(t, []string{"custom-value"}, md.Get("x-custom-key"))
		assert.Equal(t, []string{"fixed-name"}, md.Get("x-request-name"))
	}
}

func TestGrpcClientWriterMetadataClientStream(t *testing.T) {
	ts := test_server.StartGRPCServer(t, test_server.WithReflection())

	yamlConf := fmt.Sprintf(`
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayMultipleHellos
  reflection: true
  rpc_type: client_stream
  metadata:
    x-stream-id: my-stream
  batching:
    count: 4
`, ts.Port)

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
		ts.Mu.Lock()
		defer ts.Mu.Unlock()
		return ts.SayMultiHellosInvocations == 1
	}, time.Second*10, time.Millisecond*20)

	received := ts.GetReceivedMetadata()
	require.Len(t, received, 1)
	assert.Equal(t, []string{"my-stream"}, received[0].Get("x-stream-id"))
}

func TestGrpcClientWriterMetadataServerStream(t *testing.T) {
	ts := test_server.StartGRPCServer(t, test_server.WithReflection())

	yamlConf := fmt.Sprintf(`
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHelloHowAreYou
  reflection: true
  rpc_type: server_stream
  metadata:
    x-server-stream: "true"
`, ts.Port)

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
		ts.Mu.Lock()
		defer ts.Mu.Unlock()
		return ts.SayHelloHowAreYouInvocations == len(namesInputTestData)
	}, time.Second*10, time.Millisecond*20)

	received := ts.GetReceivedMetadata()
	require.Len(t, received, len(namesInputTestData))
	for _, md := range received {
		assert.Equal(t, []string{"true"}, md.Get("x-server-stream"))
	}
}

func TestGrpcClientWriterMetadataBidi(t *testing.T) {
	ts := test_server.StartGRPCServer(t, test_server.WithReflection())

	yamlConf := fmt.Sprintf(`
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHelloBidi
  reflection: true
  rpc_type: bidi
  metadata:
    x-bidi-key: bidi-value
  batching:
    count: 4
`, ts.Port)

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
		ts.Mu.Lock()
		defer ts.Mu.Unlock()
		return ts.SayHelloBidiInvocations == 1
	}, time.Second*10, time.Millisecond*20)

	received := ts.GetReceivedMetadata()
	require.Len(t, received, 1)
	assert.Equal(t, []string{"bidi-value"}, received[0].Get("x-bidi-key"))
}

func TestGrpcClientWriterMetadataInterpolation(t *testing.T) {
	ts := test_server.StartGRPCServer(t, test_server.WithReflection())

	yamlConf := fmt.Sprintf(`
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  reflection: true
  metadata:
    x-name: ${! json("name") }
`, ts.Port)

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
		return ts.SayHelloInvocations == len(namesInputTestData)
	}, time.Second*10, time.Millisecond*20)

	received := ts.GetReceivedMetadata()
	require.Len(t, received, len(namesInputTestData))

	gotNames := make([]string, 0, len(received))
	for _, md := range received {
		vals := md.Get("x-name")
		require.Len(t, vals, 1)
		gotNames = append(gotNames, vals[0])
	}
	assert.ElementsMatch(t, names, gotNames)
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
