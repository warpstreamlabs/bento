package grpc_client

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
	testServer := startGRPCServer(t, withReflection())

	config := fmt.Sprintf(`
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  rpc_type: unary
  reflection: true
  payload: ${! {"name":"Jem"} }
`, testServer.port)

	msgCh := startGrpcClientInput(t, config)

	var msg message.Transaction
	select {
	case msg = <-msgCh:
	case <-time.After(time.Second * 10):
		t.FailNow()
	}

	assert.Equal(t, "{\"message\":\"Hello Jem\"}", string(msg.Payload.Get(0).AsBytes()))
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
    payload: ${! {"name":"Jem"} }
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
			assert.Equal(t, 1, len(msgReceived))
			assert.Equal(t, `{"message":"Hello Jem"}`, string(msgReceived[0]))
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
