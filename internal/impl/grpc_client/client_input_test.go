package grpc_client

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
)

func TestGrpcClientInput(t *testing.T) {
	// arrange
	testServer := startGRPCServer(t, withReflection())

	config := fmt.Sprintf(`
grpc_client:
  address: localhost:%v
  service: helloworld.Greeter
  method: SayHello
  rpc_type: unary
  reflection: true
  payload: "Jem"
`, testServer.port)

	msgCh := startGrpcClientInput(t, config)

	// act
	var msg message.Transaction
	select {
	case msg = <-msgCh:
	case <-time.After(time.Second * 10):
		t.FailNow()
	}

	// assert
	assert.Equal(t, "{\"message\":\"Hello Jem\"}", string(msg.Payload.Get(0).AsBytes()))
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
