//go:build x_bento_extra
// +build x_bento_extra

package zeromq

import (
	"testing"
	"time"

	"github.com/warpstreamlabs/bento/public/service/integration"
)

func TestIntegrationZMQ(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	template := `
output:
  zmq4:
    urls:
      - tcp://localhost:$PORT
    bind: false
    socket_type: $VAR1
    poll_timeout: 5s

input:
  zmq4:
    urls:
      - tcp://*:$PORT
    bind: true
    socket_type: $VAR2
    sub_filters: [ $VAR3 ]
`
	templateN2C := `
output:
  zmq4:
    urls:
      - tcp://localhost:$PORT
    bind: false
    socket_type: $VAR1
    poll_timeout: 5s

input:
  zmq4n:
    urls:
      - tcp://*:$PORT
    bind: true
    socket_type: $VAR2
    sub_filters: [ $VAR3 ]
`
	templateC2N := `
output:
  zmq4n:
    urls:
      - tcp://localhost:$PORT
    bind: false
    socket_type: $VAR1
    poll_timeout: 5s

input:
  zmq4:
    urls:
      - tcp://*:$PORT
    bind: true
    socket_type: $VAR2
    sub_filters: [ $VAR3 ]
`
	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		integration.StreamTestStreamParallel(100),
	)

	suite.Run(
		t, template,
		integration.StreamTestOptSleepAfterInput(500*time.Millisecond),
		integration.StreamTestOptSleepAfterOutput(500*time.Millisecond),
		integration.StreamTestOptVarSet("VAR1", "PUSH"),
		integration.StreamTestOptVarSet("VAR2", "PULL"),
		integration.StreamTestOptVarSet("VAR3", ""),
	)
	t.Run("with pub sub", func(t *testing.T) {
		t.Parallel()
		suite.Run(
			t, template,
			integration.StreamTestOptSleepAfterInput(500*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(500*time.Millisecond),
			integration.StreamTestOptVarSet("VAR1", "PUB"),
			integration.StreamTestOptVarSet("VAR2", "SUB"),
			integration.StreamTestOptVarSet("VAR3", "''"),
		)
	})

	t.Run("with push pull and zmq4n output", func(t *testing.T) {
		t.Parallel()
		suite.Run(
			t, templateC2N,
			integration.StreamTestOptSleepAfterInput(500*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(500*time.Millisecond),
			integration.StreamTestOptVarSet("VAR1", "PUSH"),
			integration.StreamTestOptVarSet("VAR2", "PULL"),
			integration.StreamTestOptVarSet("VAR3", ""),
		)
	})

	t.Run("with pub sub and zmq4n input", func(t *testing.T) {
		t.Parallel()
		suite.Run(
			t, templateN2C,
			integration.StreamTestOptSleepAfterInput(500*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(500*time.Millisecond),
			integration.StreamTestOptVarSet("VAR1", "PUSH"),
			integration.StreamTestOptVarSet("VAR2", "PULL"),
			integration.StreamTestOptVarSet("VAR3", ""),
		)
	})
}
