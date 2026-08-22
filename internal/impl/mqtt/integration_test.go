package mqtt

import (
	"fmt"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/ory/dockertest/v4"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service/integration"
)

func TestIntegrationMQTT(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool := dockertest.NewPoolT(t, "", dockertest.WithMaxWait(time.Second*30))

	resource := pool.RunT(t, "ncarlier/mqtt",
		dockertest.WithTag("latest"),
		dockertest.WithoutReuse(),
	)

	require.NoError(t, pool.Retry(t.Context(), 0, func() error {
		inConf := mqtt.NewClientOptions().SetClientID("UNIT_TEST")
		inConf = inConf.AddBroker(fmt.Sprintf("tcp://localhost:%v", resource.GetPort("1883/tcp")))

		mIn := mqtt.NewClient(inConf)
		tok := mIn.Connect()
		tok.Wait()
		if cErr := tok.Error(); cErr != nil {
			return cErr
		}
		mIn.Disconnect(0)
		return nil
	}))

	template := `
output:
  mqtt:
    urls: [ tcp://localhost:$PORT ]
    qos: 1
    topic: topic-$ID
    client_id: client-output-$ID

input:
  mqtt:
    urls: [ tcp://localhost:$PORT ]
    topics: [ topic-$ID ]
    client_id: client-input-$ID
    clean_session: false
`
	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		// integration.StreamTestMetadata(), TODO
		integration.StreamTestSendBatch(10),
		integration.StreamTestStreamParallel(1000),
		// integration.StreamTestStreamParallelLossy(1000),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
		integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
		integration.StreamTestOptPort(resource.GetPort("1883/tcp")),
	)

	t.Run("with max in flight", func(t *testing.T) {
		t.Parallel()
		template := `
output:
  mqtt:
    urls: [ tcp://localhost:$PORT ]
    qos: 1
    topic: topic-$ID
    client_id: client-output-$ID
    max_in_flight: $MAX_IN_FLIGHT

input:
  mqtt:
    urls: [ tcp://localhost:$PORT ]
    topics: [ topic-$ID ]
    client_id: client-input-$ID
    clean_session: false
`
		suite.Run(
			t, template,
			integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
			integration.StreamTestOptPort(resource.GetPort("1883/tcp")),
			integration.StreamTestOptMaxInFlight(10),
		)
	})
	t.Run("with generated suffix", func(t *testing.T) {
		t.Parallel()

		template := `
output:
  mqtt:
    urls: [ tcp://localhost:$PORT ]
    qos: 1
    topic: topic-$ID
    client_id: client-output-$ID
    dynamic_client_id_suffix: "$VAR1"
    max_in_flight: $MAX_IN_FLIGHT

input:
  mqtt:
    urls: [ tcp://localhost:$PORT ]
    topics: [ topic-$ID ]
    dynamic_client_id_suffix: "$VAR1"
    client_id: client-input-$ID
    clean_session: false
`
		suite.Run(
			t, template,
			integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
			integration.StreamTestOptPort(resource.GetPort("1883/tcp")),
			integration.StreamTestOptMaxInFlight(10),
			integration.StreamTestOptVarSet("VAR1", "nanoid"),
		)
	})
}
