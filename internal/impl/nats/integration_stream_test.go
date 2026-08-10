package nats

import (
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/stan.go"
	"github.com/ory/dockertest/v4"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service/integration"
)

func TestIntegrationNatsStream(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool := dockertest.NewPoolT(t, "", dockertest.WithMaxWait(time.Second*30))

	resource := pool.RunT(t, "nats-streaming",
		dockertest.WithTag("latest"),
		dockertest.WithoutReuse(),
	)

	require.NoError(t, pool.Retry(t.Context(), 0, func() error {
		natsConn, err := stan.Connect(
			"test-cluster", "bento_test_client",
			stan.NatsURL(fmt.Sprintf("tcp://localhost:%v", resource.GetPort("4222/tcp"))),
		)
		if err != nil {
			return err
		}
		natsConn.Close()
		return nil
	}))

	template := `
output:
  nats_stream:
    urls: [ nats://localhost:$PORT ]
    cluster_id: test-cluster
    client_id: client-output-$ID
    subject: subject-$ID
    max_in_flight: $MAX_IN_FLIGHT

input:
  nats_stream:
    urls: [ nats://localhost:$PORT ]
    cluster_id: test-cluster
    client_id: client-input-$ID
    queue: queue-$ID
    subject: subject-$ID
    ack_wait: 5s
`
	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		// integration.StreamTestMetadata(), TODO
		integration.StreamTestSendBatch(10),
		integration.StreamTestStreamParallel(1000),
		integration.StreamTestStreamSequential(1000),
		integration.StreamTestStreamParallelLossy(1000),
		integration.StreamTestStreamParallelLossyThroughReconnect(1000),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
		integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
		integration.StreamTestOptPort(resource.GetPort("4222/tcp")),
	)
	t.Run("with max in flight", func(t *testing.T) {
		t.Parallel()
		suite.Run(
			t, template,
			integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
			integration.StreamTestOptPort(resource.GetPort("4222/tcp")),
			integration.StreamTestOptMaxInFlight(10),
		)
	})
}
