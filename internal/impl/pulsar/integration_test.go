package pulsar

import (
	"fmt"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ory/dockertest/v4"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service/integration"
)

func TestIntegrationPulsar(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool := dockertest.NewPoolT(t, "", dockertest.WithMaxWait(time.Minute))

	if dline, ok := t.Deadline(); ok && time.Until(dline) < time.Minute {

	}

	// The image already EXPOSEs 6650 and 8080, so PublishAllPorts covers them.
	resource := pool.RunT(t, "apachepulsar/pulsar",
		dockertest.WithTag("latest"),
		// Run a Pulsar cluster in standalone-mode https://pulsar.apache.org/docs/next/standalone-docker/
		dockertest.WithCmd([]string{"bin/pulsar", "standalone"}),
		dockertest.WithoutReuse(),
	)

	require.NoError(t, pool.Retry(t.Context(), 0, func() error {
		client, err := pulsar.NewClient(pulsar.ClientOptions{
			URL:    fmt.Sprintf("pulsar://localhost:%v/", resource.GetPort("6650/tcp")),
			Logger: NoopLogger(),
		})
		if err != nil {
			return err
		}
		prod, err := client.CreateProducer(pulsar.ProducerOptions{
			Topic: "bento-connection-test",
		})
		if err == nil {
			prod.Close()
		}
		client.Close()
		return err
	}))

	template := `
output:
  pulsar:
    url: pulsar://localhost:$PORT/
    topic: "topic-$ID"
    max_in_flight: $MAX_IN_FLIGHT

input:
  pulsar:
    url: pulsar://localhost:$PORT/
    topics: [ "topic-$ID" ]
    subscription_name: "sub-$ID"
`

	patternTemplate := `
output:
  pulsar:
    url: pulsar://localhost:$PORT/
    topic: "topic-$ID"
    max_in_flight: $MAX_IN_FLIGHT

input:
  pulsar:
    url: pulsar://localhost:$PORT/
    topics_pattern: "t.*c-$ID"
    subscription_name: "sub-$ID"
`
	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		integration.StreamTestSendBatch(10),
		integration.StreamTestStreamSequential(1000),
		integration.StreamTestStreamParallel(1000),
		integration.StreamTestStreamParallelLossy(1000),
		integration.StreamTestStreamParallelLossyThroughReconnect(1000),
		// integration.StreamTestAtLeastOnceDelivery(), // TODO: uncomment when race condition fixed
	)

	suite.Run(
		t, template,
		integration.StreamTestOptSleepAfterInput(500*time.Millisecond),
		integration.StreamTestOptSleepAfterOutput(500*time.Millisecond),
		integration.StreamTestOptPort(resource.GetPort("6650/tcp")),
	)

	t.Run("with topics pattern", func(t *testing.T) {
		suite.Run(
			t, patternTemplate,
			integration.StreamTestOptSleepAfterInput(500*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(500*time.Millisecond),
			integration.StreamTestOptPort(resource.GetPort("6650/tcp")),
		)
	})

	t.Run("with max in flight", func(t *testing.T) {
		t.Parallel()
		suite.Run(
			t, template,
			integration.StreamTestOptSleepAfterInput(500*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(500*time.Millisecond),
			integration.StreamTestOptPort(resource.GetPort("6650/tcp")),
			integration.StreamTestOptMaxInFlight(10),
		)
	})
}
