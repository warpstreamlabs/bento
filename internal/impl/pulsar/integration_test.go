package pulsar

import (
	"fmt"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service/integration"
)

func TestIntegrationPulsar(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Minute * 2
	if dline, ok := t.Deadline(); ok && time.Until(dline) < pool.MaxWait {
		pool.MaxWait = time.Until(dline)
	}

	runOpts := dockertest.RunOptions{
		Repository:   "apachepulsar/pulsar",
		Tag:          "latest",
		ExposedPorts: []string{"6650", "8080"},
		// Run a Pulsar cluster in standalone-mode https://pulsar.apache.org/docs/next/standalone-docker/
		Cmd: []string{"bin/pulsar", "standalone"},
	}

	resource, err := pool.RunWithOptions(&runOpts)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
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
