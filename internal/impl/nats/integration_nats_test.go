package nats

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"

	_ "github.com/warpstreamlabs/bento/public/components/io"
	_ "github.com/warpstreamlabs/bento/public/components/pure"
)

func startNatsContainer(t *testing.T) *dockertest.Resource {
	t.Helper()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.Run("nats", "latest", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		natsConn, err := nats.Connect(fmt.Sprintf("tcp://localhost:%v", resource.GetPort("4222/tcp")))
		if err != nil {
			return err
		}
		natsConn.Close()
		return nil
	}))

	return resource
}

func TestIntegrationNats(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	resource := startNatsContainer(t)

	template := `
output:
  nats:
    urls: [ tcp://localhost:$PORT ]
    subject: subject-$ID
    max_in_flight: $MAX_IN_FLIGHT

input:
  nats:
    urls: [ tcp://localhost:$PORT ]
    queue: queue-$ID
    subject: subject-$ID
    prefetch_count: 1048
`
	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		// integration.StreamTestMetadata(), TODO
		integration.StreamTestSendBatch(10),
		integration.StreamTestStreamParallel(500),
		integration.StreamTestStreamParallelLossy(500),
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

func TestIntegrationNatsSyncResponses(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	resource := startNatsContainer(t)

	sb := service.NewStreamBuilder()

	err := sb.SetYAML(fmt.Sprintf(`
input:
  nats:
    urls: [ nats://localhost:%v ]
    subject: foo.*

output:
  sync_response: {}
  processors:
    - mapping: 'root = content().uppercase()'
`, resource.GetPort("4222/tcp")))
	require.NoError(t, err)

	stream, err := sb.Build()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	go func() {
		if err := stream.Run(ctx); err != nil &&
			!errors.Is(err, context.DeadlineExceeded) &&
			!errors.Is(err, context.Canceled) {
			t.Errorf("stream exited: %v", err)
		}
	}()

	// send some messages into NATS with nats.Request
	nc, err := nats.Connect(fmt.Sprintf("nats://localhost:%v", resource.GetPort("4222/tcp")))
	require.NoError(t, err)

	defer func() {
		require.NoError(t, nc.Drain())
	}()

	require.Eventually(t, func() bool {
		_, err := nc.Request("foo._ready", []byte("ping"), 100*time.Millisecond)
		return err == nil
	}, 5*time.Second, 100*time.Millisecond, "stream failed to become ready")

	rep, err := nc.Request("foo.joe", []byte("joe"), time.Second)
	require.NoError(t, err)

	assert.Equal(t, []byte("JOE"), rep.Data)

	rep, err = nc.Request("foo.sue", []byte("sue"), time.Second)
	require.NoError(t, err)

	assert.Equal(t, []byte("SUE"), rep.Data)

	rep, err = nc.Request("foo.bob", []byte("bob"), time.Second)
	require.NoError(t, err)

	assert.Equal(t, []byte("BOB"), rep.Data)
}
