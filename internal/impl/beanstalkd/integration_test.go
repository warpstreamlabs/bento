package beanstalkd

import (
	"testing"
	"time"

	"github.com/ory/dockertest/v4"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service/integration"
)

const template string = `
output:
  beanstalkd:
    address: localhost:$PORT
    max_in_flight: $MAX_IN_FLIGHT

input:
  beanstalkd:
    address: localhost:$PORT
`

func TestIntegrationBeanstalkdOpenClose(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool := dockertest.NewPoolT(t, "", dockertest.WithMaxWait(time.Second*30))

	resource := pool.RunT(t, "websmurf/beanstalkd",
		dockertest.WithTag("1.12-alpine-3.14"),
		dockertest.WithoutReuse(),
	)

	require.NoError(t, pool.Retry(t.Context(), 0, func() error {
		return nil
	}))

	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptPort(resource.GetPort("11300/tcp")),
	)
}

func TestIntegrationBeanstalkdSendBatch(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool := dockertest.NewPoolT(t, "", dockertest.WithMaxWait(time.Second*30))

	resource := pool.RunT(t, "websmurf/beanstalkd",
		dockertest.WithTag("1.12-alpine-3.14"),
		dockertest.WithoutReuse(),
	)

	require.NoError(t, pool.Retry(t.Context(), 0, func() error {
		return nil
	}))

	suite := integration.StreamTests(
		integration.StreamTestSendBatch(10),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptPort(resource.GetPort("11300/tcp")),
	)
}

func TestIntegrationBeanstalkdStreamSequential(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool := dockertest.NewPoolT(t, "", dockertest.WithMaxWait(time.Second*30))

	resource := pool.RunT(t, "websmurf/beanstalkd",
		dockertest.WithTag("1.12-alpine-3.14"),
		dockertest.WithoutReuse(),
	)

	require.NoError(t, pool.Retry(t.Context(), 0, func() error {
		return nil
	}))

	suite := integration.StreamTests(
		integration.StreamTestStreamSequential(100),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptPort(resource.GetPort("11300/tcp")),
	)
}

func TestIntegrationBeanstalkdStreamParallel(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool := dockertest.NewPoolT(t, "", dockertest.WithMaxWait(time.Second*30))

	resource := pool.RunT(t, "websmurf/beanstalkd",
		dockertest.WithTag("1.12-alpine-3.14"),
		dockertest.WithoutReuse(),
	)

	require.NoError(t, pool.Retry(t.Context(), 0, func() error {
		return nil
	}))

	suite := integration.StreamTests(
		integration.StreamTestStreamParallel(100),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptPort(resource.GetPort("11300/tcp")),
	)
}
