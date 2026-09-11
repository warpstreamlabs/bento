package gcp

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsub" //nolint:staticcheck
	dockercontainer "github.com/moby/moby/api/types/container"
	dockernetwork "github.com/moby/moby/api/types/network"
	"github.com/ory/dockertest/v4"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service/integration"
)

func TestIntegrationGCPPubSub(t *testing.T) {
	integration.CheckSkip(t)

	pool := dockertest.NewPoolT(t, "", dockertest.WithMaxWait(time.Minute))

	resource := pool.RunT(t, "gcr.io/google.com/cloudsdktool/google-cloud-cli",
		dockertest.WithTag("emulators"),
		dockertest.WithContainerConfig(func(c *dockercontainer.Config) {
			c.ExposedPorts = dockernetwork.PortSet{
				dockernetwork.MustParsePort("8432/tcp"): {},
			}
		}),
		dockertest.WithCmd([]string{
			"gcloud", "beta", "emulators", "pubsub", "start", "--project=bento-test-project", "--host-port=0.0.0.0:8432",
		}),
		dockertest.WithoutReuse(),
	)

	t.Setenv("PUBSUB_EMULATOR_HOST", fmt.Sprintf("localhost:%v", resource.GetPort("8432/tcp")))
	require.NotEqual(t, "localhost:", os.Getenv("PUBSUB_EMULATOR_HOST"))

	require.NoError(t, pool.Retry(t.Context(), 0, func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		client, err := pubsub.NewClient(ctx, "bento-test-project")
		if err != nil {
			return err
		}
		_, err = client.CreateTopic(ctx, "test-probe-topic-name")
		client.Close()
		return err
	}))

	template := `
output:
  gcp_pubsub:
    project: bento-test-project
    topic: topic-$ID
    max_in_flight: $MAX_IN_FLIGHT
    metadata:
      exclude_prefixes: [ $OUTPUT_META_EXCLUDE_PREFIX ]

input:
  gcp_pubsub:
    project: bento-test-project
    subscription: sub-$ID
    create_subscription:
      enabled: true
      topic: topic-$ID
`
	suiteOpts := []integration.StreamTestOptFunc{
		integration.StreamTestOptSleepAfterInput(100 * time.Millisecond),
		integration.StreamTestOptSleepAfterOutput(100 * time.Millisecond),
		integration.StreamTestOptTimeout(time.Minute * 5),
		integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
			client, err := pubsub.NewClient(ctx, "bento-test-project")
			require.NoError(t, err)

			_, err = client.CreateTopic(ctx, fmt.Sprintf("topic-%v", vars.ID))
			require.NoError(t, err)

			client.Close()
		}),
	}
	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		integration.StreamTestMetadata(),
		integration.StreamTestMetadataFilter(),
		integration.StreamTestSendBatches(10, 1000, 10),
		integration.StreamTestStreamSequential(1000),
		integration.StreamTestStreamParallel(1000),
		integration.StreamTestStreamParallelLossy(1000),
		// integration.StreamTestAtLeastOnceDelivery(),
	)
	suite.Run(t, template, suiteOpts...)
	t.Run("with max in flight", func(t *testing.T) {
		t.Parallel()
		suite.Run(
			t, template,
			append([]integration.StreamTestOptFunc{integration.StreamTestOptMaxInFlight(10)}, suiteOpts...)...,
		)
	})
}
