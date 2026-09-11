package gcp

import (
	"context"
	"fmt"
	"testing"
	"time"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/google/uuid"
	dockercontainer "github.com/moby/moby/api/types/container"
	dockernetwork "github.com/moby/moby/api/types/network"
	"github.com/ory/dockertest/v4"
	"github.com/stretchr/testify/require"

	. "github.com/warpstreamlabs/bento/internal/impl/gcp/tests"
	_ "github.com/warpstreamlabs/bento/public/components/sql"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

const (
	projectID  = "test-project"
	instanceID = "test-instance"
	databaseID = "test-database"
)

func TestIntegrationSpannerCDCTest(t *testing.T) {
	integration.CheckSkip(t)

	ctx := context.Background()

	maxWait := time.Minute
	if deadline, ok := t.Deadline(); ok {
		maxWait = time.Until(deadline) - 100*time.Millisecond
	}

	pool := dockertest.NewPoolT(t, "", dockertest.WithMaxWait(maxWait))

	resource := pool.RunT(t, "gcr.io/cloud-spanner-emulator/emulator",
		dockertest.WithName(fmt.Sprintf("gcp_spanner_emulator-%s", uuid.NewString()[:8])),
		dockertest.WithTag("latest"),
		dockertest.WithContainerConfig(func(c *dockercontainer.Config) {
			c.ExposedPorts = dockernetwork.PortSet{
				dockernetwork.MustParsePort("9010/tcp"): {},
				dockernetwork.MustParsePort("9020/tcp"): {},
			}
		}),
		dockertest.WithoutReuse(),
	)

	t.Setenv("SPANNER_EMULATOR_HOST", "localhost:"+resource.GetPort("9010/tcp"))

	template := `
output:
  sql_insert:
    driver: spanner
    dsn: $DSN
    table: test_table_$ID
    columns: [id, message, metadata]
    args_mapping: 'root = [uuid_v4(), content().string(), meta().string()]'

input:
  gcp_spanner_cdc:
    spanner_dsn: $DSN
    stream_name: test_stream_$ID
    heartbeat_interval: 3s
  processors:
    - mapping: |
        root = this.new_values.message
`

	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		databaseAdminClient.Close()
	})

	suiteOpts := []integration.StreamTestOptFunc{
		integration.StreamTestOptSleepAfterInput(500 * time.Millisecond),
		integration.StreamTestOptSleepAfterOutput(500 * time.Millisecond),
		integration.StreamTestOptTimeout(5 * time.Minute),
		integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
			vars.ID = vars.ID[:8]

			var dsn string
			// TODO(gregfurman): We should explore better session caching when using the same DSN.
			// Re-using the same instance causes issues on shutdown since the sql_insert output
			// destroys all sessions on close. Instead, we need to create a new instance and DB each
			// time to allow these integration tests to run in parallel.
			require.NoError(t, pool.Retry(t.Context(), 0, func() error {
				instanceName, err := CreateInstance(ctx, projectID+"-"+vars.ID, instanceID+"-"+vars.ID)
				if err != nil {
					return err
				}
				dsn, err = CreateDatabase(ctx, instanceName, databaseID+"-"+vars.ID)
				return err
			}))

			vars.General["DSN"] = dsn

			op, err := databaseAdminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
				Database: dsn,
				Statements: []string{
					fmt.Sprintf(`
CREATE TABLE test_table_%s (
  id STRING(MAX) NOT NULL,
  message STRING(MAX),
  metadata STRING(MAX)
) PRIMARY KEY (id)
`, vars.ID),
					fmt.Sprintf("CREATE CHANGE STREAM test_stream_%[1]s FOR test_table_%[1]s", vars.ID),
				},
			})

			require.NoError(t, err)
			require.NoError(t, op.Wait(ctx))
		}),
	}

	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		integration.StreamTestSendBatch(10),
		integration.StreamTestSendBatches(10, 100, 10),
		integration.StreamTestStreamParallel(1000),
		integration.StreamTestStreamSequential(1000),
	)

	suite.Run(t, template, suiteOpts...)
}
