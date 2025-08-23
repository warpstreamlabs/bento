package gcp

import (
	"context"
	"fmt"
	"testing"
	"time"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
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

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = 30 * time.Second
	if deadline, ok := t.Deadline(); ok {
		pool.MaxWait = time.Until(deadline) - 100*time.Millisecond
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Name:         fmt.Sprintf("gcp_spanner_emulator-%s", uuid.NewString()[:8]),
		Repository:   "gcr.io/cloud-spanner-emulator/emulator",
		Tag:          "latest",
		ExposedPorts: []string{"9010/tcp", "9020/tcp"},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, pool.Purge(resource))
	})
	_ = resource.Expire(900)

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
			require.NoError(t, pool.Retry(func() error {
				instanceName, err := createInstance(ctx, projectID+"-"+vars.ID, instanceID+"-"+vars.ID)
				if err != nil {
					return err
				}
				dsn, err = createDatabase(ctx, instanceName, databaseID+"-"+vars.ID)
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

// CreateInstance creates a new Spanner instance with the given project and instance ID.
// Returns the instance name or an error.
func createInstance(ctx context.Context, parentProjectID, instanceID string) (string, error) {
	instanceAdminClient, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		return "", err
	}
	defer instanceAdminClient.Close()

	op, err := instanceAdminClient.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     "projects/" + parentProjectID,
		InstanceId: instanceID,
		Instance: &instancepb.Instance{
			Config:          "projects/model/instanceConfigs/regional-us-central1",
			DisplayName:     instanceID,
			ProcessingUnits: 100,
		},
	})
	if err != nil {
		return "", err
	}

	resp, err := op.Wait(ctx)
	if err != nil {
		return "", err
	}

	return resp.Name, nil
}

// CreateDatabase creates a new Spanner database with the given parent instance name and database ID.
// Returns the database name or an error.
func createDatabase(ctx context.Context, parentInstanceName, databaseID string) (string, error) {
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return "", err
	}
	defer databaseAdminClient.Close()

	op, err := databaseAdminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          parentInstanceName,
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", databaseID),
	})
	if err != nil {
		return "", err
	}

	resp, err := op.Wait(ctx)
	if err != nil {
		return "", err
	}

	return resp.Name, nil
}
