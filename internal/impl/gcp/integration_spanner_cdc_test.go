package gcp

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/anicoll/screamer"
	"github.com/anicoll/screamer/pkg/interceptor"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

const (
	testTableName = "PartitionMetadata"
	projectID     = "project-id"
	instanceID    = "instance-id"
	databaseID    = "stream-database"
)

type SpannerCDCIntegrationTestSuite struct {
	suite.Suite
	container *dockertest.Resource

	client *spanner.Client
	dsn    string
}

func TestSpannerCDCIntegrationTestSuite(t *testing.T) {
	integration.CheckSkip(t)
	suite.Run(t, new(SpannerCDCIntegrationTestSuite))
}

func (s *SpannerCDCIntegrationTestSuite) SetupSuite() {
	ctx := context.Background()

	pool, err := dockertest.NewPool("")
	require.NoError(s.T(), err)

	pool.MaxWait = 30 * time.Second
	if deadline, ok := s.T().Deadline(); ok {
		pool.MaxWait = time.Until(deadline) - 100*time.Millisecond
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Name:         "gcp_spanner_emulator",
		Repository:   "gcr.io/cloud-spanner-emulator/emulator",
		Tag:          "latest",
		ExposedPorts: []string{"9010/tcp", "9020/tcp"},
	})
	s.NoError(err)
	s.T().Cleanup(func() {
		s.NoError(pool.Purge(resource))
	})
	_ = resource.Expire(900)

	os.Setenv("SPANNER_EMULATOR_HOST", "localhost:"+resource.GetPort("9010/tcp"))
	s.T().Cleanup(func() {
		defer os.Unsetenv("SPANNER_EMULATOR_HOST")
	})
	var instanceName string
	// Wait for gcp-spanner-emulator to properly start up
	err = pool.Retry(func() error {
		ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelFunc()

		if instanceName, err = createInstance(ctx, projectID, instanceID); err != nil {
			return err
		}
		return nil
	})

	require.NoError(s.T(), err, "Failed to start gcp-spanner-emulator")

	s.dsn, err = createDatabase(ctx, instanceName, databaseID) // create database
	s.NoError(err)
}

func (s *SpannerCDCIntegrationTestSuite) TearDownSuite() {
	if s.container != nil {
		err := s.container.Close()
		s.NoError(err)
	}
}

func (s *SpannerCDCIntegrationTestSuite) AfterTest(suiteName, testName string) {
	if s.client != nil {
		s.client.Close()
	}
}

func createTableAndChangeStream(ctx context.Context, databaseName, tableName, streamName string) error {
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	defer databaseAdminClient.Close()

	op, err := databaseAdminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database: databaseName,
		Statements: []string{
			fmt.Sprintf(`CREATE TABLE %s (
				Int64           INT64,
				String          STRING(MAX),
			) PRIMARY KEY (Int64)`, tableName),
			fmt.Sprintf(`CREATE CHANGE STREAM %s FOR %s`, streamName, tableName),
		},
	})
	if err != nil {
		return err
	}

	if err := op.Wait(ctx); err != nil {
		return err
	}

	return err
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

func (s *SpannerCDCIntegrationTestSuite) Test_SpannerCDC() {
	var err error
	ctx := context.Background()
	spannerInterceptor := interceptor.NewQueueInterceptor(100)
	// This is specifically for the emulator as it does not support concurrent requests.
	s.client, err = spanner.NewClient(ctx, s.dsn, option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(spannerInterceptor.UnaryInterceptor)))
	s.NoError(err)

	tableName := "test"
	streamName := tableName + "Stream"
	metaTableName := "meta_table"

	s.Run("create table and change stream", func() {
		err = createTableAndChangeStream(ctx, s.client.DatabaseName(), tableName, streamName)
		require.NoError(s.T(), err)
	})

	var cdcCfg cdcConfig
	s.Run("parse base template spec", func() {
		spec := spannerCdcSpec()
		template := fmt.Sprintf(`
spanner_dsn: %[1]s
metadata_table: %[2]s
spanner_metadata_dsn: %[1]s
stream_name: %[3]s
`, s.dsn, metaTableName, streamName)
		parsed, err := spec.ParseYAML(template, nil)
		s.NoError(err)
		cdcCfg, err = cdcConfigFromParsed(parsed)
		s.NoError(err)
	})

	var input service.Input
	s.Run("new up input and connect", func() {
		input, err = newGcpSpannerCDCInput(cdcCfg, service.MockResources())
		s.NoError(err)
		s.NoError(input.Connect(ctx))
	})

	s.Run("read from input", func() {
		var commetTimestamp time.Time
		s.Run("generate change stream events", func() {
			commetTimestamp, err = s.client.Apply(ctx, []*spanner.Mutation{
				spanner.InsertOrUpdate(tableName, []string{"Int64", "String"}, []interface{}{30, "Alice"}),
				spanner.InsertOrUpdate(tableName, []string{"Int64", "String"}, []interface{}{25, "Bob"}),
				spanner.InsertOrUpdate(tableName, []string{"Int64", "String"}, []interface{}{31, "Claire"}),
			})
			s.NoError(err)
		})

		expectedMessages := []screamer.DataChangeRecord{
			{
				CommitTimestamp: commetTimestamp,
				RecordSequence:  "000000",
				TableName:       tableName,
				ColumnTypes: []*screamer.ColumnType{
					{
						Name: "Int64",
						Type: screamer.Type{
							Code:             screamer.TypeCode_INT64,
							ArrayElementType: screamer.TypeCode_NONE,
						},
						IsPrimaryKey:    true,
						OrdinalPosition: 1,
					},
					{
						Name: "String",
						Type: screamer.Type{
							Code:             screamer.TypeCode_STRING,
							ArrayElementType: screamer.TypeCode_NONE,
						},
						OrdinalPosition: 2,
					},
				},
				Mods: []*screamer.Mod{
					{
						Keys: map[string]interface{}{"Int64": "30"},
						NewValues: map[string]interface{}{
							"String": "Alice",
						},
						OldValues: nil,
					},
					{
						Keys: map[string]interface{}{"Int64": "25"},
						NewValues: map[string]interface{}{
							"String": "Bob",
						},
						OldValues: nil,
					},
					{
						Keys: map[string]interface{}{"Int64": "31"},
						NewValues: map[string]interface{}{
							"String": "Claire",
						},
						OldValues: nil,
					},
				},
				ValueCaptureType: "OLD_AND_NEW_VALUES",
				ModType:          screamer.ModType_INSERT,
			},
		}
		t := s.T()

		for _, expected := range expectedMessages {
			msg, ackFunc, err := input.Read(ctx)
			require.NoError(t, err)

			err = ackFunc(ctx, err)
			require.NoError(t, err)

			msgBytes, err := msg.AsBytes()
			require.NoError(t, err)

			dcr := screamer.DataChangeRecord{}
			s.NoError(json.Unmarshal(msgBytes, &dcr))
			require.Equal(t, expected.ModType, dcr.ModType)

			require.Empty(t, cmp.Diff(expected.Mods, dcr.Mods, cmpopts.SortSlices(func(a, b *screamer.Mod) bool {
				return a.Keys["Int64"].(string) < b.Keys["Int64"].(string)
			})))

			require.Empty(t, cmp.Diff(expected.ColumnTypes, dcr.ColumnTypes, cmpopts.SortSlices(func(a, b *screamer.ColumnType) bool {
				return a.Name < b.Name
			})))

			require.True(t, dcr.IsLastRecordInTransactionInPartition)
			require.Equal(t, tableName, dcr.TableName)
			require.Equal(t, commetTimestamp.UTC(), dcr.CommitTimestamp.UTC())
		}
	})

	s.Run("handles context cancellation while reading", func() {
		// Create a context with cancellation
		readCtx, cancel := context.WithCancel(ctx)

		// Start a goroutine to cancel the context after a short delay
		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		// Generate some data to read
		_, err = s.client.Apply(readCtx, []*spanner.Mutation{
			spanner.InsertOrUpdate(tableName, []string{"Int64", "String"}, []interface{}{40, "Daniel"}),
		})
		s.NoError(err)

		// Attempt to read with a context that will be cancelled
		_, _, err = input.Read(readCtx)

		// Verify we got a context cancellation error
		s.ErrorIs(err, context.Canceled, "Expected context.Canceled error, got: %v", err)
	})
}
