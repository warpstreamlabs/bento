package gcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

type gcpEmulator struct {
	httpEndpoint string
	grpcEndpoint string

	client        *bigquery.Client
	storageClient *managedwriter.Client

	schema bigquery.Schema
}

func (e *gcpEmulator) Close() error {
	e.client.Close()
	e.storageClient.Close()
	return nil
}

func newBigQueryEmulator(ctx context.Context, projectID, httpEndpoint, grpcEndpoint string, bqSchema *bigquery.Schema) (*gcpEmulator, error) {
	client, err := bigquery.NewClient(ctx, projectID, option.WithoutAuthentication(), option.WithEndpoint(httpEndpoint))
	if err != nil {
		return nil, err
	}

	conn, err := grpc.NewClient(grpcEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	storageClient, err := managedwriter.NewClient(ctx, projectID, option.WithoutAuthentication(), option.WithEndpoint(grpcEndpoint), option.WithGRPCConn(conn))
	if err != nil {
		return nil, err
	}

	schema := bigquery.Schema{
		{Name: "what1", Type: bigquery.StringFieldType},
		{Name: "what2", Type: bigquery.IntegerFieldType},
		{Name: "what3", Type: bigquery.BooleanFieldType},
	}
	if bqSchema != nil {
		schema = *bqSchema
	}

	return &gcpEmulator{
		httpEndpoint: httpEndpoint,
		grpcEndpoint: grpcEndpoint,

		storageClient: storageClient,
		client:        client,

		schema: schema,
	}, nil
}

func (e *gcpEmulator) setup(ctx context.Context, projectID, datasetID, tableID string) error {
	var err error

	dataset := e.client.DatasetInProject(projectID, datasetID)
	_, err = dataset.Metadata(ctx)
	if err != nil {
		return err
	}

	metaData := &bigquery.TableMetadata{
		Name:           tableID,
		Schema:         e.schema,
		ExpirationTime: time.Now().AddDate(1, 0, 0),
	}

	table := dataset.Table(tableID)
	if _, tblErr := table.Metadata(ctx); tblErr != nil && hasStatusCode(tblErr, http.StatusNotFound) {
		if err := table.Create(ctx, metaData); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	// Verify _default stream exists
	destTable := managedwriter.TableParentFromParts(projectID, datasetID, tableID)
	defStreamName := fmt.Sprintf("%s/streams/_default", destTable)

	_, err = e.storageClient.GetWriteStream(ctx, &storagepb.GetWriteStreamRequest{
		Name: defStreamName,
		View: storagepb.WriteStreamView_FULL,
	})
	if err != nil {
		return fmt.Errorf("failed to get write stream for table: %w", err)
	}

	return nil
}

func setupBigQueryEmulator(t *testing.T, projectID, datasetID, tableID string, schema *bigquery.Schema) *gcpEmulator {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = 30 * time.Second
	if deadline, ok := t.Deadline(); ok {
		pool.MaxWait = time.Until(deadline) - 100*time.Millisecond
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "ghcr.io/goccy/bigquery-emulator",
		Tag:          "latest",
		ExposedPorts: []string{"9050/tcp", "9060/tcp"},
		Cmd:          []string{"--project", projectID, "--dataset", datasetID, "--log-level", "debug"},
		Platform:     "linux/x86_64",
	})
	require.NoError(t, err)
	require.NoError(t, resource.Expire(900))

	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	httpEndpoint := "http://localhost:" + resource.GetPort("9050/tcp")
	grpcEndpoint := "localhost:" + resource.GetPort("9060/tcp")

	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()

	emulator, err := newBigQueryEmulator(ctx, projectID, httpEndpoint, grpcEndpoint, schema)
	assert.NoError(t, err)

	retryErr := pool.Retry(func() error {
		ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelFunc()
		return emulator.setup(ctx, projectID, datasetID, tableID)
	})
	require.NoError(t, retryErr)

	return emulator
}

type mockBQIterator struct {
	err error

	rows []string

	idx int
	// the index at which to return an error
	errIdx int
}

func (ti *mockBQIterator) Next(dst any) error {
	if ti.err != nil && ti.idx == ti.errIdx {
		return ti.err
	}

	if ti.idx >= len(ti.rows) {
		return iterator.Done
	}

	row := ti.rows[ti.idx]

	ti.idx++

	return json.Unmarshal([]byte(row), dst)
}

type mockBQClient struct {
	mock.Mock
}

func (client *mockBQClient) RunQuery(ctx context.Context, options *bqQueryBuilderOptions) (bigqueryIterator, error) {
	args := client.Called(ctx, options)

	var iter bigqueryIterator
	if mi := args.Get(0); mi != nil {
		iter = mi.(bigqueryIterator)
	}

	return iter, args.Error(1)
}

func (client *mockBQClient) Close() error {
	return nil
}

func TestParseQueryPriority(t *testing.T) {
	spec := service.NewConfigSpec().Field(service.NewStringField("foo").Default(""))

	conf, err := spec.ParseYAML(`foo: batch`, nil)
	require.NoError(t, err)
	priority, err := parseQueryPriority(conf, "foo")
	require.NoError(t, err)
	require.Equal(t, bigquery.BatchPriority, priority)

	conf, err = spec.ParseYAML(`foo: interactive`, nil)
	require.NoError(t, err)
	priority, err = parseQueryPriority(conf, "foo")
	require.NoError(t, err)
	require.Equal(t, bigquery.InteractivePriority, priority)
}

func TestParseQueryPriority_Empty(t *testing.T) {
	spec := service.NewConfigSpec().Field(service.NewStringField("foo").Default(""))

	conf, err := spec.ParseYAML("", nil)
	require.NoError(t, err)
	priority, err := parseQueryPriority(conf, "foo")
	require.NoError(t, err)
	require.Equal(t, priority, bigquery.QueryPriority(""))
}

func TestParseQueryPriority_Unrecognised(t *testing.T) {
	spec := service.NewConfigSpec().Field(service.NewStringField("foo").Default(""))

	conf, err := spec.ParseYAML("foo: blahblah", nil)
	require.NoError(t, err)
	priority, err := parseQueryPriority(conf, "foo")
	require.ErrorContains(t, err, "unrecognised query priority")
	require.Equal(t, priority, bigquery.QueryPriority(""))
}
