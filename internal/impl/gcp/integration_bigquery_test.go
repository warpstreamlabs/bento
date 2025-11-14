package gcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"runtime"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"

	_ "github.com/warpstreamlabs/bento/public/components/pure"
)

var testIntBQProcessorYAML = `
project: test-project
table: test_dataset.test_table
columns:
  - name
  - age
`

var testIntBQInputYAML = `
project: test-project
table: test_dataset.test_table
columns: 
  - name
  - age
`

var testIntBQOutputYAML = `
project: test-project
dataset: test_dataset
table: test_table
columns:
  - name
  - age
`

func TestIntegrationBigQuery(t *testing.T) {
	integration.CheckSkip(t)

	ctx := context.Background()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = 30 * time.Second
	if deadline, ok := t.Deadline(); ok {
		pool.MaxWait = time.Until(deadline) - 100*time.Millisecond
	}

	runOpts := dockertest.RunOptions{
		Name:         "gcp_bigquery_emulator",
		Repository:   "ghcr.io/goccy/bigquery-emulator",
		Tag:          "latest",
		ExposedPorts: []string{"9050/tcp", "9060/tcp"},
		Cmd:          []string{"--project", "test-project"},
	}

	if runtime.GOOS == "darwin" {
		runOpts.Platform = "linux/amd64"
	}

	resource, err := pool.RunWithOptions(&runOpts)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)

	// wait for ready
	err = pool.Retry(func() error {
		conn, err := net.Dial("tcp", resource.GetHostPort("9050/tcp"))
		if err != nil {
			return err
		}
		_ = conn.Close()
		return nil
	})
	require.NoError(t, err)

	bigqueryDockerAddress := fmt.Sprintf("http://%s", resource.GetHostPort("9050/tcp"))

	client, err := bigquery.NewClient(
		ctx,
		"test-project",
		option.WithEndpoint(bigqueryDockerAddress),
		option.WithoutAuthentication(),
	)
	require.NoError(t, err)

	err = pool.Retry(func() error {
		return client.Dataset("test_dataset").Create(ctx, &bigquery.DatasetMetadata{})
	})
	require.NoError(t, err)

	schema := bigquery.Schema{
		{Name: "name", Type: bigquery.StringFieldType},
		{Name: "age", Type: bigquery.IntegerFieldType},
	}
	err = client.Dataset("test_dataset").Table("test_table").Create(ctx, &bigquery.TableMetadata{Schema: schema})
	require.NoError(t, err)

	q := client.Query(`
	INSERT INTO test_dataset.test_table(name, age) 
	VALUES 
	('Alice', 30),
	('Bob', 25);
	`)

	_, err = q.Run(ctx)
	require.NoError(t, err)

	testBigQueryOutput(t, ctx, bigqueryDockerAddress)
	testBigQueryProcessor(t, ctx, bigqueryDockerAddress)
	testBigQueryInput(t, ctx, bigqueryDockerAddress)
}

func testBigQueryOutput(t *testing.T, ctx context.Context, bigqueryDockerAddress string) {
	spec := gcpBigQueryConfig()
	parsed, err := spec.ParseYAML(testIntBQOutputYAML, nil)
	require.NoError(t, err)

	outputConf, err := gcpBigQueryOutputConfigFromParsed(parsed)
	require.NoError(t, err)
	output, err := newGCPBigQueryOutput(outputConf, nil)
	require.NoError(t, err)

	output.clientURL = gcpBQClientURL(bigqueryDockerAddress)

	err = output.Connect(ctx)
	require.NoError(t, err)

	msg := service.NewMessage([]byte(`{"name":"Claire", "age": 45}`))

	msgBatch := []*service.Message{msg}

	err = output.WriteBatch(ctx, msgBatch)
	require.NoError(t, err)
}

func testBigQueryProcessor(t *testing.T, ctx context.Context, bigqueryDockerAddress string) {

	spec := newBigQuerySelectProcessorConfig()

	parsed, err := spec.ParseYAML(testIntBQProcessorYAML, nil)
	require.NoError(t, err)

	proc, err := newBigQuerySelectProcessor(parsed, &bigQueryProcessorOptions{
		clientOptions: []option.ClientOption{
			option.WithoutAuthentication(),
			option.WithEndpoint(bigqueryDockerAddress)},
	})
	require.NoError(t, err)

	inbatch := service.MessageBatch{service.NewMessage([]byte(`{}`))}

	outBatches, err := proc.ProcessBatch(ctx, inbatch)
	require.NoError(t, err)
	require.Len(t, outBatches, 1)

	outbatch := outBatches[0]
	require.Len(t, outbatch, 1)

	msgBytes, err := outbatch[0].AsBytes()
	require.NoError(t, err)

	expected := []map[string]any{
		{"age": 30, "name": "Alice"},
		{"age": 25, "name": "Bob"},
		{"age": 45, "name": "Claire"},
	}

	expectedMsg, err := json.Marshal(expected)
	require.NoError(t, err)

	require.JSONEq(t, string(expectedMsg), string(msgBytes))

}

func testBigQueryInput(t *testing.T, ctx context.Context, bigqueryDockerAddress string) {

	spec := newBigQuerySelectInputConfig()
	parsed, err := spec.ParseYAML(testIntBQInputYAML, nil)
	require.NoError(t, err)

	input, err := newBigQuerySelectInput(parsed, nil, []option.ClientOption{
		option.WithoutAuthentication(),
		option.WithEndpoint(bigqueryDockerAddress),
	})
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)

	expectedMessages := []string{
		`{"age":30, "name":"Alice"}`,
		`{"age":25, "name":"Bob"}`,
		`{"age":45, "name":"Claire"}`,
	}

	for _, expected := range expectedMessages {
		msg, ackFunc, err := input.Read(ctx)
		require.NoError(t, err)

		err = ackFunc(ctx, err)
		require.NoError(t, err)

		msgBytes, err := msg.AsBytes()
		require.NoError(t, err)

		require.JSONEq(t, expected, string(msgBytes))
	}
}
