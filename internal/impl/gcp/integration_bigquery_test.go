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
	dockercontainer "github.com/moby/moby/api/types/container"
	dockernetwork "github.com/moby/moby/api/types/network"
	"github.com/ory/dockertest/v4"
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

	maxWait := 30 * time.Second
	if deadline, ok := t.Deadline(); ok {
		maxWait = time.Until(deadline) - 100*time.Millisecond
	}

	pool := dockertest.NewPoolT(t, "", dockertest.WithMaxWait(maxWait))

	if runtime.GOOS == "darwin" {
		// v4 has no Platform option; pre-pulling picks the architecture because
		// Run skips its own pull once the image inspects cleanly.
		pullAMD64(t, pool, "ghcr.io/goccy/bigquery-emulator:latest")
	}

	resource := pool.RunT(t, "ghcr.io/goccy/bigquery-emulator",
		dockertest.WithName("gcp_bigquery_emulator"),
		dockertest.WithTag("latest"),
		// Unbound ports that GetHostPort reads below.
		dockertest.WithContainerConfig(func(c *dockercontainer.Config) {
			c.ExposedPorts = dockernetwork.PortSet{
				dockernetwork.MustParsePort("9050/tcp"): {},
				dockernetwork.MustParsePort("9060/tcp"): {},
			}
		}),
		dockertest.WithCmd([]string{"--project", "test-project"}),
		dockertest.WithoutReuse(),
	)

	// wait for ready
	err := pool.Retry(t.Context(), 0, func() error {
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

	err = pool.Retry(t.Context(), 0, func() error {
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
