package aws

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"

	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
)

func TestIntegrationDDBPartiql(t *testing.T) {
	integration.CheckSkip(t)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "amazon/dynamodb-local",
		ExposedPorts: []string{"8000/tcp"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		return createMusicTable(context.Background(), t, resource.GetPort("8000/tcp"))
	}))

	// use bento to populate the table
	popDataBuilder := service.NewStreamBuilder()

	err = popDataBuilder.SetYAML(fmt.Sprintf(`
input:
  csv:
    paths: ["./resources/dynamodbMusicTestDataPopTable.csv"]

output:
  aws_dynamodb:
    table: Music
    json_map_columns:
      uuid: uuid
      title: title
      year: year
    endpoint: http://localhost:%v
    region: us-east-1
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
`, resource.GetPort("8000/tcp")))
	require.NoError(t, err)

	popDataStream, err := popDataBuilder.Build()
	require.NoError(t, err)

	err = popDataStream.Run(context.Background())
	require.NoError(t, err)

	// insert using processor under test:
	insertConf, err := testutil.ProcessorFromYAML(fmt.Sprintf(`
aws_dynamodb_partiql:
  query: "INSERT INTO Music VALUE {'uuid': ?, 'title': ?, 'year': ?}"
  args_mapping: |
      root = [
        { "S": this.uuid },
        { "S": this.title },
        { "S": this.year },
      ]
  endpoint: http://localhost:%v
  region: us-east-1
  credentials:
    id: xxxxx
    secret: xxxxx
    token: xxxxx
`, resource.GetPort("8000/tcp")))
	require.NoError(t, err)

	insertProcessor, err := mock.NewManager().NewProcessor(insertConf)
	require.NoError(t, err)

	msgInsert := message.QuickBatch([][]byte{[]byte(`{"uuid":"00000000-0000-0000-0000-000000000020", "title":"Hot Pink", "year":"2019"}`)})
	_, err = insertProcessor.ProcessBatch(context.Background(), msgInsert)
	require.NoError(t, err)

	// select using processor under test:
	selectConf, err := testutil.ProcessorFromYAML(fmt.Sprintf(`
aws_dynamodb_partiql:
  query: "SELECT * FROM Music WHERE uuid = ?"
  args_mapping: |
      root = [
        { "S": this.uuid },
      ]
  endpoint: http://localhost:%v
  region: us-east-1
  credentials:
    id: xxxxx
    secret: xxxxx
    token: xxxxx
`, resource.GetPort("8000/tcp")))
	require.NoError(t, err)

	selectProcessor, err := mock.NewManager().NewProcessor(selectConf)
	require.NoError(t, err)

	msgSelect := message.QuickBatch([][]byte{[]byte(`{"uuid":"00000000-0000-0000-0000-000000000020"}`)})
	resBatches, err := selectProcessor.ProcessBatch(context.Background(), msgSelect)
	require.NoError(t, err)

	resBatch := resBatches[0]
	actualBytes := resBatch.Get(0).AsBytes()

	var actual map[string]any
	err = json.Unmarshal(actualBytes, &actual)
	require.NoError(t, err)

	expected := map[string]any{
		"uuid":  map[string]any{"S": "00000000-0000-0000-0000-000000000020"},
		"title": map[string]any{"S": "Hot Pink"},
		"year":  map[string]any{"S": "2019"},
	}

	assert.Equal(t, expected, actual)
}
