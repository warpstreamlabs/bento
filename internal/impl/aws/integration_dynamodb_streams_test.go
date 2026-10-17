package aws

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconf "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"

	_ "github.com/warpstreamlabs/bento/public/components/pure"
)

// DynamoDB Streams requires LocalStack >= 3.0 (the shared GetLocalStack uses
// 4.9.2 which does not return stream records). The helper getLocalStackForStreams
// is defined in integration_dynamodb_streams_smoke_test.go.

func createStreamEnabledTable(ctx context.Context, t testing.TB, port, tableName string) {
	t.Helper()

	endpoint := fmt.Sprintf("http://localhost:%v", port)
	conf, err := awsconf.LoadDefaultConfig(ctx,
		awsconf.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
		awsconf.WithRegion("us-east-1"),
	)
	require.NoError(t, err)

	conf.BaseEndpoint = &endpoint
	client := dynamodb.NewFromConfig(conf)

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest,
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: types.StreamViewTypeNewAndOldImages,
		},
	})
	require.NoError(t, err)

	waiter := dynamodb.NewTableExistsWaiter(client)
	require.NoError(t, waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}, time.Minute))
}

func putItems(ctx context.Context, t testing.TB, port, tableName string, items []map[string]types.AttributeValue) {
	t.Helper()

	endpoint := fmt.Sprintf("http://localhost:%v", port)
	conf, err := awsconf.LoadDefaultConfig(ctx,
		awsconf.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
		awsconf.WithRegion("us-east-1"),
	)
	require.NoError(t, err)

	conf.BaseEndpoint = &endpoint
	client := dynamodb.NewFromConfig(conf)

	for _, item := range items {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item:      item,
		})
		require.NoError(t, err)
	}
}

func ddbStreamsInputYAML(tableName, checkpointTable, port string) string {
	return fmt.Sprintf(`
input:
  aws_dynamodb_streams:
    table: %v
    start_from_oldest: true
    checkpoint:
      table: %v
      create: true
    commit_period: 1s
    rebalance_period: 3s
    lease_period: 5s
    poll_interval: 500ms
    endpoint: http://localhost:%v
    region: us-east-1
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx

output:
  drop: {}
`, tableName, checkpointTable, port)
}

func TestIntegrationDynamoDBStreams(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	servicePort := getLocalStackForStreams(t)

	t.Run("basic_consumption", func(t *testing.T) {
		testDynamoDBStreamsBasicConsumption(t, servicePort)
	})

	t.Run("checkpointing", func(t *testing.T) {
		testDynamoDBStreamsCheckpointing(t, servicePort)
	})
}

func testDynamoDBStreamsBasicConsumption(t *testing.T, port string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	tableName := "stream-basic-test"
	checkpointTable := "stream-basic-test-cp"

	createStreamEnabledTable(ctx, t, port, tableName)

	builder := service.NewStreamBuilder()
	require.NoError(t, builder.SetYAML(ddbStreamsInputYAML(tableName, checkpointTable, port)))

	var mu sync.Mutex
	var received []map[string]any
	var receivedMeta []map[string]string
	require.NoError(t, builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
		b, err := msg.AsBytes()
		if err != nil {
			return err
		}
		var parsed map[string]any
		if err := json.Unmarshal(b, &parsed); err != nil {
			return err
		}

		meta := make(map[string]string)
		_ = msg.MetaWalk(func(key, value string) error {
			meta[key] = value
			return nil
		})

		mu.Lock()
		received = append(received, parsed)
		receivedMeta = append(receivedMeta, meta)
		mu.Unlock()
		return nil
	}))

	stream, err := builder.Build()
	require.NoError(t, err)

	streamCtx, streamCancel := context.WithCancel(ctx)
	defer streamCancel()
	errChan := make(chan error, 1)
	go func() {
		errChan <- stream.Run(streamCtx)
	}()

	// Wait for the consumer to establish shard iterators.
	time.Sleep(5 * time.Second)

	// Write items after consumer is running (LocalStack requires this ordering).
	putItems(ctx, t, port, tableName, []map[string]types.AttributeValue{
		{
			"pk":   &types.AttributeValueMemberS{Value: "user-1"},
			"name": &types.AttributeValueMemberS{Value: "Alice"},
		},
		{
			"pk":   &types.AttributeValueMemberS{Value: "user-2"},
			"name": &types.AttributeValueMemberS{Value: "Bob"},
		},
		{
			"pk":   &types.AttributeValueMemberS{Value: "user-3"},
			"name": &types.AttributeValueMemberS{Value: "Charlie"},
		},
	})

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) >= 3
	}, 60*time.Second, 500*time.Millisecond, "timed out waiting for stream events")

	streamCancel()
	<-errChan

	mu.Lock()
	defer mu.Unlock()

	eventNames := make(map[string]bool)
	pks := make(map[string]bool)
	for i, evt := range received {
		eventNames[evt["eventName"].(string)] = true

		dynamo, ok := evt["dynamodb"].(map[string]any)
		require.True(t, ok, "event %d missing dynamodb field", i)

		newImage, ok := dynamo["newImage"].(map[string]any)
		require.True(t, ok, "event %d missing newImage", i)

		pk, ok := newImage["pk"].(string)
		require.True(t, ok, "event %d missing pk in newImage", i)
		pks[pk] = true

		assert.Equal(t, tableName, receivedMeta[i]["dynamodb_table"])
		assert.NotEmpty(t, receivedMeta[i]["dynamodb_stream_arn"])
		assert.NotEmpty(t, receivedMeta[i]["dynamodb_shard_id"])
		assert.NotEmpty(t, receivedMeta[i]["dynamodb_event_id"])
		assert.Equal(t, "INSERT", receivedMeta[i]["dynamodb_event_name"])
		assert.NotEmpty(t, receivedMeta[i]["dynamodb_sequence_number"])
	}

	assert.True(t, eventNames["INSERT"], "expected INSERT events")
	assert.True(t, pks["user-1"], "missing user-1")
	assert.True(t, pks["user-2"], "missing user-2")
	assert.True(t, pks["user-3"], "missing user-3")
}

// testDynamoDBStreamsCheckpointing verifies that consuming records creates
// checkpoint entries in the DynamoDB checkpoint table with the expected schema.
func testDynamoDBStreamsCheckpointing(t *testing.T, port string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	tableName := "stream-cp-test"
	checkpointTable := "stream-cp-test-cp"

	createStreamEnabledTable(ctx, t, port, tableName)

	builder := service.NewStreamBuilder()
	require.NoError(t, builder.SetYAML(ddbStreamsInputYAML(tableName, checkpointTable, port)))

	var mu sync.Mutex
	var count int
	require.NoError(t, builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
		mu.Lock()
		count++
		mu.Unlock()
		return nil
	}))

	stream, err := builder.Build()
	require.NoError(t, err)

	streamCtx, streamCancel := context.WithCancel(ctx)
	defer streamCancel()
	errChan := make(chan error, 1)
	go func() {
		errChan <- stream.Run(streamCtx)
	}()

	time.Sleep(5 * time.Second)

	putItems(ctx, t, port, tableName, []map[string]types.AttributeValue{
		{
			"pk":   &types.AttributeValueMemberS{Value: "cp-item-1"},
			"data": &types.AttributeValueMemberS{Value: "one"},
		},
	})

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return count >= 1
	}, 30*time.Second, 500*time.Millisecond, "timed out waiting for events")

	// Wait for checkpoint to flush (commit_period is 1s).
	time.Sleep(3 * time.Second)

	streamCancel()
	<-errChan

	// Verify the checkpoint table was created and has entries.
	endpoint := fmt.Sprintf("http://localhost:%v", port)
	conf, err := awsconf.LoadDefaultConfig(ctx,
		awsconf.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
		awsconf.WithRegion("us-east-1"),
	)
	require.NoError(t, err)
	conf.BaseEndpoint = &endpoint
	client := dynamodb.NewFromConfig(conf)

	scanOut, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String(checkpointTable),
	})
	require.NoError(t, err)

	require.NotEmpty(t, scanOut.Items, "checkpoint table should have at least one entry")

	for _, item := range scanOut.Items {
		// Verify expected checkpoint schema fields.
		streamID, ok := item["StreamID"].(*types.AttributeValueMemberS)
		require.True(t, ok, "checkpoint missing StreamID")
		assert.Equal(t, tableName, streamID.Value, "StreamID should be the table name")

		_, ok = item["ShardID"].(*types.AttributeValueMemberS)
		require.True(t, ok, "checkpoint missing ShardID")

		_, ok = item["SequenceNumber"].(*types.AttributeValueMemberS)
		assert.True(t, ok, "checkpoint should have SequenceNumber")

		t.Logf("Checkpoint entry: StreamID=%v, ShardID=%v", streamID.Value, item["ShardID"])
	}
}
