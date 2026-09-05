package aws

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconf "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service/integration"
)

func getLocalStackForStreams(t testing.TB) string {
	t.Helper()

	portInt, err := integration.GetFreePort()
	require.NoError(t, err)

	port := strconv.Itoa(portInt)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 5 * time.Minute

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "localstack/localstack",
		Tag:          "3.0",
		ExposedPorts: []string{"4566/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			docker.Port(port + "/tcp"): {
				{HostIP: "", HostPort: port},
			},
		},
		Env: []string{
			"SERVICES=dynamodb,dynamodbstreams",
			"LS_LOG=warn",
		},
	})
	require.NoError(t, err)
	port = resource.GetPort("4566/tcp")
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})
	_ = resource.Expire(900)

	require.NoError(t, pool.Retry(func() error {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%s/_localstack/health", port))
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("localstack not ready: %d", resp.StatusCode)
		}
		return nil
	}))

	return port
}

func TestIntegrationDynamoDBStreamsSmokeSDK(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	port := getLocalStackForStreams(t)
	endpoint := fmt.Sprintf("http://localhost:%v", port)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conf, err := awsconf.LoadDefaultConfig(ctx,
		awsconf.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
		awsconf.WithRegion("us-east-1"),
	)
	require.NoError(t, err)
	conf.BaseEndpoint = &endpoint

	ddbClient := dynamodb.NewFromConfig(conf)
	streamsClient := dynamodbstreams.NewFromConfig(conf)

	tableName := "smoke-test-streams"

	// Create table with streams enabled.
	_, err = ddbClient.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []dtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: dtypes.ScalarAttributeTypeS},
		},
		KeySchema: []dtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: dtypes.KeyTypeHash},
		},
		BillingMode: dtypes.BillingModePayPerRequest,
		StreamSpecification: &dtypes.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: dtypes.StreamViewTypeNewAndOldImages,
		},
	})
	require.NoError(t, err)

	waiter := dynamodb.NewTableExistsWaiter(ddbClient)
	require.NoError(t, waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}, time.Minute))

	// Get stream ARN.
	desc, err := ddbClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	require.NoError(t, err)
	require.NotNil(t, desc.Table.LatestStreamArn)
	streamARN := *desc.Table.LatestStreamArn
	t.Logf("Stream ARN: %v", streamARN)

	// Describe stream to get shards.
	streamDesc, err := streamsClient.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
		StreamArn: aws.String(streamARN),
	})
	require.NoError(t, err)
	require.NotNil(t, streamDesc.StreamDescription)
	t.Logf("Shards: %d", len(streamDesc.StreamDescription.Shards))

	for _, shard := range streamDesc.StreamDescription.Shards {
		t.Logf("  Shard: %v, parent: %v", *shard.ShardId, shard.ParentShardId)
	}

	require.NotEmpty(t, streamDesc.StreamDescription.Shards, "no shards found")

	// Get shard iterator.
	shardID := *streamDesc.StreamDescription.Shards[0].ShardId
	iterOut, err := streamsClient.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         aws.String(streamARN),
		ShardId:           aws.String(shardID),
		ShardIteratorType: "TRIM_HORIZON",
	})
	require.NoError(t, err)
	require.NotNil(t, iterOut.ShardIterator)
	t.Log("Got shard iterator")

	// Write an item AFTER getting the iterator.
	_, err = ddbClient.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]dtypes.AttributeValue{
			"pk":   &dtypes.AttributeValueMemberS{Value: "test-1"},
			"data": &dtypes.AttributeValueMemberS{Value: "hello"},
		},
	})
	require.NoError(t, err)
	t.Log("Item written after iterator")

	// Also try ListStreams to see what LocalStack reports.
	listOut, err := streamsClient.ListStreams(ctx, &dynamodbstreams.ListStreamsInput{
		TableName: aws.String(tableName),
	})
	require.NoError(t, err)
	t.Logf("ListStreams returned %d streams", len(listOut.Streams))
	for _, s := range listOut.Streams {
		t.Logf("  Stream: %v, table: %v", *s.StreamArn, *s.TableName)
	}

	time.Sleep(2 * time.Second) // Give LocalStack time to propagate

	// Poll for records.
	var records int
	iter := *iterOut.ShardIterator
	for i := 0; i < 10; i++ {
		recOut, err := streamsClient.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
			ShardIterator: aws.String(iter),
		})
		require.NoError(t, err)
		records += len(recOut.Records)
		t.Logf("Poll %d: got %d records (total %d)", i, len(recOut.Records), records)

		if records > 0 {
			for _, r := range recOut.Records {
				t.Logf("  Event: %v, ID: %v", r.EventName, *r.EventID)
			}
			break
		}

		if recOut.NextShardIterator == nil {
			t.Log("Shard exhausted")
			break
		}
		iter = *recOut.NextShardIterator
		time.Sleep(time.Second)
	}

	require.Greater(t, records, 0, "expected at least one record from DynamoDB stream")
}
