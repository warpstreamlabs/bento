package aws

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/go-faker/faker/v4"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

type dynamoDBClientWatcher struct {
	dynamoDBAPI

	BatchCalls      atomic.Int32
	IndividualCalls atomic.Int32
}

func (dcw *dynamoDBClientWatcher) BatchWriteItem(
	ctx context.Context, in *dynamodb.BatchWriteItemInput, opts ...func(*dynamodb.Options),
) (*dynamodb.BatchWriteItemOutput, error) {
	dcw.BatchCalls.Add(1)
	return dcw.dynamoDBAPI.BatchWriteItem(ctx, in, opts...)
}

func (dcw *dynamoDBClientWatcher) PutItem(
	ctx context.Context, in *dynamodb.PutItemInput, opts ...func(*dynamodb.Options),
) (*dynamodb.PutItemOutput, error) {
	dcw.IndividualCalls.Add(1)
	return dcw.dynamoDBAPI.PutItem(ctx, in, opts...)
}

func startDynamodbLocal(t *testing.T, tableName string) (port string) {
	t.Helper()

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
		return createDynamodDbTable(tableName, resource.GetPort("8000/tcp"))
	}))

	return resource.GetPort("8000/tcp")
}

func createDynamodDbTable(tableName, port string) (err error) {
	endpoint := fmt.Sprintf("http://localhost:%v", port)

	conf, err := config.LoadDefaultConfig(context.Background(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"xxxxx",
			"xxxxx",
			"xxxxx"),
		),
		config.WithRegion("us-east-1"),
	)
	if err != nil {
		return err
	}

	conf.BaseEndpoint = &endpoint
	client := dynamodb.NewFromConfig(conf)

	_, err = client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{{
			AttributeName: aws.String("id"),
			KeyType:       types.KeyTypeHash,
		}},
		TableName:   aws.String(tableName),
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		return err
	} else {
		waiter := dynamodb.NewTableExistsWaiter(client)
		err = waiter.Wait(context.Background(), &dynamodb.DescribeTableInput{
			TableName: aws.String(tableName)}, 5*time.Minute)
		if err != nil {
			return err
		}
	}
	return nil
}

func TestHandleBatchSizeGreaterThan25(t *testing.T) {
	integration.CheckSkip(t)

	port := startDynamodbLocal(t, "FooTable")

	db := testDDBOWriterV2(t, fmt.Sprintf(`
table: FooTable
json_map_columns:
  id: id
  name: name
json_map_datatypes:
  id: S
  name: S
endpoint: http://localhost:%v
region: us-east-1
credentials:
  id: xxxxx
  secret: xxxxx
  token: xxxxx`, port))

	connectCtx, connectDone := context.WithTimeout(context.Background(), 5*time.Second)
	defer connectDone()
	err := db.Connect(connectCtx)
	require.NoError(t, err)

	watcher := &dynamoDBClientWatcher{dynamoDBAPI: db.client}
	db.client = watcher

	// create a batch size that would be too big for 1 call to BatchWriteItem
	var batch service.MessageBatch
	for range 30 {
		batch = append(batch, service.NewMessage(fmt.Appendf(nil, `{"id": "%v", "name": "%v"}`, faker.UUIDHyphenated(), faker.Name())))
	}

	writeCtx, writeDone := context.WithTimeout(context.Background(), 5*time.Second)
	defer writeDone()
	err = db.WriteBatch(writeCtx, batch)
	require.NoError(t, err)

	// we break the batch down and continue to use BatchWriteItem not PutItem
	assert.Equal(t, int32(2), watcher.BatchCalls.Load())
	assert.Equal(t, int32(0), watcher.IndividualCalls.Load())
}
