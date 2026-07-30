package aws

import (
	"context"
	"fmt"
	"strings"
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

func startDynamodbLocal(t *testing.T, tableName string, opts ...bool) (port string) {
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
		return createDynamodDbTable(tableName, resource.GetPort("8000/tcp"), opts...)
	}))

	return resource.GetPort("8000/tcp")
}

func createDynamodDbTable(tableName, port string, opts ...bool) (err error) {
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

	var ks []types.KeySchemaElement
	var ad []types.AttributeDefinition
	if opts == nil { // TODO sort this opts out...
		ks = []types.KeySchemaElement{{
			AttributeName: aws.String("id"),
			KeyType:       types.KeyTypeHash,
		}}
		ad = []types.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		}
	} else {
		ks = []types.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String("sort"),
				KeyType:       types.KeyTypeRange,
			},
		}
		ad = []types.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("sort"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		}
	}

	_, err = client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		AttributeDefinitions: ad,
		KeySchema:            ks,
		TableName:            aws.String(tableName),
		BillingMode:          types.BillingModePayPerRequest,
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
partition_key: id
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

func TestHandleItemSizeOver400KB(t *testing.T) {
	integration.CheckSkip(t)

	port := startDynamodbLocal(t, "FooTable")

	db := testDDBOWriterV2(t, fmt.Sprintf(`
table: FooTable
partition_key: id
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

	var batch service.MessageBatch
	for range 4 {
		batch = append(batch, service.NewMessage(fmt.Appendf(nil, `{"id": "%v", "name": "%v"}`, faker.UUIDHyphenated(), faker.Name())))
	}

	batch = append(batch, service.NewMessage(fmt.Appendf(nil, `{"id": "%v", "name": "%v"}`, faker.UUIDHyphenated(), strings.Repeat("A", 401_000))))

	var bErr *service.BatchError
	errs := []error{}

	index := batch.Index()

	writeCtx, writeDone := context.WithTimeout(context.Background(), 5*time.Second)
	defer writeDone()
	err = db.WriteBatch(writeCtx, batch)

	require.ErrorAsf(t, err, &bErr, "expected a batch error but got: %T: %v", bErr, bErr)
	require.ErrorContains(t, bErr, "Item too big")
	bErr.WalkMessagesIndexedBy(index, func(i int, m *service.Message, err error) bool {
		if err != nil {
			errs = append(errs, err)
		}
		return true
	})
	require.Len(t, errs, 1, "expected one error in batch error")
	require.ErrorContains(t, errs[0], "Item too big")

	assert.Equal(t, int32(1), watcher.BatchCalls.Load())
	assert.Equal(t, int32(0), watcher.IndividualCalls.Load())

	endpoint := fmt.Sprintf("http://localhost:%v", port)

	conf, err := config.LoadDefaultConfig(context.Background(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"xxxxx",
			"xxxxx",
			"xxxxx"),
		),
		config.WithRegion("us-east-1"),
	)
	require.NoError(t, err)

	conf.BaseEndpoint = &endpoint
	client := dynamodb.NewFromConfig(conf)

	x := dynamodb.ScanInput{
		TableName: aws.String("FooTable"),
	}

	y, err := client.Scan(context.TODO(), &x)
	require.NoError(t, err)

	assert.Equal(t, int32(4), y.Count)
}

func TestHandleParitionKeyTooBig(t *testing.T) {
	integration.CheckSkip(t)

	port := startDynamodbLocal(t, "FooTable")

	db := testDDBOWriterV2(t, fmt.Sprintf(`
table: FooTable
partition_key: id
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

	var batch service.MessageBatch
	for range 4 {
		batch = append(batch, service.NewMessage(fmt.Appendf(nil, `{"id": "%v", "name": "%v"}`, faker.UUIDHyphenated(), faker.Name())))
	}

	batch = append(batch, service.NewMessage(fmt.Appendf(nil, `{"id": "%v", "name": "%v"}`, strings.Repeat("A", 3000), faker.Name())))

	var bErr *service.BatchError
	errs := []error{}

	index := batch.Index()

	writeCtx, writeDone := context.WithTimeout(context.Background(), 5*time.Second)
	defer writeDone()
	err = db.WriteBatch(writeCtx, batch)

	require.ErrorAsf(t, err, &bErr, "expected a batch error but got: %T: %v", bErr, bErr)
	require.ErrorContains(t, bErr, "Parition key too big")
	bErr.WalkMessagesIndexedBy(index, func(i int, m *service.Message, err error) bool {
		if err != nil {
			errs = append(errs, err)
		}
		return true
	})
	require.Len(t, errs, 1, "expected one error in batch error")
	require.ErrorContains(t, errs[0], "Parition key too big")

	assert.Equal(t, int32(1), watcher.BatchCalls.Load())
	assert.Equal(t, int32(0), watcher.IndividualCalls.Load())

	endpoint := fmt.Sprintf("http://localhost:%v", port)

	conf, err := config.LoadDefaultConfig(context.Background(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"xxxxx",
			"xxxxx",
			"xxxxx"),
		),
		config.WithRegion("us-east-1"),
	)
	require.NoError(t, err)

	conf.BaseEndpoint = &endpoint
	client := dynamodb.NewFromConfig(conf)

	x := dynamodb.ScanInput{
		TableName: aws.String("FooTable"),
	}

	y, err := client.Scan(context.TODO(), &x)
	require.NoError(t, err)

	assert.Equal(t, int32(4), y.Count)
}

func TestHandleSortKeyTooBig(t *testing.T) {
	integration.CheckSkip(t)

	// with a sort key
	opts := []bool{true}

	port := startDynamodbLocal(t, "FooTable", opts...)

	db := testDDBOWriterV2(t, fmt.Sprintf(`
table: FooTable
partition_key: id
sort_key: sort
json_map_columns:
  id: id
  name: name
  sort: sort
json_map_datatypes:
  id: S
  name: S
  sort: S
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

	var batch service.MessageBatch
	for range 4 {
		batch = append(batch, service.NewMessage(fmt.Appendf(nil, `{"id": "%v", "sort": "%v", "name": "%v"}`, faker.UUIDHyphenated(), faker.Email(), faker.Name())))
	}

	batch = append(batch, service.NewMessage(fmt.Appendf(nil, `{"id": "%v", "sort": "%v", "name": "%v"}`, faker.UUIDHyphenated(), strings.Repeat("A", 2000), faker.Name())))

	var bErr *service.BatchError
	errs := []error{}

	index := batch.Index()

	writeCtx, writeDone := context.WithTimeout(context.Background(), 5*time.Second)
	defer writeDone()
	err = db.WriteBatch(writeCtx, batch)

	require.ErrorAsf(t, err, &bErr, "expected a batch error but got: %T: %v", bErr, bErr)
	require.ErrorContains(t, bErr, "Sort key too big")
	bErr.WalkMessagesIndexedBy(index, func(i int, m *service.Message, err error) bool {
		if err != nil {
			errs = append(errs, err)
		}
		return true
	})
	require.Len(t, errs, 1, "expected one error in batch error")
	require.ErrorContains(t, errs[0], "Sort key too big")

	assert.Equal(t, int32(1), watcher.BatchCalls.Load())
	assert.Equal(t, int32(0), watcher.IndividualCalls.Load())

	endpoint := fmt.Sprintf("http://localhost:%v", port)

	conf, err := config.LoadDefaultConfig(context.Background(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"xxxxx",
			"xxxxx",
			"xxxxx"),
		),
		config.WithRegion("us-east-1"),
	)
	require.NoError(t, err)

	conf.BaseEndpoint = &endpoint
	client := dynamodb.NewFromConfig(conf)

	x := dynamodb.ScanInput{
		TableName: aws.String("FooTable"),
	}

	y, err := client.Scan(context.TODO(), &x)
	require.NoError(t, err)

	assert.Equal(t, int32(4), y.Count)
}

func TestCheckTableKeySchema(t *testing.T) {
	integration.CheckSkip(t)

	tests := map[string]struct {
		opts         []bool
		partitionKey string
		sortKey      string
		expected     string
	}{
		"No supplied keys skips check": {
			opts:         []bool{},
			partitionKey: "",
			sortKey:      "",
			expected:     "",
		},
		"Check Partition Key does not match": {
			opts:         []bool{},
			partitionKey: "partition_key: id_dne",
			sortKey:      "",
			expected:     "supplied partition_key doesn't match Table schema",
		},
		"Check Sort Key does not match": {
			opts:         []bool{true},
			partitionKey: "partition_key: id",
			sortKey:      "sort_key: sort_dne",
			expected:     "supplied sort_key doesn't match Table schema",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			port := startDynamodbLocal(t, "FooTable", test.opts...)

			db := testDDBOWriterV2(t, fmt.Sprintf(`
table: FooTable
%v
%v
json_map_columns:
  id: id
  name: name
  sort: sort
json_map_datatypes:
  id: S
  name: S
  sort: S
endpoint: http://localhost:%v
region: us-east-1
credentials:
  id: xxxxx
  secret: xxxxx
  token: xxxxx`, test.partitionKey, test.sortKey, port))

			connectCtx, connectDone := context.WithTimeout(context.Background(), 5*time.Second)
			defer connectDone()
			err := db.Connect(connectCtx)
			if test.expected != "" {
				require.ErrorContains(t, err, test.expected)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
