package aws

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/public/service"
)

func testDDBOWriterV2(t *testing.T, conf string) *dynamoDBWriterV2 {
	t.Helper()

	pConf, err := dynamoDBOutputSpecV2().ParseYAML(conf, nil)
	require.NoError(t, err)

	w, err := newDynamoDBWriterV2FromParsed(pConf, service.MockResources())
	require.NoError(t, err)

	return w
}

func TestDynamoDBHappyV2(t *testing.T) {
	db := testDDBOWriterV2(t, `
table: FooTable
json_map_columns:
  id: id
  name: data.name
  age: data.age
json_map_datatypes:
  id: S
  name: S
  age: N
`)

	var request map[string][]types.WriteRequest

	db.client = &mockDynamoDB{
		fn: func(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			t.Error("not expected")
			return nil, errors.New("not implemented")
		},
		batchFn: func(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
			request = input.RequestItems
			return &dynamodb.BatchWriteItemOutput{}, nil
		},
	}

	require.NoError(t, db.WriteBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"1234", "data": {"name":"Penelope", "age": 43}}`)),
		service.NewMessage([]byte(`{"id":"5678", "data": {"name":"Odysseus", "age": 55}}`)),
	}))

	expected := map[string][]types.WriteRequest{
		"FooTable": {
			types.WriteRequest{
				PutRequest: &types.PutRequest{
					Item: map[string]types.AttributeValue{
						"id": &types.AttributeValueMemberS{
							Value: "1234",
						},
						"name": &types.AttributeValueMemberS{
							Value: "Penelope",
						},
						"age": &types.AttributeValueMemberN{
							Value: "43",
						},
					},
				},
			},
			types.WriteRequest{
				PutRequest: &types.PutRequest{
					Item: map[string]types.AttributeValue{
						"id": &types.AttributeValueMemberS{
							Value: "5678",
						},
						"name": &types.AttributeValueMemberS{
							Value: "Odysseus",
						},
						"age": &types.AttributeValueMemberN{
							Value: "55",
						},
					},
				},
			},
		},
	}

	assert.Equal(t, expected, request)
}
