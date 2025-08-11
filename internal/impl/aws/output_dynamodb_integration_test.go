package aws

import (
	"context"
	"errors"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"

	_ "github.com/warpstreamlabs/bento/public/components/io"
	_ "github.com/warpstreamlabs/bento/public/components/pure"
)

func TestIntegrationDynamoDBOutput(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

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
	builder := service.NewStreamBuilder()

	err = builder.SetYAML(fmt.Sprintf(`
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

	stream, err := builder.Build()
	require.NoError(t, err)

	err = stream.Run(context.Background())
	require.NoError(t, err)

	// let's do a delete + put mixed batch:
	builder = service.NewStreamBuilder()

	err = builder.SetYAML(fmt.Sprintf(`
input:
  csv:
    paths: ["./resources/dynamodbMusicTestDataPutAndDelete.csv"]

output:
  aws_dynamodb:
    table: Music
    json_map_columns:
      uuid: uuid
      title: title
      year: year
    delete:
      condition: |
        root = this.isDelete == "true"
      partition_key: uuid
    endpoint: http://localhost:%v
    region: us-east-1
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
  `, resource.GetPort("8000/tcp")))
	require.NoError(t, err)

	stream, err = builder.Build()
	require.NoError(t, err)

	err = stream.Run(context.Background())
	require.NoError(t, err)

	// let's get the data using the client

	tableName := "Music"
	conf, err := config.LoadDefaultConfig(context.Background(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
		config.WithRegion("us-east-1"),
	)
	require.NoError(t, err)

	endpoint := fmt.Sprintf("http://localhost:%v", resource.GetPort("8000/tcp"))
	conf.BaseEndpoint = &endpoint
	client := dynamodb.NewFromConfig(conf)

	items, err := getAllItems(context.Background(), client, tableName)
	require.NoError(t, err)

	finalDynamoDBData := make(map[string]string)

	for _, item := range items {
		var uuid string
		var title string

		err := attributevalue.Unmarshal(item["uuid"], &uuid)
		require.NoError(t, err)

		err = attributevalue.Unmarshal(item["title"], &title)
		require.NoError(t, err)

		finalDynamoDBData[uuid] = title
	}

	expected := map[string]string{
		"00000000-0000-0000-0000-000000000011": "OK Computer",
		"00000000-0000-0000-0000-000000000006": "Hotel California",
		"00000000-0000-0000-0000-000000000013": "1989",
		"00000000-0000-0000-0000-000000000015": "Lemonade",
		"00000000-0000-0000-0000-000000000008": "Led Zeppelin IV",
		"00000000-0000-0000-0000-000000000017": "AM",
		"00000000-0000-0000-0000-000000000022": "Jagged Little Pill",
		"00000000-0000-0000-0000-000000000016": "To Pimp a Butterfly",
		"00000000-0000-0000-0000-000000000007": "Born in the U.S.A.",
		"00000000-0000-0000-0000-000000000018": "A Rush of Blood to the Head",
		"00000000-0000-0000-0000-000000000012": "The Miseducation of Lauryn Hill",
		"00000000-0000-0000-0000-000000000014": "Good Kid, M.A.A.D City",
		"00000000-0000-0000-0000-000000000023": "Parachutes",
		"00000000-0000-0000-0000-000000000021": "Appetite for Destruction",
		"00000000-0000-0000-0000-000000000005": "Abbey Road",
		"00000000-0000-0000-0000-000000000020": "Bad",
		"00000000-0000-0000-0000-000000000009": "Purple Rain",
		"00000000-0000-0000-0000-000000000019": "Random Access Memories",
		"00000000-0000-0000-0000-000000000010": "Nevermind",
		"00000000-0000-0000-0000-000000000024": "Folklore",
	}

	require.Equal(t, expected, finalDynamoDBData)
}

func createMusicTable(ctx context.Context, t testing.TB, dynamoPort string) error {
	endpoint := fmt.Sprintf("http://localhost:%v", dynamoPort)

	tableName := "Music"

	conf, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
		config.WithRegion("us-east-1"),
	)
	require.NoError(t, err)

	conf.BaseEndpoint = &endpoint
	client := dynamodb.NewFromConfig(conf)

	dt, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &tableName,
	})
	if err != nil {
		var derr *types.ResourceNotFoundException
		if !errors.As(err, &derr) {
			return err
		}
	}

	if dt != nil && dt.Table != nil && dt.Table.TableStatus == types.TableStatusActive {
		return nil
	}

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{AttributeDefinitions: []types.AttributeDefinition{{
		AttributeName: aws.String("uuid"),
		AttributeType: types.ScalarAttributeTypeS,
	}},
		KeySchema: []types.KeySchemaElement{{
			AttributeName: aws.String("uuid"),
			KeyType:       types.KeyTypeHash,
		}},
		TableName:   aws.String(tableName),
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		log.Printf("Couldn't create test table %v. Here's why: %v\n", tableName, err)
	} else {
		waiter := dynamodb.NewTableExistsWaiter(client)
		err = waiter.Wait(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(tableName)}, 5*time.Minute)
		if err != nil {
			log.Printf("Wait for table exists failed. Here's why: %v\n", err)
			return err
		}
		log.Printf("Creating test table %v", tableName)
	}
	return nil
}

func getAllItems(ctx context.Context, client *dynamodb.Client, tableName string) ([]map[string]types.AttributeValue, error) {
	output, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to scan table: %w", err)
	}
	return output.Items, nil
}
