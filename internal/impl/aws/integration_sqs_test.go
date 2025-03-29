package aws

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"

	_ "github.com/warpstreamlabs/bento/internal/impl/pure"
)

func sqsIntegrationSuite(t *testing.T, lsPort string) {
	t.Run("end_to_end", func(t *testing.T) {
		template := `
output:
  aws_sqs:
    url: http://localhost:$PORT/000000000000/queue-$ID
    endpoint: http://localhost:$PORT
    region: eu-west-1
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
    max_in_flight: $MAX_IN_FLIGHT
    batching:
      count: $OUTPUT_BATCH_COUNT

input:
  aws_sqs:
    url: http://localhost:$PORT/000000000000/queue-$ID
    endpoint: http://localhost:$PORT
    region: eu-west-1
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
`
		integration.StreamTests(
			integration.StreamTestOpenClose(),
			integration.StreamTestSendBatch(10),
			integration.StreamTestStreamSequential(50),
			integration.StreamTestStreamParallel(50),
			integration.StreamTestStreamParallelLossy(50),
			integration.StreamTestStreamParallelLossyThroughReconnect(50),
		).Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				require.NoError(t, createBucketQueue(ctx, "", lsPort, vars.ID))
			}),
			integration.StreamTestOptPort(lsPort),
		)
	})

	t.Run("custom_headers", func(t *testing.T) {
		sqsIntegrationCustomHeaders(t, lsPort)
	})

	t.Run("attributes_metadata", func(t *testing.T) {
		sqsIntegrationTestAttributesMetadata(t, lsPort)
	})

}

func sqsIntegrationCustomHeaders(t *testing.T, lsPort string) {
	// NOTE: LocalStack allows for overriding how many SQS messages are sent/receieved in a single
	// SQS call using the custom 'x-localstack-sqs-override-message-count' header.
	template := `
output:
  aws_sqs:
    url: http://localhost:$PORT/000000000000/queue-$ID
    endpoint: http://localhost:$PORT
    region: eu-west-1
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
    max_in_flight: $MAX_IN_FLIGHT
    batching:
      count: $OUTPUT_BATCH_COUNT

input:
  aws_sqs:
    url: http://localhost:$PORT/000000000000/queue-$ID
    endpoint: http://localhost:$PORT
    region: eu-west-1
    custom_request_headers:
      x-localstack-sqs-override-message-count: $INPUT_BATCH_COUNT
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
`
	integration.StreamTests(
		integration.StreamTestSendBatch(50),        // Send 50
		integration.StreamTestStreamSequential(50), // Send 50
		integration.StreamTestReceiveBatchCount(1), // Receive 1
	).Run(
		t, template,
		integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
			require.NoError(t, createBucketQueue(ctx, "", lsPort, vars.ID))
		}),
		integration.StreamTestOptPort(lsPort),
	)
}

func sqsIntegrationTestAttributesMetadata(t *testing.T, lsPort string) {
	var sqsClient *sqs.Client

	conf, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
		config.WithRegion("eu-west-1"),
	)
	require.NoError(t, err)

	sqsQueueName := "queue-" + uuid.NewString()[:8]
	sqsEndpoint := fmt.Sprintf("http://localhost:%v", lsPort)

	conf.BaseEndpoint = &sqsEndpoint
	sqsClient = sqs.NewFromConfig(conf)

	response, err := sqsClient.CreateQueue(context.TODO(), &sqs.CreateQueueInput{
		QueueName: aws.String(sqsQueueName),
	})
	require.NoError(t, err)
	// sqsQueueUrl := fmt.Sprintf("http://localhost:%s/000000000000/%s", lsPort, sqsQueueName)

	template := fmt.Sprintf(`
aws_sqs:
  url: %s
  endpoint: %s
  region: eu-west-1
  credentials:
    id: xxxxx
    secret: xxxxx
    token: xxxxx
`, *response.QueueUrl, sqsEndpoint)

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	sqsMsgOutCh := make(chan service.MessageBatch)
	defer close(sqsMsgOutCh)

	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(ctx context.Context, mb service.MessageBatch) error {
		sqsMsgOutCh <- mb.DeepCopy()
		return nil
	}))

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)

	if _, err := sqsClient.SendMessage(context.Background(), &sqs.SendMessageInput{
		MessageBody: aws.String("hello world!"),
		QueueUrl:    response.QueueUrl,
		MessageAttributes: map[string]types.MessageAttributeValue{
			"CustomerInfo": {
				DataType:    aws.String("String"),
				StringValue: aws.String("John Doe, Premium Customer"),
			},
			"TransactionAmount": {
				DataType:    aws.String("Number"),
				StringValue: aws.String("325.65"),
			},
			"Priority": {
				DataType:    aws.String("Number.int"),
				StringValue: aws.String("1"),
			},
			"MetadataInfo": {
				DataType:    aws.String("String.JSON"),
				StringValue: aws.String("{\"region\":\"west\",\"priority\":\"high\",\"tags\":[\"urgent\",\"review\"]}"),
			},
		},
		MessageSystemAttributes: map[string]types.MessageSystemAttributeValue{
			"AWSTraceHeader": {
				DataType:    aws.String("String"),
				StringValue: aws.String("Root=1-5f79c0ab-e42f32e8d29c38571712f1de"),
			},
		},
	}); err != nil {
		t.Error(err)
	}

	go func() {
		_ = streamOut.Run(context.Background())
	}()

	var msg *service.Message
	select {
	case batch, open := <-sqsMsgOutCh:
		require.True(t, open, "Expected queue to be open")
		require.NotNil(t, batch, "Expected message not to be nil")
		require.Len(t, batch, 1)
		msg = batch[0]
	case <-time.After(10 * time.Second):
		t.Fatal("Timed out waiting for SQS message after 5 seconds")
	}

	require.NoError(t, streamOut.StopWithin(time.Second*10), "Expected stream to stop within 10s")

	data, ok := msg.MetaGetMut("CustomerInfo")
	require.True(t, ok, "Expected CustomerInfo metadata to exist")
	require.Equal(t, "John Doe, Premium Customer", data)

	data, ok = msg.MetaGetMut("TransactionAmount")
	require.True(t, ok, "Expected TransactionAmount metadata to exist")
	require.Equal(t, "325.65", data)

	data, ok = msg.MetaGetMut("Priority")
	require.True(t, ok, "Expected Priority metadata to exist")
	require.Equal(t, "1", data)

	data, ok = msg.MetaGetMut("MetadataInfo")
	require.True(t, ok, "Expected MetadataInfo metadata to exist")
	require.Equal(t, "{\"region\":\"west\",\"priority\":\"high\",\"tags\":[\"urgent\",\"review\"]}", data)

	data, ok = msg.MetaGetMut("AWSTraceHeader")
	require.True(t, ok, "Expected AWSTraceHeader to exist")
	require.Equal(t, "Root=1-5f79c0ab-e42f32e8d29c38571712f1de", data, "Incorrect AWSTraceHeader value")

	msgBody, err := msg.AsBytes()
	require.NoError(t, err)
	require.Equal(t, "hello world!", string(msgBody))
}
