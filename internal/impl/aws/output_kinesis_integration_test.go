package aws

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

func TestKinesisIntegration(t *testing.T) {
	integration.CheckSkip(t)

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	port := GetLocalStack(t, nil)
	endpoint := fmt.Sprintf("http://localhost:%s", port)

	pConf, err := koOutputSpec().ParseYAML(fmt.Sprintf(`
stream: foo
partition_key: ${! json("id") }
region: us-east-1
endpoint: "%v"
credentials:
  id: xxxxxx
  secret: xxxxxx
  token: xxxxxx
`, endpoint), nil)
	require.NoError(t, err)

	conf, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
		config.WithRegion("us-east-1"),
	)
	require.NoError(t, err)
	conf.BaseEndpoint = &endpoint

	// bootstrap kinesis
	client := kinesis.NewFromConfig(conf)

	if _, err := client.CreateStream(context.TODO(), &kinesis.CreateStreamInput{
		ShardCount: aws.Int32(1),
		StreamName: aws.String("foo"),
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	koConf, err := koConfigFromParsed(pConf)
	require.NoError(t, err)

	t.Run("testKinesisConnect", func(t *testing.T) {
		testKinesisConnect(t, koConf, client)
	})

	t.Run("testKinesisConnectWithInvalidStream", func(t *testing.T) {
		koConf.Stream = "invalid-foo"
		testKinesisConnectWithInvalidStream(t, koConf, client)
	})
}

func testKinesisConnect(t *testing.T, c koConfig, client *kinesis.Client) {
	r, err := newKinesisWriter(c, service.MockResources())
	if err != nil {
		t.Fatal(err)
	}

	if err := r.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		require.NoError(t, r.Close(context.Background()))
	}()

	records := [][]byte{
		[]byte(`{"foo":"bar","id":123}`),
		[]byte(`{"foo":"baz","id":456}`),
		[]byte(`{"foo":"qux","id":789}`),
	}

	var msg service.MessageBatch
	for _, record := range records {
		msg = append(msg, service.NewMessage(record))
	}

	if err := r.WriteBatch(context.Background(), msg); err != nil {
		t.Fatal(err)
	}

	iterator, err := client.GetShardIterator(context.TODO(), &kinesis.GetShardIteratorInput{
		ShardId:           aws.String("shardId-000000000000"),
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
		StreamName:        aws.String(c.Stream),
	})
	if err != nil {
		t.Fatal(err)
	}

	out, err := client.GetRecords(context.TODO(), &kinesis.GetRecordsInput{
		Limit:         aws.Int32(10),
		ShardIterator: iterator.ShardIterator,
	})
	if err != nil {
		t.Error(err)
	}
	if act, exp := len(out.Records), len(records); act != exp {
		t.Fatalf("Expected GetRecords response to have records with length of %d, got %d", exp, act)
	}
	for i, record := range records {
		if !bytes.Equal(out.Records[i].Data, record) {
			t.Errorf("Expected record %d to equal %v, got %v", i, record, out.Records[i])
		}
	}
}

func testKinesisConnectWithInvalidStream(t *testing.T, c koConfig, client *kinesis.Client) {
	r, err := newKinesisWriter(c, service.MockResources())
	if err != nil {
		t.Fatal(err)
	}

	retries := 3
	for i := 0; i < retries; i++ {
		err := r.Connect(context.Background())
		assert.Error(t, err)
	}
}
