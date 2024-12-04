//nolint:staticcheck // Ignore SA1019
package aws

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

func TestIntegrationS3Processor(t *testing.T) {
	integration.CheckSkip(t)
	servicePort := getLocalStack(t)
	bucketName := "test-bucket"
	objectKey := "example.txt"
	objectData := "hello world"

	uploadFile(servicePort, bucketName, objectKey, objectData)

	t.Run("s3Processor", func(t *testing.T) {
		s3ProcessorTest(t, bucketName, servicePort)
	})
}

func createS3ProcessorFromYaml(template string) (p processor.V1, err error) {

	conf, err := testutil.ProcessorFromYAML(template)
	if err != nil {
		return nil, err
	}

	p, err = mock.NewManager().NewProcessor(conf)
	if err != nil {
		return nil, err
	}

	return p, nil
}

func s3ProcessorTest(t *testing.T, bucketName string, servicePort string) {
	confStr := fmt.Sprintf(`
aws_s3:
  bucket: %s
  key: example.txt
  force_path_style_urls: true
  region: eu-west-1
  endpoint: http://localhost:%s
  credentials:
    id: xxxxx
    secret: xxxxx
    token: xxxxx
  scanner:
    to_the_end: {}
`, bucketName, servicePort)

	p, err := createS3ProcessorFromYaml(confStr)
	require.NoError(t, err)

	ctx := context.TODO()
	msg := message.QuickBatch([][]byte{[]byte("test message")})
	resBatches, err := p.ProcessBatch(ctx, msg)
	resBatch := resBatches[0]

	expectedMsg := message.QuickBatch([][]byte{[]byte("hello world")})
	var expectedContentLength int64 = 11
	for _, part := range expectedMsg {
		part.MetaSetMut("s3_key", "example.txt")
		part.MetaSetMut("s3_bucket", "test-bucket")
		part.MetaSetMut("s3_content_type", "application/octet-stream")
		part.MetaSetMut("s3_content_length", expectedContentLength)
	}

	for _, part := range resBatch {
		part.MetaDelete("s3_last_modified")
		part.MetaDelete("s3_last_modified_unix")
	}

	require.NoError(t, err)
	assert.Equal(t, expectedMsg.Get(0), resBatch.Get(0))

}

func uploadFile(s3Port string, bucketName string, objectKey string, objectData string) {
	endpoint := fmt.Sprintf("http://localhost:%v", s3Port)

	ctx := context.TODO()

	conf, _ := config.LoadDefaultConfig(ctx,
		config.WithRegion("eu-west-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           endpoint,
				SigningRegion: "eu-west-1",
			}, nil
		})),
	)

	client := s3.NewFromConfig(conf, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	_, _ = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader([]byte(objectData)),
	})
}
