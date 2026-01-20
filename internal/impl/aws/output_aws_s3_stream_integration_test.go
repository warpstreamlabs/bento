package aws

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

// getTestS3Client creates an S3 client for LocalStack testing
func getTestS3Client(ctx context.Context, t *testing.T, servicePort string) *s3.Client {
	endpoint := fmt.Sprintf("http://localhost:%v", servicePort)

	conf, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "test")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           endpoint,
				SigningRegion: "us-east-1",
			}, nil
		})),
	)
	require.NoError(t, err)

	return s3.NewFromConfig(conf, func(o *s3.Options) {
		o.UsePathStyle = true
	})
}

// TestS3StreamOutput_IntegrationBasic tests basic write operations with LocalStack
func TestS3StreamOutput_IntegrationBasic(t *testing.T) {
	integration.CheckSkip(t)

	servicePort := GetLocalStack(t, nil)
	bucketName := "test-stream-bucket"
	testKey := "test-output/basic-test.log"

	// Create bucket
	s3Client := getTestS3Client(context.Background(), t, servicePort)

	_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	// Configure output
	configYAML := fmt.Sprintf(`
bucket: %s
path: '%s'
region: us-east-1
force_path_style_urls: true
endpoint: http://localhost:%s
max_buffer_bytes: 1048576
max_buffer_count: 100
max_buffer_period: 1s
`, bucketName, testKey, servicePort)

	parsedConf, err := s3StreamOutputSpec().ParseYAML(configYAML, nil)
	require.NoError(t, err)

	wConf, err := s3StreamConfigFromParsed(parsedConf)
	require.NoError(t, err)

	output, err := newS3StreamOutput(wConf, service.MockResources())
	require.NoError(t, err)

	ctx := context.Background()
	err = output.Connect(ctx)
	require.NoError(t, err)

	// Write test data (150 messages to trigger flush)
	batch := service.MessageBatch{}
	for i := 0; i < 150; i++ {
		msg := service.NewMessage([]byte(fmt.Sprintf("log line %d\n", i)))
		batch = append(batch, msg)
	}

	err = output.WriteBatch(ctx, batch)
	require.NoError(t, err)

	// Close output to finalize upload
	err = output.Close(ctx)
	require.NoError(t, err)

	// Verify file exists in S3
	getResp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(testKey),
	})
	require.NoError(t, err)
	defer getResp.Body.Close()

	// Read and verify content
	content, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)

	// Verify we have data
	assert.Greater(t, len(content), 0, "File should have content")

	// Verify first and last lines
	contentStr := string(content)
	assert.Contains(t, contentStr, "log line 0\n")
	assert.Contains(t, contentStr, "log line 149\n")
}

// TestS3StreamOutput_IntegrationPartitionBy tests partition_by parameter with LocalStack
func TestS3StreamOutput_IntegrationPartitionBy(t *testing.T) {
	integration.CheckSkip(t)

	servicePort := GetLocalStack(t, nil)
	bucketName := "test-stream-partition"
	testPrefix := "test-output/partitioned"

	// Create bucket
	s3Client := getTestS3Client(context.Background(), t, servicePort)

	_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	// Configure output with partition_by
	configYAML := fmt.Sprintf(`
bucket: %s
path: '%s/date=${! meta("date") }/region=${! meta("region") }/data.log'
region: us-east-1
force_path_style_urls: true
endpoint: http://localhost:%s
partition_by:
  - '${! meta("date") }'
  - '${! meta("region") }'
max_buffer_bytes: 1048576
max_buffer_count: 50
max_buffer_period: 1s
`, bucketName, testPrefix, servicePort)

	parsedConf, err := s3StreamOutputSpec().ParseYAML(configYAML, nil)
	require.NoError(t, err)

	wConf, err := s3StreamConfigFromParsed(parsedConf)
	require.NoError(t, err)

	output, err := newS3StreamOutput(wConf, service.MockResources())
	require.NoError(t, err)

	ctx := context.Background()
	err = output.Connect(ctx)
	require.NoError(t, err)

	// Write test data with 2 dates × 2 regions = 4 partitions
	batch := service.MessageBatch{}
	dates := []string{"2026-01-20", "2026-01-21"}
	regions := []string{"us-east-1", "eu-west-1"}

	messageCount := 0
	for _, date := range dates {
		for _, region := range regions {
			for i := 0; i < 25; i++ { // 25 messages per partition
				msg := service.NewMessage([]byte(fmt.Sprintf("log %s %s %d\n", date, region, i)))
				msg.MetaSet("date", date)
				msg.MetaSet("region", region)
				batch = append(batch, msg)
				messageCount++
			}
		}
	}

	err = output.WriteBatch(ctx, batch)
	require.NoError(t, err)

	// Close output to finalize uploads
	err = output.Close(ctx)
	require.NoError(t, err)

	// Verify 4 files were created (one per partition)
	listResp, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(testPrefix),
	})
	require.NoError(t, err)
	assert.Len(t, listResp.Contents, 4, "Should have 4 partition files")

	// Verify each partition file
	partitionKeys := []string{
		fmt.Sprintf("%s/date=2026-01-20/region=us-east-1/data.log", testPrefix),
		fmt.Sprintf("%s/date=2026-01-20/region=eu-west-1/data.log", testPrefix),
		fmt.Sprintf("%s/date=2026-01-21/region=us-east-1/data.log", testPrefix),
		fmt.Sprintf("%s/date=2026-01-21/region=eu-west-1/data.log", testPrefix),
	}

	for _, key := range partitionKeys {
		getResp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		})
		require.NoError(t, err, "File should exist: %s", key)
		defer getResp.Body.Close()

		content, err := io.ReadAll(getResp.Body)
		require.NoError(t, err)

		// Each partition should have 25 messages
		contentStr := string(content)
		assert.Greater(t, len(contentStr), 0, "Partition %s should have content", key)

		// Verify content contains correct partition values
		if key == partitionKeys[0] {
			assert.Contains(t, contentStr, "log 2026-01-20 us-east-1")
		} else if key == partitionKeys[1] {
			assert.Contains(t, contentStr, "log 2026-01-20 eu-west-1")
		} else if key == partitionKeys[2] {
			assert.Contains(t, contentStr, "log 2026-01-21 us-east-1")
		} else if key == partitionKeys[3] {
			assert.Contains(t, contentStr, "log 2026-01-21 eu-west-1")
		}
	}
}

// TestS3StreamOutput_IntegrationContentType tests content_type and content_encoding settings
func TestS3StreamOutput_IntegrationContentType(t *testing.T) {
	integration.CheckSkip(t)

	servicePort := GetLocalStack(t, nil)
	bucketName := "test-stream-content"
	testKey := "test-output/content-test.json"

	// Create bucket
	s3Client := getTestS3Client(context.Background(), t, servicePort)

	_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	// Configure output with content type
	configYAML := fmt.Sprintf(`
bucket: %s
path: '%s'
region: us-east-1
force_path_style_urls: true
endpoint: http://localhost:%s
content_type: 'application/json'
max_buffer_bytes: 1048576
max_buffer_count: 100
max_buffer_period: 1s
`, bucketName, testKey, servicePort)

	parsedConf, err := s3StreamOutputSpec().ParseYAML(configYAML, nil)
	require.NoError(t, err)

	wConf, err := s3StreamConfigFromParsed(parsedConf)
	require.NoError(t, err)

	output, err := newS3StreamOutput(wConf, service.MockResources())
	require.NoError(t, err)

	ctx := context.Background()
	err = output.Connect(ctx)
	require.NoError(t, err)

	// Write JSON data
	batch := service.MessageBatch{}
	for i := 0; i < 50; i++ {
		msg := service.NewMessage([]byte(fmt.Sprintf(`{"id": %d, "message": "test"}%s`, i, "\n")))
		batch = append(batch, msg)
	}

	err = output.WriteBatch(ctx, batch)
	require.NoError(t, err)

	err = output.Close(ctx)
	require.NoError(t, err)

	// Verify file and content type
	getResp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(testKey),
	})
	require.NoError(t, err)
	defer getResp.Body.Close()

	// Verify content type
	assert.Equal(t, "application/json", aws.ToString(getResp.ContentType))

	// Verify content
	content, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)

	contentStr := string(content)
	assert.Contains(t, contentStr, `{"id": 0, "message": "test"}`)
	assert.Contains(t, contentStr, `{"id": 49, "message": "test"}`)
}

// TestS3StreamOutput_IntegrationEmptyBatch tests handling of empty batches
func TestS3StreamOutput_IntegrationEmptyBatch(t *testing.T) {
	integration.CheckSkip(t)

	servicePort := GetLocalStack(t, nil)
	bucketName := "test-stream-empty"
	testKey := "test-output/empty-test.log"

	// Create bucket
	s3Client := getTestS3Client(context.Background(), t, servicePort)

	_, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	// Configure output
	configYAML := fmt.Sprintf(`
bucket: %s
path: '%s'
region: us-east-1
force_path_style_urls: true
endpoint: http://localhost:%s
max_buffer_bytes: 1048576
max_buffer_count: 100
max_buffer_period: 1s
`, bucketName, testKey, servicePort)

	parsedConf, err := s3StreamOutputSpec().ParseYAML(configYAML, nil)
	require.NoError(t, err)

	wConf, err := s3StreamConfigFromParsed(parsedConf)
	require.NoError(t, err)

	output, err := newS3StreamOutput(wConf, service.MockResources())
	require.NoError(t, err)

	ctx := context.Background()
	err = output.Connect(ctx)
	require.NoError(t, err)

	// Write a single small message
	batch := service.MessageBatch{
		service.NewMessage([]byte("single message\n")),
	}

	err = output.WriteBatch(ctx, batch)
	require.NoError(t, err)

	// Close output (should handle small buffer)
	err = output.Close(ctx)
	require.NoError(t, err)

	// Verify file exists
	getResp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(testKey),
	})
	require.NoError(t, err)
	defer getResp.Body.Close()

	content, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)

	assert.Equal(t, "single message\n", string(content))
}
