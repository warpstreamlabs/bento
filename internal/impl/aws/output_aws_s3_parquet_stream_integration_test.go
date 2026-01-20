package aws

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

// TestS3ParquetStreamOutput_IntegrationLocalStack tests the streaming Parquet output
// against LocalStack (mock S3) to verify end-to-end functionality without real AWS
func TestS3ParquetStreamOutput_IntegrationLocalStack(t *testing.T) {
	integration.CheckSkip(t)

	// Start LocalStack
	servicePort := GetLocalStack(t, nil)
	bucketName := "test-parquet-bucket"

	// Create bucket
	err := createBucket(context.Background(), servicePort, bucketName)
	require.NoError(t, err)

	testKey := fmt.Sprintf("test-output/test-%d.parquet", time.Now().Unix())

	// Create config
	configYAML := fmt.Sprintf(`
bucket: %s
path: '%s'
force_path_style_urls: true
region: eu-west-1
endpoint: http://localhost:%s
credentials:
  id: xxxxx
  secret: xxxxx
  token: xxxxx
schema:
  - name: id
    type: INT64
  - name: message
    type: UTF8
  - name: timestamp
    type: INT64
default_compression: snappy
row_group_size: 100
`, bucketName, testKey, servicePort)

	parsedConf, err := s3ParquetStreamOutputSpec().ParseYAML(configYAML, nil)
	require.NoError(t, err)

	wConf, err := s3ParquetStreamConfigFromParsed(parsedConf)
	require.NoError(t, err)

	output, err := newS3ParquetStreamOutput(wConf, service.MockResources())
	require.NoError(t, err)

	ctx := context.Background()
	err = output.Connect(ctx)
	require.NoError(t, err)

	// Write test data (250 events = 3 row groups with size 100)
	batch := service.MessageBatch{}
	for i := 0; i < 250; i++ {
		msg := service.NewMessage([]byte("{}"))
		msg.SetStructuredMut(map[string]any{
			"id":        int64(i),
			"message":   fmt.Sprintf("test message %d", i),
			"timestamp": time.Now().Unix(),
		})
		batch = append(batch, msg)
	}

	err = output.WriteBatch(ctx, batch)
	require.NoError(t, err)

	// Close to finalize file
	err = output.Close(ctx)
	require.NoError(t, err)

	// Verify file was created in S3
	s3Client := createLocalS3Client(t, servicePort)

	// Download and verify the file
	getResp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(testKey),
	})
	require.NoError(t, err)
	defer getResp.Body.Close()

	data, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)
	assert.Greater(t, len(data), 100, "File should have content")

	// Verify it's a valid Parquet file
	reader := bytes.NewReader(data)
	parquetFile, err := parquet.OpenFile(reader, int64(len(data)))
	require.NoError(t, err)

	// Verify row count
	assert.Equal(t, int64(250), parquetFile.NumRows(), "Should have 250 rows")

	// Verify row groups (should be 3: 100 + 100 + 50)
	rowGroups := parquetFile.RowGroups()
	assert.Len(t, rowGroups, 3, "Should have 3 row groups")
	assert.Equal(t, int64(100), rowGroups[0].NumRows())
	assert.Equal(t, int64(100), rowGroups[1].NumRows())
	assert.Equal(t, int64(50), rowGroups[2].NumRows())

	// Read and verify first row data is accessible
	rows := parquetFile.RowGroups()[0].Rows()
	defer rows.Close()

	// Read first row
	buffer := make([]parquet.Row, 1)
	n, err := rows.ReadRows(buffer)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "Should read one row")

	// Verify the row has data
	assert.Greater(t, len(buffer[0]), 0, "Row should have values")
}

// TestS3ParquetStreamOutput_IntegrationPartitionBy tests partition_by parameter with LocalStack
func TestS3ParquetStreamOutput_IntegrationPartitionBy(t *testing.T) {
	integration.CheckSkip(t)

	servicePort := GetLocalStack(t, nil)
	bucketName := "test-parquet-partition"

	err := createBucket(context.Background(), servicePort, bucketName)
	require.NoError(t, err)

	testPrefix := fmt.Sprintf("partition-test-%d", time.Now().Unix())

	configYAML := fmt.Sprintf(`
bucket: %s
path: '%s/date=${! meta("date") }/region=${! meta("region") }/data.parquet'
partition_by:
  - '${! meta("date") }'
  - '${! meta("region") }'
force_path_style_urls: true
region: eu-west-1
endpoint: http://localhost:%s
credentials:
  id: xxxxx
  secret: xxxxx
  token: xxxxx
schema:
  - name: id
    type: INT64
  - name: value
    type: UTF8
default_compression: snappy
row_group_size: 50
`, bucketName, testPrefix, servicePort)

	parsedConf, err := s3ParquetStreamOutputSpec().ParseYAML(configYAML, nil)
	require.NoError(t, err)

	wConf, err := s3ParquetStreamConfigFromParsed(parsedConf)
	require.NoError(t, err)

	output, err := newS3ParquetStreamOutput(wConf, service.MockResources())
	require.NoError(t, err)

	ctx := context.Background()
	err = output.Connect(ctx)
	require.NoError(t, err)

	// Write test data with 2 dates × 2 regions = 4 partitions
	batch := service.MessageBatch{}
	dates := []string{"2026-01-20", "2026-01-21"}
	regions := []string{"us-east-1", "eu-west-1"}

	eventID := 0
	for _, date := range dates {
		for _, region := range regions {
			// 25 events per partition
			for i := 0; i < 25; i++ {
				msg := service.NewMessage([]byte("{}"))
				msg.MetaSetMut("date", date)
				msg.MetaSetMut("region", region)
				msg.SetStructuredMut(map[string]any{
					"id":    int64(eventID),
					"value": fmt.Sprintf("%s-%s-%d", date, region, i),
				})
				batch = append(batch, msg)
				eventID++
			}
		}
	}

	err = output.WriteBatch(ctx, batch)
	require.NoError(t, err)

	err = output.Close(ctx)
	require.NoError(t, err)

	// Verify 4 files were created (one per partition)
	s3Client := createLocalS3Client(t, servicePort)
	listResp, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(testPrefix),
	})
	require.NoError(t, err)
	assert.Len(t, listResp.Contents, 4, "Should have 4 files (one per partition)")

	// Verify each partition file has 25 rows
	for _, obj := range listResp.Contents {
		getResp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    obj.Key,
		})
		require.NoError(t, err)

		data, err := io.ReadAll(getResp.Body)
		require.NoError(t, err)
		getResp.Body.Close()

		reader := bytes.NewReader(data)
		parquetFile, err := parquet.OpenFile(reader, int64(len(data)))
		require.NoError(t, err)

		assert.Equal(t, int64(25), parquetFile.NumRows(),
			"Each partition file should have 25 rows: %s", *obj.Key)
	}
}

// createLocalS3Client creates an S3 client configured for LocalStack
func createLocalS3Client(t *testing.T, servicePort string) *s3.Client {
	endpoint := fmt.Sprintf("http://localhost:%s", servicePort)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("eu-west-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					PartitionID:   "aws",
					URL:           endpoint,
					SigningRegion: "eu-west-1",
				}, nil
			})),
	)
	require.NoError(t, err)

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
}
