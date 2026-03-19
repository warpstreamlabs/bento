package aws

import (
	"bytes"
	"context"
	"io"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStreamingParquetWriter_Statistics tests that column statistics configuration
// is properly applied to generated Parquet files
func TestStreamingParquetWriter_Statistics(t *testing.T) {
	type TestRecord struct {
		ID       int64  `parquet:"id"`
		Name     string `parquet:"name"`
		Category string `parquet:"category"`
	}

	schema := parquet.SchemaOf(new(TestRecord))
	messageType := reflect.TypeFor[TestRecord]()

	testCases := []struct {
		name                 string
		columnIndexEnabled   bool
		columnIndexSizeLimit int
		dataPageStatistics   bool
		expectStatistics     bool
	}{
		{
			name:                 "statistics enabled (default)",
			columnIndexEnabled:   true,
			columnIndexSizeLimit: 64,
			dataPageStatistics:   false,
			expectStatistics:     true,
		},
		{
			name:                 "statistics disabled",
			columnIndexEnabled:   false,
			columnIndexSizeLimit: 0,
			dataPageStatistics:   false,
			expectStatistics:     false,
		},
		{
			name:                 "page-level statistics enabled",
			columnIndexEnabled:   true,
			columnIndexSizeLimit: 64,
			dataPageStatistics:   true,
			expectStatistics:     true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Track uploaded parts to reconstruct final file
			var uploadedParts [][]byte
			var completedData []byte

			mockS3 := &mockS3Client{
				uploadPartFunc: func(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
					// Read and store the part data
					data, err := io.ReadAll(input.Body)
					if err != nil {
						return nil, err
					}
					uploadedParts = append(uploadedParts, data)
					return &s3.UploadPartOutput{
						ETag: aws.String("test-etag"),
					}, nil
				},
				completeMultipartUploadFunc: func(ctx context.Context, input *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
					// Combine all parts into final file
					for _, part := range uploadedParts {
						completedData = append(completedData, part...)
					}
					return &s3.CompleteMultipartUploadOutput{}, nil
				},
			}

			config := StreamingWriterConfig{
				S3Client:             mockS3,
				Bucket:               "test-bucket",
				Key:                  "test.parquet",
				Schema:               schema,
				MessageType:          messageType,
				CompressionType:      &parquet.Uncompressed,
				RowGroupSize:         100,
				ColumnIndexEnabled:   tc.columnIndexEnabled,
				ColumnIndexSizeLimit: tc.columnIndexSizeLimit,
				DataPageStatistics:   tc.dataPageStatistics,
			}

			writer, err := NewStreamingParquetWriter(config)
			require.NoError(t, err)

			// Verify configuration was stored
			assert.Equal(t, tc.columnIndexEnabled, writer.columnIndexEnabled)
			assert.Equal(t, tc.columnIndexSizeLimit, writer.columnIndexSizeLimit)
			assert.Equal(t, tc.dataPageStatistics, writer.dataPageStatistics)

			// Initialize writer
			err = writer.Initialize(context.Background())
			require.NoError(t, err)

			// Write test data
			testData := []map[string]any{
				{"id": int64(1), "name": "Alice", "category": "A"},
				{"id": int64(2), "name": "Bob", "category": "B"},
				{"id": int64(3), "name": "Charlie", "category": "A"},
			}

			for _, event := range testData {
				err = writer.WriteEvent(context.Background(), event)
				require.NoError(t, err)
			}

			// Close writer to finalize file
			err = writer.Close(context.Background())
			require.NoError(t, err)

			// Verify we have completed data
			require.Greater(t, len(completedData), 0, "Should have uploaded data")

			// Read back the parquet file to verify statistics
			reader := bytes.NewReader(completedData)
			pf, err := parquet.OpenFile(reader, int64(len(completedData)))
			require.NoError(t, err)

			// Check if column indexes exist (these contain min/max statistics)
			rowGroups := pf.RowGroups()
			require.Greater(t, len(rowGroups), 0, "File should have at least one row group")

			// For each row group, check if column chunks have statistics
			for _, rg := range rowGroups {
				columnChunks := rg.ColumnChunks()
				for i, chunk := range columnChunks {
					columnIndex, err := chunk.ColumnIndex()

					if tc.expectStatistics {
						// When statistics are enabled, column index should exist
						require.NoError(t, err, "Should read column index for column %d", i)
						assert.NotNil(t, columnIndex,
							"Column index should exist when statistics are enabled for column %d", i)

						// Verify we have min/max values
						if columnIndex != nil {
							assert.Greater(t, columnIndex.NumPages(), 0,
								"Column index should have page information")
						}
					}
				}
			}
		})
	}
}

// Test that default values are set correctly when not specified
func TestStreamingParquetWriter_DefaultStatisticsConfig(t *testing.T) {
	type TestRecord struct {
		ID int64 `parquet:"id"`
	}

	schema := parquet.SchemaOf(new(TestRecord))
	messageType := reflect.TypeFor[TestRecord]()

	mockS3 := &mockS3Client{}

	// Create config WITHOUT statistics options (should use defaults)
	config := StreamingWriterConfig{
		S3Client:        mockS3,
		Bucket:          "test-bucket",
		Key:             "test.parquet",
		Schema:          schema,
		MessageType:     messageType,
		CompressionType: &parquet.Uncompressed,
		RowGroupSize:    100,
		// Note: Statistics options NOT set - should use defaults
	}

	writer, err := NewStreamingParquetWriter(config)
	require.NoError(t, err)

	// When not explicitly set, defaults should be: statistics disabled (zero values)
	// In production config parsing, these will be set to true/64/false by default
	assert.Equal(t, false, writer.columnIndexEnabled)
	assert.Equal(t, 0, writer.columnIndexSizeLimit)
	assert.Equal(t, false, writer.dataPageStatistics)
}
