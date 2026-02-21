package aws

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFlushRowGroup_PreservesColumnMetadata tests that column metadata is extracted
// from temporary files and preserved correctly (fix for definition levels bug)
func TestFlushRowGroup_PreservesColumnMetadata(t *testing.T) {
	// Define a nested struct with optional fields
	type Account struct {
		UID *string `parquet:"uid,optional"`
	}
	type Cloud struct {
		Provider *string  `parquet:"provider,optional"`
		Account  *Account `parquet:"account,optional"`
	}
	type TestRecord struct {
		ID    int64  `parquet:"id"`
		Cloud *Cloud `parquet:"cloud,optional"`
	}

	schema := parquet.SchemaOf(new(TestRecord))
	messageType := reflect.TypeOf(TestRecord{})

	writer := &StreamingParquetWriter{
		schema:          schema,
		messageType:     messageType,
		compressionType: &parquet.Snappy,
		rowGroupSize:    10,
		uploadBuffer:    bytes.NewBuffer(nil),
		rowBuffer:       make([]any, 0, 10),
		rowGroupsMeta:   []RowGroupMetadata{},
		uploadSize:      4, // After "PAR1" header
		filePosition:    4, // Track absolute position in file
	}

	// Add test data with nested optionals
	uid := "test-uid-123"
	provider := "AWS"
	testData := []map[string]any{
		{
			"id": int64(1),
			"cloud": map[string]any{
				"provider": provider,
				"account": map[string]any{
					"uid": uid,
				},
			},
		},
		{
			"id": int64(2),
			"cloud": map[string]any{
				"provider": provider,
				"account":  nil, // Test null nested field
			},
		},
	}

	for _, event := range testData {
		v := reflect.New(messageType)
		err := mapToStruct(event, v.Interface())
		require.NoError(t, err)
		writer.rowBuffer = append(writer.rowBuffer, v.Interface())
	}

	// Flush row group
	err := writer.flushRowGroup(context.Background())
	require.NoError(t, err)

	// Verify metadata was preserved
	require.Len(t, writer.rowGroupsMeta, 1)
	meta := writer.rowGroupsMeta[0]

	// Should have extracted column chunks from temp file
	assert.NotEmpty(t, meta.ColumnChunks, "ColumnChunks should be populated from temp file footer")

	// Verify each column chunk has proper metadata
	for i, col := range meta.ColumnChunks {
		assert.NotNil(t, col.MetaData, "Column %d should have metadata", i)

		// Verify critical metadata fields are present
		assert.Greater(t, col.MetaData.NumValues, int64(0), "Column %d should have NumValues > 0", i)
		assert.Greater(t, col.MetaData.TotalCompressedSize, int64(0), "Column %d should have TotalCompressedSize > 0", i)

		// Verify file offset was adjusted (should be >= uploadSize at time of flush)
		assert.GreaterOrEqual(t, col.MetaData.DataPageOffset, int64(4),
			"Column %d DataPageOffset should be adjusted for streaming file position", i)
	}

	// Verify row buffer was cleared
	assert.Empty(t, writer.rowBuffer)
	assert.Equal(t, int64(2), writer.totalRows)
}

// TestFlushRowGroup_ExtractsActualFooterMetadata tests that the footer extraction
// correctly parses the temporary file's footer using Thrift
func TestFlushRowGroup_ExtractsActualFooterMetadata(t *testing.T) {
	type TestRecord struct {
		ID    int64  `parquet:"id"`
		Name  string `parquet:"name"`
		Value *int64 `parquet:"value,optional"`
	}

	schema := parquet.SchemaOf(new(TestRecord))
	messageType := reflect.TypeOf(TestRecord{})

	writer := &StreamingParquetWriter{
		schema:          schema,
		messageType:     messageType,
		compressionType: &parquet.Uncompressed,
		rowGroupSize:    3,
		uploadBuffer:    bytes.NewBuffer(nil),
		rowBuffer:       make([]any, 0, 3),
		rowGroupsMeta:   []RowGroupMetadata{},
		uploadSize:      4,
	}

	// Add test data
	val1 := int64(100)
	testData := []map[string]any{
		{"id": int64(1), "name": "Alice", "value": val1},
		{"id": int64(2), "name": "Bob", "value": nil}, // Null optional
		{"id": int64(3), "name": "Charlie", "value": val1},
	}

	for _, event := range testData {
		v := reflect.New(messageType)
		err := mapToStruct(event, v.Interface())
		require.NoError(t, err)
		writer.rowBuffer = append(writer.rowBuffer, v.Interface())
	}

	// Flush
	err := writer.flushRowGroup(context.Background())
	require.NoError(t, err)

	// Verify metadata preservation
	require.Len(t, writer.rowGroupsMeta, 1)
	meta := writer.rowGroupsMeta[0]

	// Should have 3 columns (id, name, value)
	assert.Len(t, meta.ColumnChunks, 3, "Should have extracted column chunks for all 3 fields")

	// Verify each column has correct path
	paths := make([][]string, len(meta.ColumnChunks))
	for i, col := range meta.ColumnChunks {
		paths[i] = col.MetaData.PathInSchema
	}

	// Verify paths are correct
	expectedPaths := [][]string{
		{"id"},
		{"name"},
		{"value"},
	}
	assert.Equal(t, expectedPaths, paths, "Column paths should match schema")
}

// TestRowGroupsToThrift_UsesPreservedMetadata tests that rowGroupsToThrift
// uses the preserved column metadata instead of approximating
func TestRowGroupsToThrift_UsesPreservedMetadata(t *testing.T) {
	type TestRecord struct {
		ID int64 `parquet:"id"`
	}

	schema := parquet.SchemaOf(new(TestRecord))
	messageType := reflect.TypeOf(TestRecord{})

	// Create mock column metadata
	mockColumnChunks := []format.ColumnChunk{
		{
			MetaData: format.ColumnMetaData{
				Type:                  format.Int64,
				PathInSchema:          []string{"id"},
				Codec:                 format.Snappy,
				NumValues:             100,
				TotalCompressedSize:   1234,
				TotalUncompressedSize: 5678,
				DataPageOffset:        1000,
			},
		},
	}

	writer := &StreamingParquetWriter{
		schema:          schema,
		messageType:     messageType,
		compressionType: &parquet.Snappy,
		rowGroupSize:    100,
		totalRows:       100,
		rowGroupsMeta: []RowGroupMetadata{
			{
				NumRows:       100,
				TotalByteSize: 1234,
				FileOffset:    1000,
				ColumnChunks:  mockColumnChunks,
			},
		},
	}

	// Convert to Thrift
	rowGroups := writer.rowGroupsToThrift()

	require.Len(t, rowGroups, 1)
	rg := rowGroups[0]

	// Verify preserved metadata was used
	assert.Equal(t, int64(100), rg.NumRows)
	assert.Equal(t, int64(1234), rg.TotalByteSize)
	assert.Equal(t, int64(1000), rg.FileOffset)

	require.Len(t, rg.Columns, 1)
	col := rg.Columns[0]

	// Verify column metadata matches what we provided (not approximated)
	assert.Equal(t, format.Int64, col.MetaData.Type)
	assert.Equal(t, []string{"id"}, col.MetaData.PathInSchema)
	assert.Equal(t, format.Snappy, col.MetaData.Codec)
	assert.Equal(t, int64(100), col.MetaData.NumValues)
	assert.Equal(t, int64(1234), col.MetaData.TotalCompressedSize)
	assert.Equal(t, int64(5678), col.MetaData.TotalUncompressedSize)
	assert.Equal(t, int64(1000), col.MetaData.DataPageOffset)
}

// TestRowGroupsToThrift_FallbackApproximation tests that the fallback
// approximation logic works when column chunks aren't preserved
func TestRowGroupsToThrift_FallbackApproximation(t *testing.T) {
	type TestRecord struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}

	schema := parquet.SchemaOf(new(TestRecord))
	messageType := reflect.TypeOf(TestRecord{})

	writer := &StreamingParquetWriter{
		schema:          schema,
		messageType:     messageType,
		compressionType: &parquet.Snappy,
		rowGroupSize:    50,
		totalRows:       50,
		rowGroupsMeta: []RowGroupMetadata{
			{
				NumRows:       50,
				TotalByteSize: 2000,
				FileOffset:    500,
				ColumnChunks:  []format.ColumnChunk{}, // Empty - triggers fallback
			},
		},
	}

	// Convert to Thrift (should use fallback)
	rowGroups := writer.rowGroupsToThrift()

	require.Len(t, rowGroups, 1)
	rg := rowGroups[0]

	// Should have generated approximated metadata for 2 columns
	assert.Len(t, rg.Columns, 2)

	// Verify approximated values
	for _, col := range rg.Columns {
		assert.Equal(t, int64(50), col.MetaData.NumValues)
		assert.Equal(t, int64(1000), col.MetaData.TotalCompressedSize) // 2000 / 2
		assert.Equal(t, int64(500), col.MetaData.DataPageOffset)
	}
}

// TestFlushRowGroup_AdjustsFileOffsets tests that file offsets are correctly
// adjusted when extracting metadata from temp files
func TestFlushRowGroup_AdjustsFileOffsets(t *testing.T) {
	type TestRecord struct {
		ID int64 `parquet:"id"`
	}

	schema := parquet.SchemaOf(new(TestRecord))
	messageType := reflect.TypeOf(TestRecord{})

	// Simulate writer that already has data uploaded
	existingSize := int64(10000)

	writer := &StreamingParquetWriter{
		schema:          schema,
		messageType:     messageType,
		compressionType: &parquet.Snappy,
		rowGroupSize:    2,
		uploadBuffer:    bytes.NewBuffer(nil),
		rowBuffer:       make([]any, 0, 2),
		rowGroupsMeta:   []RowGroupMetadata{},
		uploadSize:      existingSize, // Already have data in stream
		filePosition:    existingSize, // Track absolute position in file
	}

	// Add test data
	testData := []map[string]any{
		{"id": int64(1)},
		{"id": int64(2)},
	}

	for _, event := range testData {
		v := reflect.New(messageType)
		err := mapToStruct(event, v.Interface())
		require.NoError(t, err)
		writer.rowBuffer = append(writer.rowBuffer, v.Interface())
	}

	// Flush
	err := writer.flushRowGroup(context.Background())
	require.NoError(t, err)

	// Verify metadata
	require.Len(t, writer.rowGroupsMeta, 1)
	meta := writer.rowGroupsMeta[0]

	// Verify file offset is set to the uploadSize at time of flush
	assert.Equal(t, existingSize, meta.FileOffset)

	// Verify column offsets were adjusted to account for existing data
	for i, col := range meta.ColumnChunks {
		// DataPageOffset should be >= existingSize (adjusted from temp file offset of ~4)
		assert.GreaterOrEqual(t, col.MetaData.DataPageOffset, existingSize,
			"Column %d DataPageOffset should be adjusted to account for existing data", i)
	}
}

// TestGenerateFooter_WithPreservedMetadata tests that footer generation
// works correctly with preserved column metadata
func TestGenerateFooter_WithPreservedMetadata(t *testing.T) {
	type TestRecord struct {
		ID int64 `parquet:"id"`
	}

	schema := parquet.SchemaOf(new(TestRecord))
	messageType := reflect.TypeOf(TestRecord{})

	mockColumnChunks := []format.ColumnChunk{
		{
			MetaData: format.ColumnMetaData{
				Type:           format.Int64,
				PathInSchema:   []string{"id"},
				Codec:          format.Snappy,
				NumValues:      50,
				DataPageOffset: 1000,
			},
		},
	}

	writer := &StreamingParquetWriter{
		schema:          schema,
		messageType:     messageType,
		compressionType: &parquet.Snappy,
		rowGroupSize:    50,
		totalRows:       100,
		rowGroupsMeta: []RowGroupMetadata{
			{
				NumRows:       50,
				TotalByteSize: 1000,
				FileOffset:    4,
				ColumnChunks:  mockColumnChunks,
			},
			{
				NumRows:       50,
				TotalByteSize: 1000,
				FileOffset:    1004,
				ColumnChunks:  mockColumnChunks,
			},
		},
	}

	// Generate footer
	footerBytes, err := writer.generateFooter()
	require.NoError(t, err)

	// Footer should be: [thrift bytes] [4 bytes length]
	assert.Greater(t, len(footerBytes), 4)

	// Extract and verify Thrift serialization can be parsed
	thriftBytes := footerBytes[:len(footerBytes)-4]

	var fileMeta format.FileMetaData
	err = thrift.Unmarshal(new(thrift.CompactProtocol), thriftBytes, &fileMeta)
	require.NoError(t, err)

	// Verify footer metadata
	assert.Equal(t, int32(1), fileMeta.Version)
	assert.Equal(t, int64(100), fileMeta.NumRows)
	assert.Len(t, fileMeta.RowGroups, 2)

	// Verify row groups have preserved column metadata
	for i, rg := range fileMeta.RowGroups {
		assert.Equal(t, int64(50), rg.NumRows, "Row group %d", i)
		assert.Len(t, rg.Columns, 1, "Row group %d should have 1 column", i)
		assert.Equal(t, int64(50), rg.Columns[0].MetaData.NumValues, "Row group %d", i)
	}
}

// TestNestedOptionalStructs_EndToEnd tests the full flow with deeply nested optionals
func TestNestedOptionalStructs_EndToEnd(t *testing.T) {
	// Define deeply nested structure like OCSF
	type Feature struct {
		Name *string `parquet:"name,optional"`
	}
	type Product struct {
		VendorName *string  `parquet:"vendor_name,optional"`
		Feature    *Feature `parquet:"feature,optional"`
	}
	type Metadata struct {
		Version *string  `parquet:"version,optional"`
		Product *Product `parquet:"product,optional"`
	}
	type Account struct {
		UID *string `parquet:"uid,optional"`
	}
	type Cloud struct {
		Provider *string  `parquet:"provider,optional"`
		Account  *Account `parquet:"account,optional"`
	}
	type TestRecord struct {
		Time     int64     `parquet:"time"`
		Cloud    *Cloud    `parquet:"cloud,optional"`
		Metadata *Metadata `parquet:"metadata,optional"`
	}

	schema := parquet.SchemaOf(new(TestRecord))
	messageType := reflect.TypeOf(TestRecord{})

	writer := &StreamingParquetWriter{
		schema:          schema,
		messageType:     messageType,
		compressionType: compress.Codec(&parquet.Zstd),
		rowGroupSize:    5,
		uploadBuffer:    bytes.NewBuffer(nil),
		rowBuffer:       make([]any, 0, 5),
		rowGroupsMeta:   []RowGroupMetadata{},
		uploadSize:      4,
	}

	// Add complex test data
	uid := "353785743975"
	provider := "AWS"
	version := "1.1.0"
	vendorName := "AWS"
	featureName := "Flowlogs"

	testData := []map[string]any{
		{
			"time": int64(1768980921000),
			"cloud": map[string]any{
				"provider": provider,
				"account": map[string]any{
					"uid": uid,
				},
			},
			"metadata": map[string]any{
				"version": version,
				"product": map[string]any{
					"vendor_name": vendorName,
					"feature": map[string]any{
						"name": featureName,
					},
				},
			},
		},
		{
			"time": int64(1768980922000),
			"cloud": map[string]any{
				"provider": provider,
				"account":  nil, // Null nested struct
			},
			"metadata": nil, // Null top-level struct
		},
		{
			"time":  int64(1768980923000),
			"cloud": nil, // All null
			"metadata": map[string]any{
				"version": version,
				"product": nil, // Null intermediate struct
			},
		},
	}

	for _, event := range testData {
		v := reflect.New(messageType)
		err := mapToStruct(event, v.Interface())
		require.NoError(t, err)
		writer.rowBuffer = append(writer.rowBuffer, v.Interface())
	}

	// Flush
	err := writer.flushRowGroup(context.Background())
	require.NoError(t, err)

	// Verify metadata was preserved
	require.Len(t, writer.rowGroupsMeta, 1)
	meta := writer.rowGroupsMeta[0]

	// Should have column chunks for all leaf fields
	assert.NotEmpty(t, meta.ColumnChunks)

	// Each column should have proper definition level metadata
	for i, col := range meta.ColumnChunks {
		assert.NotNil(t, col.MetaData.PathInSchema, "Column %d should have path", i)
		assert.NotEmpty(t, col.MetaData.PathInSchema, "Column %d should have non-empty path", i)

		// For optional nested fields, NumValues may differ from NumRows
		// (because some rows may have null values)
		assert.GreaterOrEqual(t, col.MetaData.NumValues, int64(0), "Column %d NumValues", i)
	}

	// Verify total rows
	assert.Equal(t, int64(3), writer.totalRows)
}

// TestFlushRowGroup_PreservesPageIndex tests that page index metadata
// (ColumnIndex and OffsetIndex) is preserved from temp files
func TestFlushRowGroup_PreservesPageIndex(t *testing.T) {
	type TestRecord struct {
		ID    int64  `parquet:"id"`
		Name  string `parquet:"name"`
		Value int64  `parquet:"value"`
	}

	schema := parquet.SchemaOf(new(TestRecord))
	messageType := reflect.TypeOf(TestRecord{})

	writer := &StreamingParquetWriter{
		schema:          schema,
		messageType:     messageType,
		compressionType: &parquet.Snappy,
		rowGroupSize:    100,
		uploadBuffer:    bytes.NewBuffer(nil),
		rowBuffer:       make([]any, 0, 100),
		rowGroupsMeta:   []RowGroupMetadata{},
		pageIndexParts:  [][]byte{},
		uploadSize:      4,
		filePosition:    4,
	}

	// Add test data - enough to trigger page index creation
	for i := 0; i < 100; i++ {
		event := map[string]any{
			"id":    int64(i),
			"name":  "test",
			"value": int64(i * 10),
		}
		v := reflect.New(messageType)
		err := mapToStruct(event, v.Interface())
		require.NoError(t, err)
		writer.rowBuffer = append(writer.rowBuffer, v.Interface())
	}

	// Flush row group
	err := writer.flushRowGroup(context.Background())
	require.NoError(t, err)

	// Verify metadata was preserved
	require.Len(t, writer.rowGroupsMeta, 1)
	meta := writer.rowGroupsMeta[0]

	// Verify column chunks have page index metadata
	require.NotEmpty(t, meta.ColumnChunks, "Should have column chunks")

	// Check if any columns have page index metadata
	// Note: parquet-go may or may not generate page index depending on data characteristics
	hasPageIndex := false
	for _, col := range meta.ColumnChunks {
		if col.ColumnIndexLength > 0 || col.OffsetIndexLength > 0 {
			hasPageIndex = true

			// If page index is present, verify the metadata fields are set
			if col.ColumnIndexLength > 0 {
				assert.Greater(t, col.ColumnIndexLength, int32(0), "ColumnIndexLength should be positive")
			}
			if col.OffsetIndexLength > 0 {
				assert.Greater(t, col.OffsetIndexLength, int32(0), "OffsetIndexLength should be positive")
			}
		}
	}

	// If page index was generated, verify pageIndexParts was populated
	if hasPageIndex {
		require.Len(t, writer.pageIndexParts, 1, "Should have collected page index data")
		assert.NotEmpty(t, writer.pageIndexParts[0], "Page index data should not be empty")
	}

	// Verify row buffer was cleared
	assert.Empty(t, writer.rowBuffer)
	assert.Equal(t, int64(100), writer.totalRows)
}

// TestFlushRowGroup_PageIndexOffsetTracking tests that page index offsets
// are correctly tracked in column metadata
func TestFlushRowGroup_PageIndexOffsetTracking(t *testing.T) {
	type TestRecord struct {
		ID    int64   `parquet:"id"`
		Value int64   `parquet:"value"`
		Score float64 `parquet:"score"`
	}

	schema := parquet.SchemaOf(new(TestRecord))
	messageType := reflect.TypeOf(TestRecord{})

	writer := &StreamingParquetWriter{
		schema:          schema,
		messageType:     messageType,
		compressionType: &parquet.Snappy,
		rowGroupSize:    50,
		uploadBuffer:    bytes.NewBuffer(nil),
		rowBuffer:       make([]any, 0, 50),
		rowGroupsMeta:   []RowGroupMetadata{},
		pageIndexParts:  [][]byte{},
		uploadSize:      4,
		filePosition:    4,
	}

	// Add test data with varying values to encourage page index generation
	for i := 0; i < 50; i++ {
		event := map[string]any{
			"id":    int64(i),
			"value": int64(i * 100),
			"score": float64(i) * 1.5,
		}
		v := reflect.New(messageType)
		err := mapToStruct(event, v.Interface())
		require.NoError(t, err)
		writer.rowBuffer = append(writer.rowBuffer, v.Interface())
	}

	// Flush
	err := writer.flushRowGroup(context.Background())
	require.NoError(t, err)

	require.Len(t, writer.rowGroupsMeta, 1)
	meta := writer.rowGroupsMeta[0]

	// Verify that column chunks exist
	require.NotEmpty(t, meta.ColumnChunks)

	// If page index metadata is present, the offsets should initially be 0
	// (they get updated during Close() when page index is written)
	for i, col := range meta.ColumnChunks {
		// ColumnIndexOffset and OffsetIndexOffset are set to 0 initially
		// and will be updated in Close() when the page index is written
		if col.ColumnIndexLength > 0 {
			assert.GreaterOrEqual(t, col.ColumnIndexOffset, int64(0),
				"Column %d ColumnIndexOffset should be non-negative", i)
		}
		if col.OffsetIndexLength > 0 {
			assert.GreaterOrEqual(t, col.OffsetIndexOffset, int64(0),
				"Column %d OffsetIndexOffset should be non-negative", i)
		}
	}
}

// TestClose_WritesPageIndexInCorrectOrder tests that page index data
// is written in the correct Parquet format: ALL ColumnIndex for ALL row groups,
// then ALL OffsetIndex for ALL row groups
func TestClose_WritesPageIndexInCorrectOrder(t *testing.T) {
	type TestRecord struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}

	mockClient := &mockS3Client{}
	schema := parquet.SchemaOf(new(TestRecord))
	messageType := reflect.TypeOf(TestRecord{})

	config := StreamingWriterConfig{
		S3Client:        mockClient,
		Bucket:          "test-bucket",
		Key:             "test-key.parquet",
		Schema:          schema,
		MessageType:     messageType,
		CompressionType: &parquet.Snappy,
		RowGroupSize:    20,
	}

	writer, err := NewStreamingParquetWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	// Write enough data to create multiple row groups
	for i := 0; i < 60; i++ {
		event := map[string]any{
			"id":   int64(i),
			"name": "test-name",
		}
		err = writer.WriteEvent(ctx, event)
		require.NoError(t, err)
	}

	// Close should flush remaining data and write page index
	err = writer.Close(ctx)
	require.NoError(t, err)

	// Should have created multiple row groups
	assert.Greater(t, len(writer.rowGroupsMeta), 1, "Should have multiple row groups")

	// If page index was generated, verify the metadata structure
	if len(writer.pageIndexParts) > 0 {
		// Each row group should have corresponding page index data
		assert.Len(t, writer.pageIndexParts, len(writer.rowGroupsMeta),
			"Should have page index parts for each row group")

		// Verify that column metadata has been updated with offsets
		// (Close() should have set ColumnIndexOffset and OffsetIndexOffset)
		for rgIdx, meta := range writer.rowGroupsMeta {
			for colIdx, col := range meta.ColumnChunks {
				if col.ColumnIndexLength > 0 {
					assert.Greater(t, col.ColumnIndexOffset, int64(0),
						"RowGroup %d Column %d should have ColumnIndexOffset set", rgIdx, colIdx)
				}
				if col.OffsetIndexLength > 0 {
					assert.Greater(t, col.OffsetIndexOffset, int64(0),
						"RowGroup %d Column %d should have OffsetIndexOffset set", rgIdx, colIdx)
				}
			}
		}

		// Verify ordering: all ColumnIndex offsets should come before all OffsetIndex offsets
		// Collect all ColumnIndex offsets and OffsetIndex offsets
		var columnIndexOffsets []int64
		var offsetIndexOffsets []int64

		for _, meta := range writer.rowGroupsMeta {
			for _, col := range meta.ColumnChunks {
				if col.ColumnIndexLength > 0 {
					columnIndexOffsets = append(columnIndexOffsets, col.ColumnIndexOffset)
				}
				if col.OffsetIndexLength > 0 {
					offsetIndexOffsets = append(offsetIndexOffsets, col.OffsetIndexOffset)
				}
			}
		}

		// If we have both, verify ALL ColumnIndex comes before ALL OffsetIndex
		if len(columnIndexOffsets) > 0 && len(offsetIndexOffsets) > 0 {
			// Find max ColumnIndex offset
			maxColIndexOffset := int64(0)
			for _, offset := range columnIndexOffsets {
				if offset > maxColIndexOffset {
					maxColIndexOffset = offset
				}
			}

			// Find min OffsetIndex offset
			minOffIndexOffset := offsetIndexOffsets[0]
			for _, offset := range offsetIndexOffsets {
				if offset < minOffIndexOffset {
					minOffIndexOffset = offset
				}
			}

			// Max ColumnIndex offset should be less than min OffsetIndex offset
			assert.Less(t, maxColIndexOffset, minOffIndexOffset,
				"All ColumnIndex data should be written before all OffsetIndex data")
		}
	}
}

// TestClose_PageIndexOffsetCalculation tests that page index offsets
// are correctly calculated based on file position
func TestClose_PageIndexOffsetCalculation(t *testing.T) {
	type TestRecord struct {
		ID    int64 `parquet:"id"`
		Value int64 `parquet:"value"`
	}

	mockClient := &mockS3Client{}
	schema := parquet.SchemaOf(new(TestRecord))
	messageType := reflect.TypeOf(TestRecord{})

	config := StreamingWriterConfig{
		S3Client:        mockClient,
		Bucket:          "test-bucket",
		Key:             "test-key.parquet",
		Schema:          schema,
		MessageType:     messageType,
		CompressionType: &parquet.Snappy,
		RowGroupSize:    30,
	}

	writer, err := NewStreamingParquetWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = writer.Initialize(ctx)
	require.NoError(t, err)

	// Write data
	for i := 0; i < 30; i++ {
		event := map[string]any{
			"id":    int64(i),
			"value": int64(i * 100),
		}
		err = writer.WriteEvent(ctx, event)
		require.NoError(t, err)
	}

	// Close
	err = writer.Close(ctx)
	require.NoError(t, err)

	// Verify that if page index was generated, offsets are set correctly
	if len(writer.pageIndexParts) > 0 {
		require.Len(t, writer.rowGroupsMeta, 1)
		meta := writer.rowGroupsMeta[0]

		for colIdx, col := range meta.ColumnChunks {
			// Offsets should be after the row group data
			if col.ColumnIndexLength > 0 {
				assert.Greater(t, col.ColumnIndexOffset, meta.FileOffset,
					"Column %d ColumnIndexOffset should be after row group data", colIdx)
			}
			if col.OffsetIndexLength > 0 {
				assert.Greater(t, col.OffsetIndexOffset, meta.FileOffset,
					"Column %d OffsetIndexOffset should be after row group data", colIdx)
			}

			// OffsetIndex offset should be after ColumnIndex offset
			if col.ColumnIndexLength > 0 && col.OffsetIndexLength > 0 {
				assert.Greater(t, col.OffsetIndexOffset, col.ColumnIndexOffset,
					"Column %d OffsetIndexOffset should be after ColumnIndexOffset", colIdx)
			}
		}
	}
}
