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
		ID    int64   `parquet:"id"`
		Cloud *Cloud  `parquet:"cloud,optional"`
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
				"account": nil, // Test null nested field
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
	assert.Greater(t, len(meta.ColumnChunks), 0, "ColumnChunks should be populated from temp file footer")

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
	assert.Len(t, writer.rowBuffer, 0)
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
	assert.Equal(t, 3, len(meta.ColumnChunks), "Should have extracted column chunks for all 3 fields")

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
				Type:             format.Int64,
				PathInSchema:     []string{"id"},
				Codec:            format.Snappy,
				NumValues:        50,
				DataPageOffset:   1000,
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
			"time": int64(1768980923000),
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
	assert.Greater(t, len(meta.ColumnChunks), 0)

	// Each column should have proper definition level metadata
	for i, col := range meta.ColumnChunks {
		assert.NotNil(t, col.MetaData.PathInSchema, "Column %d should have path", i)
		assert.Greater(t, len(col.MetaData.PathInSchema), 0, "Column %d should have non-empty path", i)

		// For optional nested fields, NumValues may differ from NumRows
		// (because some rows may have null values)
		assert.GreaterOrEqual(t, col.MetaData.NumValues, int64(0), "Column %d NumValues", i)
	}

	// Verify total rows
	assert.Equal(t, int64(3), writer.totalRows)
}
