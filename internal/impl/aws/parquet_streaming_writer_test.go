package aws

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStreamingParquetWriter_BasicFunctionality tests the core writer without S3
func TestStreamingParquetWriter_BasicFunctionality(t *testing.T) {
	// Define a simple test schema
	type TestRecord struct {
		ID      int64  `parquet:"id"`
		Name    string `parquet:"name"`
		Value   float64 `parquet:"value"`
	}

	schema := parquet.SchemaOf(new(TestRecord))
	messageType := reflect.TypeOf(TestRecord{})

	// Create test events
	testEvents := []map[string]any{
		{"id": int64(1), "name": "Alice", "value": 10.5},
		{"id": int64(2), "name": "Bob", "value": 20.3},
		{"id": int64(3), "name": "Charlie", "value": 15.7},
	}

	t.Run("row buffering", func(t *testing.T) {
		config := StreamingWriterConfig{
			Schema:          schema,
			MessageType:     messageType,
			CompressionType: &parquet.Snappy,
			RowGroupSize:    10, // Large enough to not flush
		}

		writer := &StreamingParquetWriter{
			schema:          config.Schema,
			messageType:     config.MessageType,
			compressionType: config.CompressionType,
			rowGroupSize:    config.RowGroupSize,
			uploadBuffer:    bytes.NewBuffer(nil),
			rowBuffer:       make([]any, 0, config.RowGroupSize),
		}

		// Write events
		for _, event := range testEvents {
			v := reflect.New(messageType)
			err := mapToStruct(event, v.Interface())
			require.NoError(t, err)

			writer.rowBuffer = append(writer.rowBuffer, v.Interface())
		}

		// Verify buffering
		assert.Equal(t, 3, len(writer.rowBuffer))
		assert.Equal(t, int64(0), writer.totalRows) // Not yet flushed
	})

	t.Run("row group flushing", func(t *testing.T) {
		config := StreamingWriterConfig{
			Schema:          schema,
			MessageType:     messageType,
			CompressionType: &parquet.Snappy,
			RowGroupSize:    2, // Small to trigger flush
		}

		writer := &StreamingParquetWriter{
			schema:          config.Schema,
			messageType:     config.MessageType,
			compressionType: config.CompressionType,
			rowGroupSize:    config.RowGroupSize,
			uploadBuffer:    bytes.NewBuffer(nil),
			rowBuffer:       make([]any, 0, config.RowGroupSize),
			rowGroupsMeta:   []RowGroupMetadata{},
		}

		// Add events
		for _, event := range testEvents[:2] {
			v := reflect.New(messageType)
			err := mapToStruct(event, v.Interface())
			require.NoError(t, err)
			writer.rowBuffer = append(writer.rowBuffer, v.Interface())
		}

		// Flush manually
		err := writer.flushRowGroup(context.Background())
		require.NoError(t, err)

		// Verify flush occurred
		assert.Equal(t, 0, len(writer.rowBuffer)) // Buffer cleared
		assert.Equal(t, int64(2), writer.totalRows)
		assert.Equal(t, 1, len(writer.rowGroupsMeta))
		assert.Greater(t, writer.uploadSize, int64(0))
	})

	t.Run("footer generation", func(t *testing.T) {
		config := StreamingWriterConfig{
			Schema:          schema,
			MessageType:     messageType,
			CompressionType: &parquet.Snappy,
			RowGroupSize:    10,
		}

		writer := &StreamingParquetWriter{
			schema:          config.Schema,
			messageType:     config.MessageType,
			compressionType: config.CompressionType,
			rowGroupSize:    config.RowGroupSize,
			uploadBuffer:    bytes.NewBuffer(nil),
			rowBuffer:       make([]any, 0, config.RowGroupSize),
			totalRows:       100,
			rowGroupsMeta: []RowGroupMetadata{
				{NumRows: 50, TotalByteSize: 1024, FileOffset: 4},
				{NumRows: 50, TotalByteSize: 1024, FileOffset: 1028},
			},
		}

		// Generate footer
		footer, err := writer.generateFooter()
		require.NoError(t, err)

		// Verify footer structure
		assert.Greater(t, len(footer), 0, "Footer should have content")

		// Footer should end with 4 bytes for length + 4 bytes "PAR1" magic
		// But generateFooter only returns footer + length, not the final "PAR1"
		assert.Greater(t, len(footer), 8, "Footer should be substantial")
	})
}

// TestStreamingParquetWriter_MapToStruct tests the map conversion
func TestStreamingParquetWriter_MapToStruct(t *testing.T) {
	type TestRecord struct {
		ID    int64   `parquet:"id" json:"ID"`
		Name  string  `parquet:"name" json:"Name"`
		Value float64 `parquet:"value" json:"Value"`
	}

	testCases := []struct {
		name     string
		input    map[string]any
		expected TestRecord
		wantErr  bool
	}{
		{
			name: "valid conversion",
			input: map[string]any{
				"ID":    int64(42),
				"Name":  "test",
				"Value": float64(3.14),
			},
			expected: TestRecord{ID: 42, Name: "test", Value: 3.14},
			wantErr:  false,
		},
		{
			name: "missing optional fields",
			input: map[string]any{
				"ID": int64(1),
			},
			expected: TestRecord{ID: 1, Name: "", Value: 0.0},
			wantErr:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var result TestRecord
			err := mapToStruct(tc.input, &result)

			if tc.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

// TestStreamingParquetWriter_Stats tests writer statistics
func TestStreamingParquetWriter_Stats(t *testing.T) {
	config := StreamingWriterConfig{
		Schema:          parquet.SchemaOf(new(struct{ ID int64 `parquet:"id"` })),
		MessageType:     reflect.TypeOf(struct{ ID int64 `parquet:"id"` }{}),
		CompressionType: &parquet.Snappy,
		RowGroupSize:    10,
	}

	writer := &StreamingParquetWriter{
		schema:          config.Schema,
		messageType:     config.MessageType,
		compressionType: config.CompressionType,
		rowGroupSize:    config.RowGroupSize,
		uploadBuffer:    bytes.NewBuffer(nil),
		rowBuffer:       make([]any, 0, config.RowGroupSize),
		totalRows:       150,
		rowGroupsMeta:   []RowGroupMetadata{{}, {}, {}},
		uploadSize:      5242880, // 5 MB
	}

	// Add some buffered rows
	writer.rowBuffer = append(writer.rowBuffer, &struct{ ID int64 `parquet:"id"` }{ID: 1})
	writer.rowBuffer = append(writer.rowBuffer, &struct{ ID int64 `parquet:"id"` }{ID: 2})

	stats := writer.Stats()

	assert.Equal(t, int64(150), stats.TotalRows)
	assert.Equal(t, 3, stats.RowGroups)
	assert.Equal(t, 2, stats.BufferedRows)
	assert.Equal(t, int64(5242880), stats.BufferedBytes)
}
