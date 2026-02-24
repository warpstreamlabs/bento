package aws

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"

	bentoparquet "github.com/warpstreamlabs/bento/internal/impl/parquet"
)

// S3API defines the S3 operations needed for multipart uploads
type S3API interface {
	CreateMultipartUpload(ctx context.Context, input *s3.CreateMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(ctx context.Context, input *s3.AbortMultipartUploadInput, opts ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
}

// StreamingParquetWriter writes Parquet data incrementally to S3 using multipart uploads.
// It buffers rows into row groups and uploads parts as they reach the S3 minimum part size.
type StreamingParquetWriter struct {
	// Configuration
	schema          *parquet.Schema
	messageType     reflect.Type
	compressionType compress.Codec
	rowGroupSize    int64

	// S3 client and upload state
	s3Client       S3API
	bucket         string
	key            string
	uploadID       *string
	partNumber     int32
	completedParts []types.CompletedPart

	// Buffering
	rowBuffer    []any
	uploadBuffer *bytes.Buffer
	uploadSize   int64 // Size of data in upload buffer (resets after each part upload)

	// Metadata tracking for footer
	rowGroupsMeta  []RowGroupMetadata
	totalRows      int64
	filePosition   int64    // Absolute position in the final file (never resets)
	pageIndexParts [][]byte // Page index data from each row group (written at end)

	// Lifecycle
	created   time.Time
	lastWrite time.Time
	closed    bool

	// Thread safety
	mu sync.Mutex
}

// RowGroupMetadata tracks information about each row group for footer generation
type RowGroupMetadata struct {
	NumRows       int64
	TotalByteSize int64
	FileOffset    int64
	ColumnChunks  []format.ColumnChunk // Preserve actual column metadata from temp file
}

// StreamingWriterConfig contains configuration for creating a StreamingParquetWriter
type StreamingWriterConfig struct {
	S3Client        S3API
	Bucket          string
	Key             string
	Schema          *parquet.Schema
	MessageType     reflect.Type
	CompressionType compress.Codec
	RowGroupSize    int64 // Number of rows per row group (default: 1000)
}

// NewStreamingParquetWriter creates a new streaming Parquet writer
func NewStreamingParquetWriter(config StreamingWriterConfig) (*StreamingParquetWriter, error) {
	if config.RowGroupSize <= 0 {
		config.RowGroupSize = 1000 // Default
	}

	w := &StreamingParquetWriter{
		schema:          config.Schema,
		messageType:     config.MessageType,
		compressionType: config.CompressionType,
		rowGroupSize:    config.RowGroupSize,
		s3Client:        config.S3Client,
		bucket:          config.Bucket,
		key:             config.Key,
		uploadBuffer:    bytes.NewBuffer(nil),
		rowBuffer:       make([]any, 0, config.RowGroupSize),
		created:         time.Now(),
		lastWrite:       time.Now(),
	}

	return w, nil
}

// Initialize starts the S3 multipart upload
func (w *StreamingParquetWriter) Initialize(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.uploadID != nil {
		return errors.New("writer already initialized")
	}

	// Start multipart upload
	resp, err := w.s3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(w.bucket),
		Key:    aws.String(w.key),
	})
	if err != nil {
		return fmt.Errorf("failed to create multipart upload: %w", err)
	}

	w.uploadID = resp.UploadId

	// Write Parquet file header (magic bytes "PAR1")
	header := []byte("PAR1")
	w.uploadBuffer.Write(header)
	w.uploadSize = int64(len(header))
	w.filePosition = int64(len(header))

	return nil
}

// WriteEvent adds an event to the buffer and flushes row group when full
func (w *StreamingParquetWriter) WriteEvent(ctx context.Context, event map[string]any) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.New("writer is closed")
	}

	if w.uploadID == nil {
		return errors.New("writer not initialized")
	}

	// Convert event to Parquet struct
	v := reflect.New(w.messageType)
	if err := mapToStruct(event, v.Interface()); err != nil {
		return fmt.Errorf("failed to convert event to struct: %w", err)
	}

	// Add to row buffer
	w.rowBuffer = append(w.rowBuffer, v.Interface())
	w.lastWrite = time.Now()

	// Flush row group if buffer is full
	if int64(len(w.rowBuffer)) >= w.rowGroupSize {
		return w.flushRowGroup(ctx)
	}

	return nil
}

// flushRowGroup encodes buffered rows to a Parquet row group and adds to upload buffer
func (w *StreamingParquetWriter) flushRowGroup(ctx context.Context) error {
	if len(w.rowBuffer) == 0 {
		return nil
	}

	// NOTE: This approach creates a complete Parquet file temporarily to extract
	// row group bytes. While this may seem inefficient, it's actually the most
	// straightforward way to leverage parquet-go's encoding without implementing
	// low-level Parquet format details. Memory overhead is minimal since only
	// one row group is encoded at a time (~row_group_size * row_size bytes).

	tempBuffer := bytes.NewBuffer(nil)
	writer := parquet.NewGenericWriter[any](
		tempBuffer,
		w.schema,
		parquet.Compression(w.compressionType),
	)

	// Write all rows in buffer
	_, err := writer.Write(w.rowBuffer)
	if err != nil {
		return fmt.Errorf("failed to write row group: %w", err)
	}

	// Close writer to finalize encoding
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close row group writer: %w", err)
	}

	// Extract row group bytes by stripping header and footer
	// Parquet format: [4 bytes "PAR1"] [row group data] [footer] [4 bytes footer size] [4 bytes "PAR1"]
	fullFileData := tempBuffer.Bytes()

	// Skip first 4 bytes (header "PAR1")
	if len(fullFileData) < 12 {
		return fmt.Errorf("encoded data too short: %d bytes", len(fullFileData))
	}

	// Read footer size from last 8 bytes: [4 bytes footer length][4 bytes "PAR1"]
	footerSizeBytes := fullFileData[len(fullFileData)-8 : len(fullFileData)-4]
	footerSize := int(footerSizeBytes[0]) | int(footerSizeBytes[1])<<8 |
		int(footerSizeBytes[2])<<16 | int(footerSizeBytes[3])<<24

	// Extract just the row group data (skip header, remove footer + footer metadata)
	rowGroupStart := 4
	rowGroupEnd := len(fullFileData) - footerSize - 8

	if rowGroupEnd <= rowGroupStart {
		return fmt.Errorf("invalid row group boundaries: start=%d, end=%d", rowGroupStart, rowGroupEnd)
	}

	rowGroupData := fullFileData[rowGroupStart:rowGroupEnd]
	rowGroupSize := int64(len(rowGroupData))

	// Extract actual column metadata from the temporary file's footer
	// This ensures definition/repetition levels are correctly preserved
	footerBytes := fullFileData[rowGroupEnd : len(fullFileData)-8]
	var fileMeta format.FileMetaData
	if err := thrift.Unmarshal(new(thrift.CompactProtocol), footerBytes, &fileMeta); err != nil {
		return fmt.Errorf("failed to parse temporary file footer: %w", err)
	}

	// Extract column chunks and page index data from the temp file
	var columnChunks []format.ColumnChunk
	var pageIndexData []byte

	if len(fileMeta.RowGroups) > 0 {
		columnChunks = make([]format.ColumnChunk, len(fileMeta.RowGroups[0].Columns))

		// Collect page index data from temp file
		// Important: Parquet format requires all ColumnIndex data first, then all OffsetIndex data
		pageIndexBuffer := bytes.NewBuffer(nil)

		// First pass: collect all ColumnIndex data
		for _, col := range fileMeta.RowGroups[0].Columns {
			if col.ColumnIndexOffset > 0 && col.ColumnIndexLength > 0 {
				columnIndexData := fullFileData[col.ColumnIndexOffset : col.ColumnIndexOffset+int64(col.ColumnIndexLength)]
				pageIndexBuffer.Write(columnIndexData)
			}
		}

		// Second pass: collect all OffsetIndex data
		for _, col := range fileMeta.RowGroups[0].Columns {
			if col.OffsetIndexOffset > 0 && col.OffsetIndexLength > 0 {
				offsetIndexData := fullFileData[col.OffsetIndexOffset : col.OffsetIndexOffset+int64(col.OffsetIndexLength)]
				pageIndexBuffer.Write(offsetIndexData)
			}
		}

		pageIndexData = pageIndexBuffer.Bytes()

		// Store page index data for this row group to write later (after all row groups)
		w.pageIndexParts = append(w.pageIndexParts, pageIndexData)

		// Adjust column metadata offsets
		for i, col := range fileMeta.RowGroups[0].Columns {
			adjustedCol := col
			adjustedCol.MetaData.DataPageOffset = w.filePosition + col.MetaData.DataPageOffset - 4 // Subtract temp file header size
			if adjustedCol.MetaData.DictionaryPageOffset != 0 {
				adjustedCol.MetaData.DictionaryPageOffset = w.filePosition + adjustedCol.MetaData.DictionaryPageOffset - 4
			}
			// CRITICAL: Clear page index and bloom filter offsets from temp file
			// The streaming writer doesn't include page indexes or bloom filters,
			// so these offsets are invalid. If left set, parquet-go will try to
			// read them and panic with "slice bounds out of range" errors.
			adjustedCol.MetaData.IndexPageOffset = 0
			adjustedCol.MetaData.BloomFilterOffset = 0

			// CRITICAL: Also clear ColumnChunk-level page index offsets
			// These are separate from the ColumnMetaData offsets and must also be cleared
			adjustedCol.OffsetIndexOffset = 0
			adjustedCol.OffsetIndexLength = 0
			adjustedCol.ColumnIndexOffset = 0
			adjustedCol.ColumnIndexLength = 0

			columnChunks[i] = adjustedCol
		}
	}

	// Add row group data to upload buffer
	rowGroupFileOffset := w.filePosition
	w.uploadBuffer.Write(rowGroupData)
	w.uploadSize += rowGroupSize
	w.filePosition += rowGroupSize

	// Track metadata for footer
	w.rowGroupsMeta = append(w.rowGroupsMeta, RowGroupMetadata{
		NumRows:       int64(len(w.rowBuffer)),
		TotalByteSize: rowGroupSize,
		FileOffset:    rowGroupFileOffset,
		ColumnChunks:  columnChunks,
	})

	w.totalRows += int64(len(w.rowBuffer))

	// Clear row buffer
	w.rowBuffer = w.rowBuffer[:0]

	// Upload part if buffer exceeds S3 minimum (5 MB)
	if w.uploadSize >= 5*1024*1024 {
		return w.uploadPart(ctx)
	}

	return nil
}

// uploadPart uploads the current buffer as an S3 part
func (w *StreamingParquetWriter) uploadPart(ctx context.Context) error {
	if w.uploadBuffer.Len() == 0 {
		return nil
	}

	w.partNumber++

	// Upload part with retry
	var uploadErr error
	for attempt := 0; attempt < 3; attempt++ {
		resp, err := w.s3Client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(w.bucket),
			Key:        aws.String(w.key),
			PartNumber: aws.Int32(w.partNumber),
			UploadId:   w.uploadID,
			Body:       bytes.NewReader(w.uploadBuffer.Bytes()),
		})

		if err == nil {
			// Success
			w.completedParts = append(w.completedParts, types.CompletedPart{
				ETag:       resp.ETag,
				PartNumber: aws.Int32(w.partNumber),
			})

			// Clear upload buffer
			w.uploadBuffer.Reset()
			w.uploadSize = 0

			return nil
		}

		uploadErr = err
		time.Sleep(time.Second * time.Duration(1<<attempt)) // Exponential backoff
	}

	// All retries failed
	w.abortMultipartUpload(context.Background())
	return fmt.Errorf("failed to upload part after retries: %w", uploadErr)
}

// Close finalizes the Parquet file by flushing remaining data, writing footer, and completing upload
func (w *StreamingParquetWriter) Close(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	if w.uploadID == nil {
		return errors.New("writer not initialized")
	}

	// Flush any remaining rows
	if len(w.rowBuffer) > 0 {
		if err := w.flushRowGroup(ctx); err != nil {
			w.abortMultipartUpload(ctx)
			return fmt.Errorf("failed to flush final row group: %w", err)
		}
	}

	// Write all page index data after row groups (before footer)
	// Parquet format: all ColumnIndex for all row groups, then all OffsetIndex for all row groups
	if len(w.pageIndexParts) > 0 {
		// First pass: write all ColumnIndex data for all row groups and update offsets
		for rgIndex := range w.pageIndexParts {
			numCols := len(w.rowGroupsMeta[rgIndex].ColumnChunks)

			for colIdx := 0; colIdx < numCols; colIdx++ {
				col := &w.rowGroupsMeta[rgIndex].ColumnChunks[colIdx]

				// Write ColumnIndex if present
				if col.ColumnIndexLength > 0 {
					// Extract ColumnIndex data from pageIndexParts
					// pageIndexParts contains [all ColumnIndex, all OffsetIndex] for this row group
					// We need to find the specific ColumnIndex for this column
					// Calculate the offset within the pageIndexParts buffer
					colIndexDataOffset := int64(0)
					for c := 0; c < colIdx; c++ {
						if w.rowGroupsMeta[rgIndex].ColumnChunks[c].ColumnIndexLength > 0 {
							colIndexDataOffset += int64(w.rowGroupsMeta[rgIndex].ColumnChunks[c].ColumnIndexLength)
						}
					}

					colIndexData := w.pageIndexParts[rgIndex][colIndexDataOffset : colIndexDataOffset+int64(col.ColumnIndexLength)]
					col.ColumnIndexOffset = w.filePosition
					w.uploadBuffer.Write(colIndexData)
					w.uploadSize += int64(len(colIndexData))
					w.filePosition += int64(len(colIndexData))
				}
			}
		}

		// Second pass: write all OffsetIndex data for all row groups and update offsets
		for rgIndex := range w.pageIndexParts {
			numCols := len(w.rowGroupsMeta[rgIndex].ColumnChunks)

			// Calculate where OffsetIndex data starts in pageIndexParts
			offsetIndexStartInPart := int64(0)
			for c := 0; c < numCols; c++ {
				if w.rowGroupsMeta[rgIndex].ColumnChunks[c].ColumnIndexLength > 0 {
					offsetIndexStartInPart += int64(w.rowGroupsMeta[rgIndex].ColumnChunks[c].ColumnIndexLength)
				}
			}

			for colIdx := 0; colIdx < numCols; colIdx++ {
				col := &w.rowGroupsMeta[rgIndex].ColumnChunks[colIdx]

				// Write OffsetIndex if present
				if col.OffsetIndexLength > 0 {
					// Calculate offset of this column's OffsetIndex within the OffsetIndex section
					offsetIndexDataOffset := offsetIndexStartInPart
					for c := 0; c < colIdx; c++ {
						if w.rowGroupsMeta[rgIndex].ColumnChunks[c].OffsetIndexLength > 0 {
							offsetIndexDataOffset += int64(w.rowGroupsMeta[rgIndex].ColumnChunks[c].OffsetIndexLength)
						}
					}

					offsetIndexData := w.pageIndexParts[rgIndex][offsetIndexDataOffset : offsetIndexDataOffset+int64(col.OffsetIndexLength)]
					col.OffsetIndexOffset = w.filePosition
					w.uploadBuffer.Write(offsetIndexData)
					w.uploadSize += int64(len(offsetIndexData))
					w.filePosition += int64(len(offsetIndexData))
				}
			}
		}

		// Check if we need to upload the page index part
		if w.uploadSize >= 5*1024*1024 {
			if err := w.uploadPart(ctx); err != nil {
				w.abortMultipartUpload(ctx)
				return fmt.Errorf("failed to upload page index part: %w", err)
			}
		}
	}

	// Generate and write Parquet footer
	footer, err := w.generateFooter()
	if err != nil {
		w.abortMultipartUpload(ctx)
		return fmt.Errorf("failed to generate footer: %w", err)
	}

	w.uploadBuffer.Write(footer)

	// Write footer magic bytes "PAR1"
	w.uploadBuffer.Write([]byte("PAR1"))

	// Upload final part (if any data in buffer)
	if w.uploadBuffer.Len() > 0 {
		if err := w.uploadPart(ctx); err != nil {
			return fmt.Errorf("failed to upload final part: %w", err)
		}
	}

	// Complete multipart upload
	_, err = w.s3Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(w.bucket),
		Key:      aws.String(w.key),
		UploadId: w.uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: w.completedParts,
		},
	})

	if err != nil {
		w.abortMultipartUpload(ctx)
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	w.closed = true
	return nil
}

// generateFooter creates the Parquet file footer with row group metadata
func (w *StreamingParquetWriter) generateFooter() ([]byte, error) {
	// Create FileMetaData structure for footer
	schema := w.schemaToThrift(w.schema)
	rowGroups := w.rowGroupsToThrift()

	fileMetaData := &format.FileMetaData{
		Version:   1,
		Schema:    schema,
		NumRows:   w.totalRows,
		RowGroups: rowGroups,
		CreatedBy: "bento-streaming-parquet-writer",
	}

	// Serialize FileMetaData to Thrift compact protocol
	footerBuffer := &bytes.Buffer{}

	// Use parquet-go's Thrift serialization (it handles this internally)
	// We need to create the footer bytes manually
	thriftBytes, err := w.serializeThrift(fileMetaData)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize FileMetaData: %w", err)
	}

	footerBuffer.Write(thriftBytes)

	// Write footer length as 4-byte little-endian integer
	footerLength := uint32(len(thriftBytes))
	lengthBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(lengthBytes, footerLength)
	footerBuffer.Write(lengthBytes)

	return footerBuffer.Bytes(), nil
}

// schemaToThrift converts parquet.Schema to Thrift format
func (w *StreamingParquetWriter) schemaToThrift(schema *parquet.Schema) []format.SchemaElement {
	// The schema root element
	numChildren := int32(len(schema.Fields()))
	elements := []format.SchemaElement{
		{
			Name:        "schema",
			NumChildren: numChildren,
		},
	}

	// Add each field recursively
	for _, field := range schema.Fields() {
		elements = w.appendFieldToThrift(elements, field)
	}

	return elements
}

// appendFieldToThrift converts a parquet.Field to Thrift SchemaElement and appends it and its children
func (w *StreamingParquetWriter) appendFieldToThrift(elements []format.SchemaElement, field parquet.Field) []format.SchemaElement {
	elem := format.SchemaElement{
		Name: field.Name(),
	}

	// Set repetition type
	var repType format.FieldRepetitionType
	if field.Optional() {
		repType = format.Optional
	} else if field.Repeated() {
		repType = format.Repeated
	} else {
		repType = format.Required
	}
	elem.RepetitionType = &repType

	// Set type based on field type
	if field.Leaf() {
		fType := formatTypeFromParquetField(field)
		elem.Type = &fType
	} else {
		// Group/struct type - has children
		numChildren := int32(len(field.Fields()))
		elem.NumChildren = numChildren
	}

	// Preserve LogicalType from the field for ALL types (leaf and group)
	// This is critical for systems like Apache Iceberg that validate types
	// and rely on modern LogicalType annotations (STRING, INT, TIMESTAMP, LIST, MAP, etc.)
	if field.Type().LogicalType() != nil {
		elem.LogicalType = field.Type().LogicalType()
	}

	// Append this element
	elements = append(elements, elem)

	// Recursively append child fields if this is a group
	if !field.Leaf() {
		for _, childField := range field.Fields() {
			elements = w.appendFieldToThrift(elements, childField)
		}
	}

	return elements
}

// rowGroupsToThrift converts collected row group metadata to Thrift format
func (w *StreamingParquetWriter) rowGroupsToThrift() []format.RowGroup {
	rowGroups := make([]format.RowGroup, len(w.rowGroupsMeta))

	for i, meta := range w.rowGroupsMeta {
		// Use the preserved column chunks from the temporary file if available
		columnChunks := meta.ColumnChunks

		// Fallback to approximated metadata if column chunks weren't preserved
		// (this shouldn't happen with the current implementation, but provides safety)
		if len(columnChunks) == 0 {
			leafColumns := w.collectLeafColumns(w.schema.Fields(), []string{})
			columnChunks = make([]format.ColumnChunk, len(leafColumns))

			for j, leafInfo := range leafColumns {
				fType := formatTypeFromParquetField(leafInfo.field)
				codec := formatCompressionCodec(w.compressionType)

				columnChunks[j] = format.ColumnChunk{
					MetaData: format.ColumnMetaData{
						Type:                  fType,
						PathInSchema:          leafInfo.path,
						Codec:                 codec,
						NumValues:             meta.NumRows,
						TotalCompressedSize:   meta.TotalByteSize / int64(len(leafColumns)),
						TotalUncompressedSize: meta.TotalByteSize / int64(len(leafColumns)),
						DataPageOffset:        meta.FileOffset,
					},
				}
			}
		}

		rowGroups[i] = format.RowGroup{
			Columns:       columnChunks,
			TotalByteSize: meta.TotalByteSize,
			NumRows:       meta.NumRows,
			FileOffset:    meta.FileOffset,
		}
	}

	return rowGroups
}

type leafColumnInfo struct {
	field parquet.Field
	path  []string
}

// collectLeafColumns recursively collects all leaf columns with their paths
func (w *StreamingParquetWriter) collectLeafColumns(fields []parquet.Field, parentPath []string) []leafColumnInfo {
	var leaves []leafColumnInfo

	for _, field := range fields {
		currentPath := append(parentPath, field.Name())

		if field.Leaf() {
			// This is a leaf column
			leaves = append(leaves, leafColumnInfo{
				field: field,
				path:  currentPath,
			})
		} else {
			// This is a group - recurse into children
			childLeaves := w.collectLeafColumns(field.Fields(), currentPath)
			leaves = append(leaves, childLeaves...)
		}
	}

	return leaves
}

// serializeThrift serializes FileMetaData using Thrift compact protocol
func (w *StreamingParquetWriter) serializeThrift(meta *format.FileMetaData) ([]byte, error) {
	// Use parquet-go's Thrift marshaling with compact protocol
	// This is the same approach used in parquet-go's writer.go
	thriftBytes, err := thrift.Marshal(new(thrift.CompactProtocol), meta)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal FileMetaData with Thrift: %w", err)
	}

	return thriftBytes, nil
}

// Helper functions for type conversion
func formatTypeFromParquetField(field parquet.Field) format.Type {
	// If field is not a leaf (has nested fields), it's a group type
	// Group types don't have a physical type in Parquet
	if !field.Leaf() {
		return format.ByteArray // Placeholder for group types
	}

	t := field.Type()
	switch t.Kind() {
	case parquet.Boolean:
		return format.Boolean
	case parquet.Int32:
		return format.Int32
	case parquet.Int64:
		return format.Int64
	case parquet.Int96:
		return format.Int96
	case parquet.Float:
		return format.Float
	case parquet.Double:
		return format.Double
	case parquet.ByteArray:
		return format.ByteArray
	case parquet.FixedLenByteArray:
		return format.FixedLenByteArray
	default:
		return format.ByteArray
	}
}

func formatCompressionCodec(codec compress.Codec) format.CompressionCodec {
	// Map compression types
	codecName := codec.String()
	switch codecName {
	case "UNCOMPRESSED":
		return format.Uncompressed
	case "SNAPPY":
		return format.Snappy
	case "GZIP":
		return format.Gzip
	case "ZSTD":
		return format.Zstd
	case "BROTLI":
		return format.Brotli
	case "LZ4":
		return format.Lz4
	default:
		return format.Uncompressed
	}
}

// abortMultipartUpload aborts the S3 multipart upload on error
func (w *StreamingParquetWriter) abortMultipartUpload(ctx context.Context) {
	if w.uploadID == nil {
		return
	}

	_, _ = w.s3Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(w.bucket),
		Key:      aws.String(w.key),
		UploadId: w.uploadID,
	})
}

// ShouldClose returns true if the writer should be closed based on lifecycle rules
func (w *StreamingParquetWriter) ShouldClose(maxDuration time.Duration, maxEvents int64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	if time.Since(w.created) > maxDuration {
		return true
	}

	if w.totalRows >= maxEvents {
		return true
	}

	return false
}

// Stats returns current writer statistics
func (w *StreamingParquetWriter) Stats() WriterStats {
	w.mu.Lock()
	defer w.mu.Unlock()

	return WriterStats{
		TotalRows:     w.totalRows,
		RowGroups:     len(w.rowGroupsMeta),
		PartsUploaded: int(w.partNumber),
		BufferedRows:  len(w.rowBuffer),
		BufferedBytes: w.uploadSize,
		Age:           time.Since(w.created),
		LastWriteAge:  time.Since(w.lastWrite),
	}
}

// WriterStats contains statistics about a streaming writer
type WriterStats struct {
	TotalRows     int64
	RowGroups     int
	PartsUploaded int
	BufferedRows  int
	BufferedBytes int64
	Age           time.Duration
	LastWriteAge  time.Duration
}

// mapToStruct is a helper function to convert map[string]any to a struct
// Uses the existing Bento parquet conversion logic
func mapToStruct(data map[string]any, target any) error {
	return bentoparquet.MapToStruct(data, target)
}
