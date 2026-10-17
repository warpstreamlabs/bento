package aws

import (
	"bytes"
	"context"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
	"github.com/stretchr/testify/require"
)

// TestClose_OffsetIndexIsRebasedToFinalFileOffsets is the doc 39 regression test: writes a
// multi-row-group file (through the real S3 multipart path, via mockS3Client, so Close()'s
// rebase logic runs exactly as it does in production) and asserts, for every column chunk in
// every row group, that OffsetIndex.PageLocations[0].Offset >= the column chunk's own byte-range
// start.
//
// Before the fix (doc 39 Phase 4), every row group after the first had this invariant violated:
// the OffsetIndex still held offsets relative to that row group's own temp buffer (as if it were
// a standalone file starting at byte 0), while only the ColumnChunk's own
// DataPageOffset/DictionaryPageOffset had been rebased to the true final-file position. Strict
// readers (arrow-rs's SerializedPageReader) panic with "attempt to subtract with overflow" when
// they compute (offset_index.page_locations[0].offset as u64 - chunk_start) and the OffsetIndex
// offset is still the old, un-rebased (and therefore too-small) value.
func TestClose_OffsetIndexIsRebasedToFinalFileOffsets(t *testing.T) {
	type TestRecord struct {
		ID    int64  `parquet:"id"`
		Name  string `parquet:"name"`
		Value int64  `parquet:"value"`
	}

	var uploadedParts [][]byte
	mockClient := &mockS3Client{
		uploadPartFunc: func(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
			body, err := io.ReadAll(input.Body)
			if err != nil {
				return nil, err
			}
			uploadedParts = append(uploadedParts, body)
			return &s3.UploadPartOutput{ETag: aws.String("test-etag")}, nil
		},
	}

	schema := parquet.SchemaOf(new(TestRecord))
	messageType := reflect.TypeFor[TestRecord]()

	config := StreamingWriterConfig{
		S3Client:        mockClient,
		Bucket:          "test-bucket",
		Key:             "test-key.parquet",
		Schema:          schema,
		MessageType:     messageType,
		CompressionType: &parquet.Zstd,
		// Small row group size forces MULTIPLE row groups — this is exactly where the rebase
		// bug lived (doc 39: "the rebase math ... breaks" specifically across row-group
		// boundaries; a single-row-group file never exercises it).
		RowGroupSize: 50,
	}

	writer, err := NewStreamingParquetWriter(config)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, writer.Initialize(ctx))

	// Enough distinct string values across enough rows to force multiple pages per column
	// (parquet-go emits an OffsetIndex with multiple PageLocations once a column chunk spans
	// more than one page) AND enough rows to force multiple row groups at RowGroupSize=50.
	for i := range 250 {
		event := map[string]any{
			"ID":    int64(i),
			"Name":  randomishString(i),
			"Value": int64(i * 7),
		}
		require.NoError(t, writer.WriteEvent(ctx, event))
	}
	require.NoError(t, writer.Close(ctx))

	// Reconstruct the final file exactly as S3 would have (concatenated multipart bodies) and
	// parse its footer to walk every row group's column chunks + offset index.
	var finalFile bytes.Buffer
	for _, part := range uploadedParts {
		finalFile.Write(part)
	}

	// Optional: dump the reconstructed file to a real local path (doc 39 Phase 2's "local repro
	// harness" — no S3, no AWS, cross-check the actual bytes with a strict external reader like
	// datafusion-cli/arrow-rs). Opt-in via env var so a normal `go test` run has no side effects.
	if outPath := os.Getenv("BENTO_OFFSET_INDEX_TEST_DUMP_PATH"); outPath != "" {
		require.NoError(t, os.WriteFile(outPath, finalFile.Bytes(), 0o644))
		t.Logf("wrote reconstructed file to %s (%d bytes)", outPath, finalFile.Len())
	}

	rowGroups, offsetIndexByRowGroupAndCol := decodeFooterForTest(t, finalFile.Bytes())

	violations := 0
	for rgIdx, rg := range rowGroups {
		for colIdx, col := range rg.Columns {
			chunkStart := col.MetaData.DataPageOffset
			if col.MetaData.DictionaryPageOffset != 0 && col.MetaData.DictionaryPageOffset < chunkStart {
				chunkStart = col.MetaData.DictionaryPageOffset
			}

			oi, ok := offsetIndexByRowGroupAndCol[[2]int{rgIdx, colIdx}]
			if !ok || len(oi.PageLocations) == 0 {
				continue // column has no offset index (e.g. all-null or too small to page) — nothing to check
			}

			firstPageOffset := oi.PageLocations[0].Offset
			if firstPageOffset < chunkStart {
				violations++
				t.Errorf("row group %d, col %d (%v): offset_index.first_page_offset=%d < chunk_start=%d (delta=%d) — this is the doc 39 underflow bug",
					rgIdx, colIdx, col.MetaData.PathInSchema, firstPageOffset, chunkStart, firstPageOffset-chunkStart)
			}
		}
	}

	require.Zero(t, violations, "found %d column chunks violating the OffsetIndex >= chunk_start invariant", violations)
}

// randomishString gives each row a distinct-enough string value that column stats/pages don't
// collapse into one page for the whole row group.
func randomishString(i int) string {
	suffixes := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"}
	return suffixes[i%len(suffixes)] + "-" + suffixes[(i*7)%len(suffixes)]
}

// decodeFooterForTest parses a complete Parquet file's footer and returns each row group's
// column chunk metadata plus a lookup of decoded OffsetIndex by (row group index, column index).
// Mirrors the footer-location logic output_aws_s3_parquet_stream_writer.go itself uses when
// re-parsing a temp buffer's footer (PAR1 header, footer length + trailing PAR1 magic).
func decodeFooterForTest(t *testing.T, data []byte) ([]format.RowGroup, map[[2]int]format.OffsetIndex) {
	t.Helper()
	require.GreaterOrEqual(t, len(data), 12, "file too short to have a valid footer")

	footerSizeBytes := data[len(data)-8 : len(data)-4]
	footerSize := int(footerSizeBytes[0]) | int(footerSizeBytes[1])<<8 |
		int(footerSizeBytes[2])<<16 | int(footerSizeBytes[3])<<24

	footerStart := len(data) - footerSize - 8
	require.GreaterOrEqual(t, footerStart, 4, "computed footer start is before the file's own PAR1 header")

	var fileMeta format.FileMetaData
	require.NoError(t, thrift.Unmarshal(new(thrift.CompactProtocol), data[footerStart:len(data)-8], &fileMeta))

	offsetIndexes := make(map[[2]int]format.OffsetIndex)
	for rgIdx, rg := range fileMeta.RowGroups {
		for colIdx, col := range rg.Columns {
			if col.OffsetIndexOffset <= 0 || col.OffsetIndexLength <= 0 {
				continue
			}
			raw := data[col.OffsetIndexOffset : col.OffsetIndexOffset+int64(col.OffsetIndexLength)]
			var oi format.OffsetIndex
			require.NoError(t, thrift.Unmarshal(new(thrift.CompactProtocol), raw, &oi),
				"row group %d col %d: failed to decode OffsetIndex", rgIdx, colIdx)
			offsetIndexes[[2]int{rgIdx, colIdx}] = oi
		}
	}

	return fileMeta.RowGroups, offsetIndexes
}
