package parquet

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

// TestParquetStrictSchemaInput tests the strict_schema configuration for the parquet input
func TestParquetStrictSchemaInput(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "parquet_strict_schema")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.RemoveAll(tmpDir)
	})

	// Create a parquet file with non-standard LIST format
	// This simulates files like those from AWS Security Lake
	nonStandardSchema := parquet.NewSchema("test", parquet.Group{
		"id":   parquet.Int(64),
		"name": parquet.String(),
		// Non-standard LIST: repeated element directly without the .list.element wrapper
		"tags": parquet.Repeated(parquet.String()),
	})

	buf := bytes.NewBuffer(nil)
	pWtr := parquet.NewGenericWriter[any](buf, nonStandardSchema)

	testData := []any{
		map[string]any{
			"id":   int64(1),
			"name": "test1",
			"tags": []any{"tag1", "tag2", "tag3"},
		},
		map[string]any{
			"id":   int64(2),
			"name": "test2",
			"tags": []any{"tag4", "tag5"},
		},
	}

	_, err = pWtr.Write(testData)
	require.NoError(t, err)
	require.NoError(t, pWtr.Close())

	filePath := filepath.Join(tmpDir, "non_standard.parquet")
	require.NoError(t, os.WriteFile(filePath, buf.Bytes(), 0o644))

	t.Run("strict_schema true should handle standard files", func(t *testing.T) {
		// Create a standard parquet file
		type standardData struct {
			ID    int64
			Value string
		}

		standardBuf := bytes.NewBuffer(nil)
		standardWtr := parquet.NewWriter(standardBuf, parquet.SchemaOf(standardData{}))
		require.NoError(t, standardWtr.Write(standardData{ID: 1, Value: "test"}))
		require.NoError(t, standardWtr.Close())

		standardPath := filepath.Join(tmpDir, "standard.parquet")
		require.NoError(t, os.WriteFile(standardPath, standardBuf.Bytes(), 0o644))

		conf, err := parquetInputConfig().ParseYAML(fmt.Sprintf(`
paths: [ "%v/standard.parquet" ]
strict_schema: true
`, tmpDir), nil)
		require.NoError(t, err)

		in, err := newParquetInputFromConfig(conf, service.MockResources())
		require.NoError(t, err)

		tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
		defer done()

		b, _, err := in.ReadBatch(tCtx)
		require.NoError(t, err)
		require.Len(t, b, 1)

		mBytes, err := b[0].AsBytes()
		require.NoError(t, err)
		assert.JSONEq(t, `{"ID":1,"Value":"test"}`, string(mBytes))

		require.NoError(t, in.Close(tCtx))
	})

	t.Run("strict_schema false should handle non-standard files", func(t *testing.T) {
		conf, err := parquetInputConfig().ParseYAML(fmt.Sprintf(`
paths: [ "%v/non_standard.parquet" ]
strict_schema: false
`, tmpDir), nil)
		require.NoError(t, err)

		in, err := newParquetInputFromConfig(conf, service.MockResources())
		require.NoError(t, err)

		tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
		defer done()

		b, _, err := in.ReadBatch(tCtx)
		require.NoError(t, err)
		require.Len(t, b, 1)

		// Verify the data was read correctly
		result, err := b[0].AsStructured()
		require.NoError(t, err)
		resultMap := result.(map[string]any)
		assert.Equal(t, int64(1), resultMap["id"])
		assert.Equal(t, "test1", resultMap["name"])
		assert.Equal(t, []any{"tag1", "tag2", "tag3"}, resultMap["tags"])

		b, _, err = in.ReadBatch(tCtx)
		require.NoError(t, err)
		require.Len(t, b, 1)

		result, err = b[0].AsStructured()
		require.NoError(t, err)
		resultMap = result.(map[string]any)
		assert.Equal(t, int64(2), resultMap["id"])
		assert.Equal(t, "test2", resultMap["name"])
		assert.Equal(t, []any{"tag4", "tag5"}, resultMap["tags"])

		require.NoError(t, in.Close(tCtx))
	})
}

// TestParquetStrictSchemaProcessor tests the strict_schema configuration for the parquet_decode processor
func TestParquetStrictSchemaProcessor(t *testing.T) {
	t.Run("strict_schema false should decode non-standard files", func(t *testing.T) {
		// Create a parquet file with non-standard LIST format
		nonStandardSchema := parquet.NewSchema("test", parquet.Group{
			"id":      parquet.Int(64),
			"message": parquet.String(),
			// Non-standard LIST format
			"values": parquet.Repeated(parquet.Int(64)),
		})

		buf := bytes.NewBuffer(nil)
		pWtr := parquet.NewGenericWriter[any](buf, nonStandardSchema)

		testData := []any{
			map[string]any{
				"id":      int64(100),
				"message": "hello",
				"values":  []any{int64(1), int64(2), int64(3)},
			},
		}

		_, err := pWtr.Write(testData)
		require.NoError(t, err)
		require.NoError(t, pWtr.Close())

		// Test with strict_schema: false
		processor := &parquetDecodeProcessor{
			strictSchema: false,
		}

		resBatch, err := processor.Process(context.Background(), service.NewMessage(buf.Bytes()))
		require.NoError(t, err)
		require.Len(t, resBatch, 1)

		result, err := resBatch[0].AsStructured()
		require.NoError(t, err)
		resultMap := result.(map[string]any)
		assert.Equal(t, int64(100), resultMap["id"])
		assert.Equal(t, "hello", resultMap["message"])
		assert.Equal(t, []any{int64(1), int64(2), int64(3)}, resultMap["values"])
	})

	t.Run("strict_schema true should decode standard files", func(t *testing.T) {
		type standardType struct {
			ID   int64
			Name string
		}

		buf := bytes.NewBuffer(nil)
		pWtr := parquet.NewGenericWriter[standardType](buf)

		_, err := pWtr.Write([]standardType{{ID: 42, Name: "test"}})
		require.NoError(t, err)
		require.NoError(t, pWtr.Close())

		// Test with strict_schema: true (default behavior)
		processor := &parquetDecodeProcessor{
			strictSchema: true,
		}

		resBatch, err := processor.Process(context.Background(), service.NewMessage(buf.Bytes()))
		require.NoError(t, err)
		require.Len(t, resBatch, 1)

		mBytes, err := resBatch[0].AsBytes()
		require.NoError(t, err)
		assert.JSONEq(t, `{"ID":42,"Name":"test"}`, string(mBytes))
	})
}

// TestParquetStrictSchemaDefault verifies that strict_schema defaults to true
func TestParquetStrictSchemaDefault(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "parquet_default")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.RemoveAll(tmpDir)
	})

	type simpleData struct {
		ID    int64
		Value string
	}

	buf := bytes.NewBuffer(nil)
	pWtr := parquet.NewWriter(buf, parquet.SchemaOf(simpleData{}))
	require.NoError(t, pWtr.Write(simpleData{ID: 1, Value: "default"}))
	require.NoError(t, pWtr.Close())

	filePath := filepath.Join(tmpDir, "test.parquet")
	require.NoError(t, os.WriteFile(filePath, buf.Bytes(), 0o644))

	// Test without specifying strict_schema (should default to true)
	conf, err := parquetInputConfig().ParseYAML(fmt.Sprintf(`
paths: [ "%v/test.parquet" ]
`, tmpDir), nil)
	require.NoError(t, err)

	in, err := newParquetInputFromConfig(conf, service.MockResources())
	require.NoError(t, err)

	// Verify the reader was created with strict_schema: true
	reader := in.(*parquetReader)
	assert.True(t, reader.strictSchema, "strict_schema should default to true")

	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	b, _, err := in.ReadBatch(tCtx)
	require.NoError(t, err)
	require.Len(t, b, 1)

	require.NoError(t, in.Close(tCtx))
}
