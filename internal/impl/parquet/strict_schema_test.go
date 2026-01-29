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

// TestNestedStructSupport tests that strict_schema: false properly handles nested structs
func TestNestedStructSupport(t *testing.T) {
	t.Run("simple 2-level nested struct", func(t *testing.T) {
		// Create a schema with nested struct: cloud.account.uid
		nestedSchema := parquet.NewSchema("test", parquet.Group{
			"id": parquet.Int(64),
			"cloud": parquet.Group{
				"account": parquet.Group{
					"uid": parquet.String(),
				},
			},
		})

		buf := bytes.NewBuffer(nil)
		pWtr := parquet.NewGenericWriter[any](buf, nestedSchema)

		testData := []any{
			map[string]any{
				"id": int64(1),
				"cloud": map[string]any{
					"account": map[string]any{
						"uid": "353785743975",
					},
				},
			},
			map[string]any{
				"id": int64(2),
				"cloud": map[string]any{
					"account": map[string]any{
						"uid": "123456789012",
					},
				},
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
		require.Len(t, resBatch, 2)

		// Verify first record
		result, err := resBatch[0].AsStructured()
		require.NoError(t, err)
		resultMap := result.(map[string]any)
		assert.Equal(t, int64(1), resultMap["id"])

		// Check nested structure
		cloud, ok := resultMap["cloud"].(map[string]any)
		require.True(t, ok, "cloud should be a map")
		account, ok := cloud["account"].(map[string]any)
		require.True(t, ok, "account should be a map")
		assert.Equal(t, "353785743975", account["uid"])

		// Verify second record
		result, err = resBatch[1].AsStructured()
		require.NoError(t, err)
		resultMap = result.(map[string]any)
		assert.Equal(t, int64(2), resultMap["id"])

		cloud, ok = resultMap["cloud"].(map[string]any)
		require.True(t, ok)
		account, ok = cloud["account"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "123456789012", account["uid"])
	})

	t.Run("3-level nested struct with multiple fields", func(t *testing.T) {
		// Create a deeper nested structure
		deepSchema := parquet.NewSchema("test", parquet.Group{
			"id": parquet.Int(64),
			"metadata": parquet.Group{
				"product": parquet.Group{
					"name":    parquet.String(),
					"version": parquet.String(),
				},
			},
			"time": parquet.Int(64),
		})

		buf := bytes.NewBuffer(nil)
		pWtr := parquet.NewGenericWriter[any](buf, deepSchema)

		testData := []any{
			map[string]any{
				"id": int64(100),
				"metadata": map[string]any{
					"product": map[string]any{
						"name":    "TestProduct",
						"version": "1.0.0",
					},
				},
				"time": int64(1234567890),
			},
		}

		_, err := pWtr.Write(testData)
		require.NoError(t, err)
		require.NoError(t, pWtr.Close())

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
		assert.Equal(t, int64(1234567890), resultMap["time"])

		metadata, ok := resultMap["metadata"].(map[string]any)
		require.True(t, ok)
		product, ok := metadata["product"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "TestProduct", product["name"])
		assert.Equal(t, "1.0.0", product["version"])
	})

	t.Run("complex multi-level nested structs", func(t *testing.T) {
		// Test multiple nested structs at the same level with various field types
		complexSchema := parquet.NewSchema("test", parquet.Group{
			"time": parquet.Int(64),
			"cloud": parquet.Group{
				"account": parquet.Group{
					"uid": parquet.String(),
				},
				"region": parquet.String(),
			},
			"src_endpoint": parquet.Group{
				"ip":   parquet.String(),
				"port": parquet.Int(32),
			},
			"dst_endpoint": parquet.Group{
				"ip":   parquet.String(),
				"port": parquet.Int(32),
			},
			"traffic": parquet.Group{
				"bytes":   parquet.Int(64),
				"packets": parquet.Int(64),
			},
		})

		buf := bytes.NewBuffer(nil)
		pWtr := parquet.NewGenericWriter[any](buf, complexSchema)

		testData := []any{
			map[string]any{
				"time": int64(1737561600000),
				"cloud": map[string]any{
					"account": map[string]any{
						"uid": "353785743975",
					},
					"region": "us-west-2",
				},
				"src_endpoint": map[string]any{
					"ip":   "10.0.1.100",
					"port": int32(443),
				},
				"dst_endpoint": map[string]any{
					"ip":   "10.0.2.200",
					"port": int32(80),
				},
				"traffic": map[string]any{
					"bytes":   int64(1024),
					"packets": int64(10),
				},
			},
		}

		_, err := pWtr.Write(testData)
		require.NoError(t, err)
		require.NoError(t, pWtr.Close())

		processor := &parquetDecodeProcessor{
			strictSchema: false,
		}

		resBatch, err := processor.Process(context.Background(), service.NewMessage(buf.Bytes()))
		require.NoError(t, err)
		require.Len(t, resBatch, 1)

		result, err := resBatch[0].AsStructured()
		require.NoError(t, err)
		resultMap := result.(map[string]any)

		// Verify all nested structures are properly reconstructed
		assert.Equal(t, int64(1737561600000), resultMap["time"])

		cloud, ok := resultMap["cloud"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "us-west-2", cloud["region"])
		account, ok := cloud["account"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "353785743975", account["uid"])

		srcEndpoint, ok := resultMap["src_endpoint"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "10.0.1.100", srcEndpoint["ip"])
		assert.Equal(t, int32(443), srcEndpoint["port"])

		dstEndpoint, ok := resultMap["dst_endpoint"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "10.0.2.200", dstEndpoint["ip"])
		assert.Equal(t, int32(80), dstEndpoint["port"])

		traffic, ok := resultMap["traffic"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, int64(1024), traffic["bytes"])
		assert.Equal(t, int64(10), traffic["packets"])
	})

	t.Run("nested struct with arrays", func(t *testing.T) {
		// Test nested struct with repeated fields
		schema := parquet.NewSchema("test", parquet.Group{
			"id": parquet.Int(64),
			"metadata": parquet.Group{
				"tags": parquet.Repeated(parquet.String()),
			},
		})

		buf := bytes.NewBuffer(nil)
		pWtr := parquet.NewGenericWriter[any](buf, schema)

		testData := []any{
			map[string]any{
				"id": int64(1),
				"metadata": map[string]any{
					"tags": []any{"production", "critical", "monitored"},
				},
			},
		}

		_, err := pWtr.Write(testData)
		require.NoError(t, err)
		require.NoError(t, pWtr.Close())

		processor := &parquetDecodeProcessor{
			strictSchema: false,
		}

		resBatch, err := processor.Process(context.Background(), service.NewMessage(buf.Bytes()))
		require.NoError(t, err)
		require.Len(t, resBatch, 1)

		result, err := resBatch[0].AsStructured()
		require.NoError(t, err)
		resultMap := result.(map[string]any)

		metadata, ok := resultMap["metadata"].(map[string]any)
		require.True(t, ok)
		tags, ok := metadata["tags"].([]any)
		require.True(t, ok)
		assert.Equal(t, []any{"production", "critical", "monitored"}, tags)
	})
}

// TestStandardParquetGoListEncoding tests that strict_schema: false properly handles
// standard parquet-go LIST encoding which uses .list.element wrapper
func TestStandardParquetGoListEncoding(t *testing.T) {
	t.Run("standard list encoding with multiple elements", func(t *testing.T) {
		// Define a Go struct with slice field - parquet-go will create .list.element wrapper
		type TestRecord struct {
			ID       int64    `parquet:"id"`
			Profiles []string `parquet:"profiles,list"`
		}

		buf := bytes.NewBuffer(nil)
		writer := parquet.NewGenericWriter[TestRecord](buf)

		testData := []TestRecord{
			{ID: 1, Profiles: []string{"cloud", "security_control"}},
			{ID: 2, Profiles: []string{"cloud"}}, // Single element
			{ID: 3, Profiles: []string{}},        // Empty array
		}

		_, err := writer.Write(testData)
		require.NoError(t, err)
		require.NoError(t, writer.Close())

		processor := &parquetDecodeProcessor{
			strictSchema: false,
		}

		resBatch, err := processor.Process(context.Background(), service.NewMessage(buf.Bytes()))
		require.NoError(t, err)
		require.Len(t, resBatch, 3)

		// Record 1: Multiple elements
		result, err := resBatch[0].AsStructured()
		require.NoError(t, err)
		resultMap := result.(map[string]any)
		assert.Equal(t, int64(1), resultMap["id"])
		profiles, ok := resultMap["profiles"].([]any)
		require.True(t, ok, "profiles should be an array")
		assert.Equal(t, []any{"cloud", "security_control"}, profiles)

		// Record 2: Single element (should still be array, not string)
		result, err = resBatch[1].AsStructured()
		require.NoError(t, err)
		resultMap = result.(map[string]any)
		assert.Equal(t, int64(2), resultMap["id"])
		profiles, ok = resultMap["profiles"].([]any)
		require.True(t, ok, "single-element profiles should still be an array")
		assert.Equal(t, []any{"cloud"}, profiles)

		// Record 3: Empty array (should be null in OCSF style)
		result, err = resBatch[2].AsStructured()
		require.NoError(t, err)
		resultMap = result.(map[string]any)
		assert.Equal(t, int64(3), resultMap["id"])
		assert.Nil(t, resultMap["profiles"], "empty array should be null")
	})

	t.Run("nested struct with standard list encoding", func(t *testing.T) {
		type Metadata struct {
			Version  string   `parquet:"version"`
			Profiles []string `parquet:"profiles,list"`
		}

		type TestRecord struct {
			ID       int64    `parquet:"id"`
			Metadata Metadata `parquet:"metadata"`
		}

		buf := bytes.NewBuffer(nil)
		writer := parquet.NewGenericWriter[TestRecord](buf)

		testData := []TestRecord{
			{
				ID: 1,
				Metadata: Metadata{
					Version:  "1.7.0",
					Profiles: []string{"cloud", "security_control", "datetime"},
				},
			},
		}

		_, err := writer.Write(testData)
		require.NoError(t, err)
		require.NoError(t, writer.Close())

		processor := &parquetDecodeProcessor{
			strictSchema: false,
		}

		resBatch, err := processor.Process(context.Background(), service.NewMessage(buf.Bytes()))
		require.NoError(t, err)
		require.Len(t, resBatch, 1)

		result, err := resBatch[0].AsStructured()
		require.NoError(t, err)
		resultMap := result.(map[string]any)

		metadata, ok := resultMap["metadata"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "1.7.0", metadata["version"])

		profiles, ok := metadata["profiles"].([]any)
		require.True(t, ok, "nested profiles should be properly unwrapped array")
		assert.Equal(t, []any{"cloud", "security_control", "datetime"}, profiles)
	})
}

// TestAWSSecurityLakeArrayEncoding tests handling of AWS Security Lake's non-standard
// .array wrapper pattern
func TestAWSSecurityLakeArrayEncoding(t *testing.T) {
	t.Run("non-standard array wrapper", func(t *testing.T) {
		// Manually create AWS Security Lake style schema with .array wrapper
		schema := parquet.NewSchema("test", parquet.Group{
			"id": parquet.Int(64),
			"metadata": parquet.Group{
				"profiles": parquet.Group{
					"array": parquet.Repeated(parquet.String()),
				},
			},
		})

		buf := bytes.NewBuffer(nil)
		writer := parquet.NewGenericWriter[any](buf, schema)

		testData := []any{
			map[string]any{
				"id": int64(1),
				"metadata": map[string]any{
					"profiles": map[string]any{
						"array": []any{"cloud", "security_control"},
					},
				},
			},
		}

		_, err := writer.Write(testData)
		require.NoError(t, err)
		require.NoError(t, writer.Close())

		processor := &parquetDecodeProcessor{
			strictSchema: false,
		}

		resBatch, err := processor.Process(context.Background(), service.NewMessage(buf.Bytes()))
		require.NoError(t, err)
		require.Len(t, resBatch, 1)

		result, err := resBatch[0].AsStructured()
		require.NoError(t, err)
		resultMap := result.(map[string]any)

		metadata, ok := resultMap["metadata"].(map[string]any)
		require.True(t, ok)

		// Should unwrap .array wrapper
		profiles, ok := metadata["profiles"].([]any)
		require.True(t, ok, "AWS Security Lake .array wrapper should be unwrapped")
		assert.Equal(t, []any{"cloud", "security_control"}, profiles)
	})
}
