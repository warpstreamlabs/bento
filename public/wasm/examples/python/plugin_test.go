package main_test

import (
	"embed"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/manager"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/plugin/runtime"
	extismv1 "github.com/warpstreamlabs/bento/internal/plugin/runtime/extism_v1"
)

var (
	//go:embed testdata
	testDataFS embed.FS

	pluginWasm, pluginManifest []byte
)

func init() {
	pluginWasm, _ = testDataFS.ReadFile("testdata/plugin.wasm")
	pluginManifest, _ = testDataFS.ReadFile("testdata/plugin.yaml")
}

func getID(t *testing.T, result map[string]any) int64 {
	t.Helper()
	switch v := result["id"].(type) {
	case json.Number:
		n, err := v.Int64()
		require.NoError(t, err)
		return n
	case float64:
		return int64(v)
	default:
		t.Fatalf("unexpected type for id: %T", v)
		return 0
	}
}

func TestHTTPProcessorPluginRegister(t *testing.T) {
	if len(pluginManifest) == 0 || len(pluginWasm) == 0 {
		t.Skip("skipping plugin tests: requires plugin.wasm and plugin.yaml in testdata/")
	}

	rt := extismv1.NewPluginRuntime()
	defer rt.Close(t.Context())

	manifest, _, err := runtime.ReadManifestYAML(pluginManifest)
	require.NoError(t, err)

	compiledPlugin, err := rt.Register(t.Context(), manifest, runtime.ByteSource(pluginWasm))
	require.NoError(t, err)

	env := bundle.NewEnvironment()
	err = compiledPlugin.RegisterWith(env)
	require.NoError(t, err)

	_, exists := env.GetDocs("http_processor", docs.TypeProcessor)
	require.True(t, exists)
}

func TestHTTPProcessorPluginExecute(t *testing.T) {
	if len(pluginManifest) == 0 || len(pluginWasm) == 0 {
		t.Skip("skipping plugin tests: requires plugin.wasm and plugin.yaml in testdata/")
	}

	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	rt := extismv1.NewPluginRuntime()
	defer rt.Close(t.Context())

	manifest, _, err := runtime.ReadManifestYAML(pluginManifest)
	require.NoError(t, err)

	compiledPlugin, err := rt.Register(t.Context(), manifest, runtime.ByteSource(pluginWasm))
	require.NoError(t, err)

	err = compiledPlugin.RegisterWith(mgr.Environment())
	require.NoError(t, err)

	t.Run("successful GET request", func(t *testing.T) {
		conf, err := testutil.ProcessorFromYAML(`
http_processor:
  url: "https://jsonplaceholder.typicode.com/todos/1"
  method: "GET"
`)
		require.NoError(t, err)

		p, err := mgr.NewProcessor(conf)
		require.NoError(t, err)

		inBatch := message.QuickBatch([][]byte{[]byte(`{}`)})
		res, err := p.ProcessBatch(t.Context(), inBatch)
		require.NoError(t, err)
		require.Len(t, res, 1)
		require.Len(t, res[0], 1)
		require.NoError(t, res[0][0].ErrorGet())

		structured, err := res[0][0].AsStructured()
		require.NoError(t, err)

		result, ok := structured.(map[string]any)
		require.True(t, ok, "expected object response")
		require.EqualValues(t, 1, getID(t, result))
	})

	t.Run("multiple messages in batch", func(t *testing.T) {
		conf, err := testutil.ProcessorFromYAML(`
http_processor:
  url: "https://jsonplaceholder.typicode.com/todos/1"
  method: "GET"
`)
		require.NoError(t, err)

		p, err := mgr.NewProcessor(conf)
		require.NoError(t, err)

		inputs := [][]byte{}
		for range []int{1, 2, 3} {
			inputs = append(inputs, []byte(`{}`))
		}

		res, err := p.ProcessBatch(t.Context(), message.QuickBatch(inputs))
		require.NoError(t, err)
		require.Len(t, res, 1)
		require.Len(t, res[0], 3)

		for _, msg := range res[0] {
			require.NoError(t, msg.ErrorGet())
			structured, err := msg.AsStructured()
			require.NoError(t, err)
			result, ok := structured.(map[string]any)
			require.True(t, ok)
			require.EqualValues(t, 1, getID(t, result))
		}
	})

	t.Run("default method is GET", func(t *testing.T) {
		conf, err := testutil.ProcessorFromYAML(`
http_processor:
  url: "https://jsonplaceholder.typicode.com/todos/1"
`)
		require.NoError(t, err)

		p, err := mgr.NewProcessor(conf)
		require.NoError(t, err)

		res, err := p.ProcessBatch(t.Context(), message.QuickBatch([][]byte{[]byte(`{}`)}))
		require.NoError(t, err)
		require.Len(t, res, 1)
		require.Len(t, res[0], 1)
		require.NoError(t, res[0][0].ErrorGet())

		structured, err := res[0][0].AsStructured()
		require.NoError(t, err)

		result, ok := structured.(map[string]any)
		require.True(t, ok)
		require.EqualValues(t, 1, getID(t, result))
	})

	t.Run("non-200 response sets error on part", func(t *testing.T) {
		conf, err := testutil.ProcessorFromYAML(`
http_processor:
  url: "https://jsonplaceholder.typicode.com/todos/99999"
  method: "GET"
`)
		require.NoError(t, err)

		p, err := mgr.NewProcessor(conf)
		require.NoError(t, err)

		res, err := p.ProcessBatch(t.Context(), message.QuickBatch([][]byte{[]byte(`{}`)}))
		require.NoError(t, err)
		require.Len(t, res, 1)
		require.Len(t, res[0], 1)
		require.Error(t, res[0][0].ErrorGet())
	})
}
