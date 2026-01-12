package main_test

import (
	"embed"
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
	testDataFS                 embed.FS
	pluginWasm, pluginManifest []byte
)

func init() {
	pluginWasm, _ = testDataFS.ReadFile("testdata/plugin.wasm")
	pluginManifest, _ = testDataFS.ReadFile("testdata/plugin.yaml")
}

func TestCSVPluginRegister(t *testing.T) {
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

	_, exists := env.GetDocs("rust_parse_csv", docs.TypeProcessor)
	require.True(t, exists)
}

func TestCSVPluginExecute(t *testing.T) {
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

	conf, err := testutil.ProcessorFromYAML(`
rust_parse_csv: {}
`)
	require.NoError(t, err)

	p, err := mgr.NewProcessor(conf)
	require.NoError(t, err)

	csvInput := `name,age,city
Alice,30,NYC
Bob,25,LA`

	inputData := [][]byte{[]byte(csvInput)}
	inBatch := message.QuickBatch(inputData)

	res, err := p.ProcessBatch(t.Context(), inBatch)
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Len(t, res[0], 1)

	structured, err := res[0][0].AsStructured()
	require.NoError(t, err)

	result, ok := structured.([]any)
	require.True(t, ok, "expected array of objects")
	require.Len(t, result, 2)

	row0, ok := result[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "Alice", row0["name"])
	require.Equal(t, "30", row0["age"])
	require.Equal(t, "NYC", row0["city"])

	row1, ok := result[1].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "Bob", row1["name"])
	require.Equal(t, "25", row1["age"])
	require.Equal(t, "LA", row1["city"])

}
