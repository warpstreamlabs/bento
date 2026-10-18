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
	wazerov1 "github.com/warpstreamlabs/bento/internal/plugin/runtime/wazero_v1"
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

func TestJavaPluginRegister(t *testing.T) {
	if len(pluginManifest) == 0 || len(pluginWasm) == 0 {
		t.Skip("skipping plugin tests: requires plugin.wasm and plugin.yaml in testdata/")
	}

	rt := wazerov1.NewPluginRuntime()
	defer rt.Close(t.Context())

	manifest, lints, err := runtime.ReadManifestYAML(pluginManifest)
	require.Empty(t, lints)
	require.NoError(t, err)

	compiledPlugin, err := rt.Register(manifest, runtime.ByteSource(pluginWasm))
	require.NoError(t, err)

	env := bundle.NewEnvironment()
	require.NoError(t, compiledPlugin.RegisterWith(env))

	_, exists := env.GetDocs("java_uppercase", docs.TypeProcessor)
	require.True(t, exists)
}

func TestJavaPluginExecute(t *testing.T) {
	if len(pluginManifest) == 0 || len(pluginWasm) == 0 {
		t.Skip("skipping plugin tests: requires plugin.wasm and plugin.yaml in testdata/")
	}

	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	rt := wazerov1.NewPluginRuntime()
	defer rt.Close(t.Context())

	manifest, _, err := runtime.ReadManifestYAML(pluginManifest)
	require.NoError(t, err)

	compiledPlugin, err := rt.Register(manifest, runtime.ByteSource(pluginWasm))
	require.NoError(t, err)
	require.NoError(t, compiledPlugin.RegisterWith(mgr.Environment()))

	conf, err := testutil.ProcessorFromYAML("java_uppercase: {}")
	require.NoError(t, err)

	proc, err := mgr.NewProcessor(conf)
	require.NoError(t, err)

	batch := message.QuickBatch([][]byte{
		[]byte("hello world"),
		[]byte("héllo wörld"),
		[]byte(""),
		nil,
	})
	batch[0].MetaSetMut("source", "test")
	batch[3].SetStructured(map[string]any{"hello": "world"})

	res, err := proc.ProcessBatch(t.Context(), batch)
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Len(t, res[0], 4)

	for _, part := range res[0] {
		require.NoError(t, part.ErrorGet())
	}
	require.Equal(t, "HELLO WORLD", string(res[0][0].AsBytes()))
	require.Equal(t, "HÉLLO WÖRLD", string(res[0][1].AsBytes()))
	require.Empty(t, res[0][2].AsBytes())

	// Only raw content is uppercased, and structured content is unchanged.
	require.JSONEq(t, `{"hello": "world"}`, string(res[0][3].AsBytes()))

	// Metadata is kept, as the plugin returns the same number of parts.
	require.Equal(t, "test", res[0][0].MetaGetStr("source"))
}
