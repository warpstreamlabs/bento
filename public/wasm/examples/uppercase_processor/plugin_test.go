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

// TODO(gregfurman): Write tests with public/service packages to allow easier plugin testing.

var (
	//go:embed testdata
	testDataFS                 embed.FS
	pluginWasm, pluginManifest []byte
)

func init() {
	pluginWasm, _ = testDataFS.ReadFile("testdata/plugin.wasm")
	pluginManifest, _ = testDataFS.ReadFile("testdata/plugin.yaml")
}

func TestPluginRegister(t *testing.T) {
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

	_, exists := env.GetDocs("uppercase", docs.TypeProcessor)
	require.True(t, exists)
}

func TestPluginExecute(t *testing.T) {
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
uppercase:
  prefix: ">> "
`)

	p, err := mgr.NewProcessor(conf)
	require.NoError(t, err)

	inputData := [][]byte{[]byte("hello"), []byte("cruel"), []byte("world")}
	inBatch := message.QuickBatch(inputData)

	expectedData := [][]byte{[]byte(">> HELLO"), []byte(">> CRUEL"), []byte(">> WORLD")}

	res, err := p.ProcessBatch(t.Context(), inBatch)
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Len(t, res[0], 3)

	require.Equal(t, message.GetAllBytes(res[0]), expectedData)

	for _, msg := range res[0] {
		meta := msg.MetaGetStr("processed_by")
		require.Equal(t, meta, "uppercase_wasm")
	}
}
