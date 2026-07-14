package main_test

import (
	"embed"
	"fmt"
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

func TestReversePluginRegister(t *testing.T) {
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

	_, exists := env.GetDocs("reverse", docs.TypeProcessor)
	require.True(t, exists)
}

func TestReversePluginExecute(t *testing.T) {
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
reverse: {}
`)
	require.NoError(t, err)

	p, err := mgr.NewProcessor(conf)
	require.NoError(t, err)

	inputData := [][]byte{[]byte("hello"), []byte("cruel"), []byte("world")}
	inBatch := message.QuickBatch(inputData)

	expectedData := [][]byte{[]byte("world"), []byte("cruel"), []byte("hello")}

	res, err := p.ProcessBatch(t.Context(), inBatch)
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Len(t, res[0], 3)
	require.Equal(t, expectedData, message.GetAllBytes(res[0]))

	for i, msg := range res[0] {
		meta := msg.MetaGetStr("processed_by")
		require.Equal(t, "reverse_wasm", meta)

		metaPos := msg.MetaGetStr("reversed_position")
		expectedMetaPos := fmt.Sprintf("%d", len(res[0])-1-i)
		require.Equal(t, expectedMetaPos, metaPos)
	}
}
