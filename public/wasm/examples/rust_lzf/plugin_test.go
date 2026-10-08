package main_test

import (
	"bytes"
	"embed"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/processor"
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

func TestLZFPluginRegister(t *testing.T) {
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

	_, exists := env.GetDocs("rust_lzf", docs.TypeProcessor)
	require.True(t, exists)
}

func TestLZFPluginExecute(t *testing.T) {
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

	procs := map[string]processor.V1{}
	for _, op := range []string{"compress", "decompress"} {
		conf, err := testutil.ProcessorFromYAML("rust_lzf:\n  operation: " + op)
		require.NoError(t, err)

		procs[op], err = mgr.NewProcessor(conf)
		require.NoError(t, err)
	}

	roundtripInput := bytes.Repeat([]byte("hello world "), 100)

	tests := []struct {
		name       string
		operations []string
		input      []byte
		expected   []byte
	}{
		{
			name:       "compress",
			operations: []string{"compress"},
			input:      []byte("hello hello hello hello"),
			expected:   []byte("\x06hello h\xe0\x05\x05\x01lo"),
		},
		{
			// A single LZF literal run: a control byte < 32 means "copy the
			// next ctrl+1 bytes as-is".
			name:       "decompress",
			operations: []string{"decompress"},
			input:      []byte("\x04hello"),
			expected:   []byte("hello"),
		},
		{
			name:       "roundtrip",
			operations: []string{"compress", "decompress"},
			input:      roundtripInput,
			expected:   roundtripInput,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			batch := message.QuickBatch([][]byte{test.input})
			for _, op := range test.operations {
				res, err := procs[op].ProcessBatch(t.Context(), batch)
				require.NoError(t, err)
				require.Len(t, res, 1)
				require.Len(t, res[0], 1)
				require.NoError(t, res[0][0].ErrorGet())
				batch = res[0]
			}
			require.Equal(t, test.expected, batch[0].AsBytes())
		})
	}
}
