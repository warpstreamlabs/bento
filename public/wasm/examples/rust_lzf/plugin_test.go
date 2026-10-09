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

	// required for TestLZFPluginBatchRoundtrip
	_ "github.com/warpstreamlabs/bento/internal/impl/pure"
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

	manifest, lints, err := runtime.ReadManifestYAML(pluginManifest)
	require.Empty(t, lints)
	require.NoError(t, err)

	compiledPlugin, err := rt.Register(manifest, runtime.ByteSource(pluginWasm))
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

	compiledPlugin, err := rt.Register(manifest, runtime.ByteSource(pluginWasm))
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
		name        string
		operations  []string
		input       []byte
		expected    []byte
		expectedErr string
	}{
		{
			name:       "compress",
			operations: []string{"compress"},
			input:      []byte("hello hello hello hello"),
			expected:   []byte("\x06hello h\xe0\x05\x05\x01lo"),
		},
		{
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
		{
			name:        "compress not possible",
			operations:  []string{"compress"},
			input:       []byte("the future is bright"),
			expectedErr: "the input data cannot be compressed",
		},
		{
			name:        "decompress corrupt",
			operations:  []string{"decompress"},
			input:       []byte("\x05hi"),
			expectedErr: "lzf Decompress failed",
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
				batch = res[0]
			}

			if test.expectedErr != "" {
				require.ErrorContains(t, batch[0].ErrorGet(), test.expectedErr)
				return
			}

			require.NoError(t, batch[0].ErrorGet())
			require.Equal(t, test.expected, batch[0].AsBytes())
		})
	}
}

func TestLZFPluginBatchRoundtrip(t *testing.T) {
	if len(pluginManifest) == 0 || len(pluginWasm) == 0 {
		t.Skip("skipping plugin tests: requires plugin.wasm and plugin.yaml in testdata/")
	}

	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	rt := extismv1.NewPluginRuntime()
	defer rt.Close(t.Context())

	manifest, _, err := runtime.ReadManifestYAML(pluginManifest)
	require.NoError(t, err)

	compiledPlugin, err := rt.Register(manifest, runtime.ByteSource(pluginWasm))
	require.NoError(t, err)

	err = compiledPlugin.RegisterWith(mgr.Environment())
	require.NoError(t, err)

	conf, err := testutil.ProcessorFromYAML(`
try:
  - rust_lzf:
      operation: compress
  - rust_lzf:
      operation: decompress
`)
	require.NoError(t, err)

	proc, err := mgr.NewProcessor(conf)
	require.NoError(t, err)

	// Each part either roundtrips unchanged, or fails compression and keeps
	// its original bytes. `try` skips decompress for parts that already failed,
	// so the compress error is the one that survives.
	parts := []struct {
		input   []byte
		wantErr string
	}{
		{input: []byte("hello hello hello hello")},
		{input: bytes.Repeat([]byte("hello world "), 100)},
		{input: bytes.Repeat([]byte("the future is bright "), 50)},
		{input: []byte("charlie bit my finger"), wantErr: "the input data cannot be compressed"},
	}

	inputs := make([][]byte, len(parts))
	for i, p := range parts {
		inputs[i] = p.input
	}

	res, err := proc.ProcessBatch(t.Context(), message.QuickBatch(inputs))
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Len(t, res[0], len(parts))

	for i, p := range parts {
		out := res[0][i]
		if p.wantErr != "" {
			require.ErrorContains(t, out.ErrorGet(), p.wantErr, "part %d", i)
		} else {
			require.NoError(t, out.ErrorGet(), "part %d", i)
		}
		require.Equal(t, p.input, out.AsBytes(), "part %d", i)
	}
}
