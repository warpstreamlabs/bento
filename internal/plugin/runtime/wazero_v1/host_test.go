package wazerov1

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/eliben/watgo"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/log"
	"github.com/warpstreamlabs/bento/internal/manager"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/plugin/runtime"
	logv1 "github.com/warpstreamlabs/bento/internal/plugin/runtime/wazero_v1/log_v1"
)

func TestHostModule(t *testing.T) {
	t.Run(logv1.ModuleName, func(t *testing.T) {
		src, err := watFiles.ReadFile("testdata/log_v1/log.wat")
		require.NoError(t, err)

		bin, err := watgo.CompileWATToWASM(src)
		require.NoError(t, err)

		manifest, _, err := runtime.ReadManifestYAML(fmt.Appendf(nil, watManifest, "logger: {enabled: true}"))
		require.NoError(t, err)

		rt := NewPluginRuntime()
		t.Cleanup(func() { require.NoError(t, rt.Close(context.Background())) })

		plugin, err := rt.Register(manifest, runtime.ByteSource(bin))
		require.NoError(t, err)

		env := bundle.NewEnvironment()
		require.NoError(t, plugin.RegisterWith(env))

		buf := &bytes.Buffer{}
		logConf := log.Config{Format: "json", LevelName: "level", MessageName: "msg"}
		lgr, err := log.New(buf, nil, logConf)
		require.NoError(t, err)

		mgr, err := manager.New(manager.NewResourceConfig(), manager.OptSetEnvironment(env), manager.OptSetLogger(lgr))
		require.NoError(t, err)

		fooConf := processor.NewConfig()
		fooConf.Label = "foo"
		fooConf.Type = "wat"
		fooConf.Plugin = map[string]any{}

		fooProc, err := mgr.NewProcessor(fooConf)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, fooProc.Close(context.Background())) })

		barConf := processor.NewConfig()
		barConf.Label = "bar"
		barConf.Type = "wat"
		barConf.Plugin = map[string]any{}

		barProc, err := mgr.NewProcessor(barConf)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, barProc.Close(context.Background())) })

		_, err = fooProc.ProcessBatch(t.Context(), message.QuickBatch([][]byte{[]byte("hello")}))
		require.NoError(t, err)

		_, err = barProc.ProcessBatch(t.Context(), message.QuickBatch([][]byte{[]byte("hello")}))
		require.NoError(t, err)

		expected := `
{"label":"foo","level":"info","msg":"hello from wat","plugin":"wat"}
{"label":"bar","level":"info","msg":"hello from wat","plugin":"wat"}
`[1:]

		require.Equal(t, expected, buf.String())
	})
}

func TestHostModuleErrors(t *testing.T) {
	tests := []struct {
		name       string
		wat        string
		wasm       string
		initErr    string
		processErr string
	}{
		{
			name:    "not enabled",
			wat:     "log_v1/log.wat",
			initErr: "wasm guest plugin imports host modules not enabled in its manifest: [bento_log_v1]",
		},
		{
			name:    "unknown module",
			wat:     "host/unknown_module.wat",
			wasm:    "logger: {enabled: true}",
			initErr: "wasm guest plugin imports host modules not enabled in its manifest: [bento_unknown_v1]",
		},
		{
			name:       "wrong signature",
			wat:        "log_v1/wrong_signature.wat",
			wasm:       "logger: {enabled: true}",
			processErr: "import func[bento_log_v1.info]: signature mismatch",
		},
		{
			name:       "out of bounds",
			wat:        "log_v1/out_of_bounds.wat",
			wasm:       "logger: {enabled: true}",
			processErr: `bento_log_v1.info: out-of-bounds read from plugin "wat"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			src, err := watFiles.ReadFile("testdata/" + test.wat)
			require.NoError(t, err)

			bin, err := watgo.CompileWATToWASM(src)
			require.NoError(t, err)

			manifest, _, err := runtime.ReadManifestYAML(fmt.Appendf(nil, watManifest, test.wasm))
			require.NoError(t, err)

			rt := NewPluginRuntime()
			t.Cleanup(func() { require.NoError(t, rt.Close(context.Background())) })

			plugin, err := rt.Register(manifest, runtime.ByteSource(bin))
			require.NoError(t, err)

			env := bundle.NewEnvironment()
			require.NoError(t, plugin.RegisterWith(env))

			mgr, err := manager.New(manager.NewResourceConfig(), manager.OptSetEnvironment(env))
			require.NoError(t, err)

			conf := processor.NewConfig()
			conf.Type = "wat"
			conf.Plugin = map[string]any{}

			proc, err := mgr.NewProcessor(conf)
			if test.initErr != "" {
				require.ErrorContains(t, err, test.initErr)
				return
			}
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, proc.Close(context.Background())) })

			batches, err := proc.ProcessBatch(t.Context(), message.QuickBatch([][]byte{[]byte("hello")}))
			require.NoError(t, err)
			require.ErrorContains(t, batches[0][0].ErrorGet(), test.processErr)
		})
	}
}
