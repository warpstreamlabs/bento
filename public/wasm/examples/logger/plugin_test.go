package main_test

import (
	"bytes"
	"embed"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/log"
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

func newProcessor(t *testing.T, loggerEnabled bool) (*bytes.Buffer, error) {
	t.Helper()
	if len(pluginManifest) == 0 || len(pluginWasm) == 0 {
		t.Skip("skipping plugin tests: requires plugin.wasm and plugin.yaml in testdata/")
	}

	buf := &bytes.Buffer{}
	logConf := log.NewConfig()
	logConf.Format = "logfmt"
	logger, err := log.New(buf, nil, logConf)
	require.NoError(t, err)

	mgr, err := manager.New(manager.NewResourceConfig(), manager.OptSetLogger(logger))
	require.NoError(t, err)

	rt := wazerov1.NewPluginRuntime()
	t.Cleanup(func() { _ = rt.Close(t.Context()) })

	manifest, _, err := runtime.ReadManifestYAML(pluginManifest)
	require.NoError(t, err)
	manifest.Runtime.Wasm.Logger.Enabled = loggerEnabled

	compiledPlugin, err := rt.Register(manifest, runtime.ByteSource(pluginWasm))
	require.NoError(t, err)
	require.NoError(t, compiledPlugin.RegisterWith(mgr.Environment()))

	conf, err := testutil.ProcessorFromYAML(`
label: my_logger
logger: {}
`)
	require.NoError(t, err)

	p, err := mgr.NewProcessor(conf)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = p.Close(t.Context()) })

	res, err := p.ProcessBatch(t.Context(), message.QuickBatch([][]byte{[]byte("hello"), []byte("world")}))
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("hello"), []byte("world")}, message.GetAllBytes(res[0]))

	return buf, nil
}

func TestPluginLogs(t *testing.T) {
	buf, err := newProcessor(t, true)
	require.NoError(t, err)

	logs := buf.String()
	require.Contains(t, logs, `msg="logger plugin initialised"`)
	require.Contains(t, logs, `msg="processing message: hello"`)
	require.Contains(t, logs, `msg="processing message: world"`)
	require.Contains(t, logs, `label=my_logger`)
}

func TestPluginLoggerNotEnabled(t *testing.T) {
	_, err := newProcessor(t, false)
	require.ErrorContains(t, err, "wasm guest plugin imports host modules not enabled in its manifest: [bento_log_v1]")
}
