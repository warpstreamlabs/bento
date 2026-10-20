package main_test

import (
	"embed"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/public/service"

	// Enable the WASM plugin runtime.
	_ "github.com/warpstreamlabs/bento/public/components/wasm"
)

//go:embed plugin
var pluginFS embed.FS

func TestMain(m *testing.M) {
	tokenizerWasm, _ := pluginFS.ReadFile("plugin/plugin.wasm")
	if len(tokenizerWasm) == 0 {
		fmt.Println("skipping plugin tests: requires plugin/plugin.wasm, built with build.sh")
		os.Exit(0)
	}

	tokenizerManifest, err := pluginFS.ReadFile("plugin/plugin.yaml")
	if err != nil {
		log.Fatal(err)
	}

	// NOTE: we are registering to the global environment here.
	if err := service.RegisterWasmPlugin(string(tokenizerManifest), tokenizerWasm); err != nil {
		log.Fatal(err)
	}

	os.Exit(m.Run())
}

func newTokenizeProcessor(t *testing.T, conf string) processor.V1 {
	t.Helper()

	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	pConf, err := testutil.ProcessorFromYAML(conf)
	require.NoError(t, err)

	proc, err := mgr.NewProcessor(pConf)
	require.NoError(t, err)
	t.Cleanup(func() { _ = proc.Close(t.Context()) })

	return proc
}

func TestTokenizeProcessor(t *testing.T) {
	tests := []struct {
		name     string
		conf     string
		input    string
		expected string
	}{
		{
			name: "with special tokens",
			conf: `
hf_tokenizer:
  path: testdata/tokenizer.json
`,
			input:    "Hello world !",
			expected: `{"attention_mask":[1,1,1,1,1],"ids":[1,3,4,5,2],"tokens":["[CLS]","hello","world","!","[SEP]"]}`,
		},
		{
			name: "without special tokens",
			conf: `
hf_tokenizer:
  path: testdata/tokenizer.json
  add_special_tokens: false
`,
			input:    "hello unknown",
			expected: `{"attention_mask":[1,1],"ids":[3,0],"tokens":["hello","[UNK]"]}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := newTokenizeProcessor(t, test.conf)

			batches, err := proc.ProcessBatch(t.Context(), message.QuickBatch([][]byte{[]byte(test.input)}))
			require.NoError(t, err)
			require.Len(t, batches, 1)
			require.Len(t, batches[0], 1)
			require.NoError(t, batches[0][0].ErrorGet())

			assert.JSONEq(t, test.expected, string(batches[0][0].AsBytes()))
		})
	}
}

func TestTokenizeProcessorMissingFile(t *testing.T) {
	proc := newTokenizeProcessor(t, `
hf_tokenizer:
  path: testdata/does_not_exist.json
`)

	batches, err := proc.ProcessBatch(t.Context(), message.QuickBatch([][]byte{[]byte("hello")}))
	require.NoError(t, err)
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 1)
	require.ErrorContains(t, batches[0][0].ErrorGet(), "failed to load tokenizer")
}
