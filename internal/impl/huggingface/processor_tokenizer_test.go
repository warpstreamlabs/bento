package huggingface_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager"
	"github.com/warpstreamlabs/bento/internal/message"

	_ "github.com/warpstreamlabs/bento/internal/impl/huggingface"
)

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
nlp_tokenize:
  path: testdata/tokenizer.json
`,
			input:    "Hello world !",
			expected: `{"attention_mask":[1,1,1,1,1],"ids":[1,3,4,5,2],"tokens":["[CLS]","hello","world","!","[SEP]"]}`,
		},
		{
			name: "without special tokens",
			conf: `
nlp_tokenize:
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
nlp_tokenize:
  path: testdata/does_not_exist.json
`)

	batches, err := proc.ProcessBatch(t.Context(), message.QuickBatch([][]byte{[]byte("hello")}))
	require.NoError(t, err)
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 1)
	require.ErrorContains(t, batches[0][0].ErrorGet(), "failed to load tokenizer")
}
