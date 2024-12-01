package strict_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/bundle/strict"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager"
	"github.com/warpstreamlabs/bento/internal/message"

	_ "github.com/warpstreamlabs/bento/internal/impl/pure"
)

func TestStrictBundleProcessor(t *testing.T) {
	senv := strict.StrictBundle(bundle.GlobalEnvironment)
	tCtx := context.Background()

	pConf, err := testutil.ProcessorFromYAML(`
bloblang: root = this
`)
	require.NoError(t, err)

	mgr, err := manager.New(
		manager.ResourceConfig{},
		manager.OptSetEnvironment(senv),
	)
	require.NoError(t, err)

	proc, err := mgr.NewProcessor(pConf)
	require.NoError(t, err)

	msg := message.QuickBatch([][]byte{[]byte("not a structured doc")})
	msgs, res := proc.ProcessBatch(tCtx, msg)
	require.Empty(t, msgs)
	require.Error(t, res)
	assert.ErrorContains(t, res, "invalid character 'o' in literal null (expecting 'u')")

	msg = message.QuickBatch([][]byte{[]byte(`{"hello":"world"}`)})
	msgs, res = proc.ProcessBatch(tCtx, msg)
	require.NoError(t, res)
	require.Len(t, msgs, 1)
	assert.Equal(t, 1, msgs[0].Len())
	assert.Equal(t, `{"hello":"world"}`, string(msgs[0].Get(0).AsBytes()))
}

func TestStrictBundleProcessorMultiMessage(t *testing.T) {
	senv := strict.StrictBundle(bundle.GlobalEnvironment)
	tCtx := context.Background()

	pConf, err := testutil.ProcessorFromYAML(`
bloblang: root = this
`)
	require.NoError(t, err)

	mgr, err := manager.New(
		manager.ResourceConfig{},
		manager.OptSetEnvironment(senv),
	)
	require.NoError(t, err)

	proc, err := mgr.NewProcessor(pConf)
	require.NoError(t, err)

	msg := message.QuickBatch([][]byte{
		[]byte("not a structured doc"),
		[]byte(`{"foo":"oof"}`),
		[]byte(`{"bar":"rab"}`),
	})
	msgs, res := proc.ProcessBatch(tCtx, msg)
	require.Empty(t, msgs)
	require.Error(t, res)
	assert.ErrorContains(t, res, "invalid character 'o' in literal null (expecting 'u')")

	// Ensure the ordering of the message does not influence the error message
	msg = message.QuickBatch([][]byte{
		[]byte(`{"foo":"oof"}`),
		[]byte("not a structured doc"),
		[]byte(`{"bar":"rab"}`),
	})
	msgs, res = proc.ProcessBatch(tCtx, msg)
	require.Empty(t, msgs)
	require.Error(t, res)
	assert.ErrorContains(t, res, "invalid character 'o' in literal null (expecting 'u')")

	// Multiple errored messages
	msg = message.QuickBatch([][]byte{
		[]byte(`{"foo":"oof"}`),
		[]byte("not a structured doc"),
		[]byte(`another unstructred doc`),
	})
	msgs, res = proc.ProcessBatch(tCtx, msg)
	require.Empty(t, msgs)
	require.Error(t, res)
	assert.ErrorContains(t, res, "invalid character 'o' in literal null (expecting 'u')")
}
