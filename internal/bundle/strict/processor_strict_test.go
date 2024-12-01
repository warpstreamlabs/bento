package strict

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/message"
)

//------------------------------------------------------------------------------

type mockProc struct{}

func (m mockProc) ProcessBatch(ctx context.Context, msg message.Batch) ([]message.Batch, error) {
	for _, m := range msg {
		_, err := m.AsStructuredMut()
		m.ErrorSet(err)
	}
	return []message.Batch{msg}, nil
}

func (m mockProc) Close(ctx context.Context) error {
	// Do nothing as our processor doesn't require resource cleanup.
	return nil
}

//------------------------------------------------------------------------------

func TestProcessorWrapWithStrict(t *testing.T) {
	tCtx := context.Background()

	// Wrap the processor with the strict interface
	strictProc := wrapWithStrict(mockProc{})

	msg := message.QuickBatch([][]byte{[]byte("not a structured doc")})
	msgs, res := strictProc.ProcessBatch(tCtx, msg)
	require.Empty(t, msgs)
	require.Error(t, res)
	assert.EqualError(t, res, "invalid character 'o' in literal null (expecting 'u')")

	msg = message.QuickBatch([][]byte{[]byte(`{"hello":"world"}`)})
	msgs, res = strictProc.ProcessBatch(tCtx, msg)
	require.NoError(t, res)
	require.Len(t, msgs, 1)
	assert.Equal(t, 1, msgs[0].Len())
	assert.Equal(t, `{"hello":"world"}`, string(msgs[0].Get(0).AsBytes()))
}

func TestProcessorWrapWithStrictMultiMessage(t *testing.T) {
	tCtx := context.Background()

	// Wrap the processor with the strict interface
	strictProc := wrapWithStrict(mockProc{})

	msg := message.QuickBatch([][]byte{
		[]byte("not a structured doc"),
		[]byte(`{"foo":"oof"}`),
		[]byte(`{"bar":"rab"}`),
	})
	msgs, res := strictProc.ProcessBatch(tCtx, msg)
	require.Empty(t, msgs)
	require.Error(t, res)
	assert.Error(t, res, "invalid character 'o' in literal null (expecting 'u')")

	// Ensure the ordering of the message does not influence the error message
	msg = message.QuickBatch([][]byte{
		[]byte(`{"foo":"oof"}`),
		[]byte("not a structured doc"),
		[]byte(`{"bar":"rab"}`),
	})
	msgs, res = strictProc.ProcessBatch(tCtx, msg)
	require.Empty(t, msgs)
	require.Error(t, res)
	assert.Error(t, res, "invalid character 'o' in literal null (expecting 'u')")

	// Multiple errored messages
	msg = message.QuickBatch([][]byte{
		[]byte(`{"foo":"oof"}`),
		[]byte("not a structured doc"),
		[]byte(`another unstructred doc`),
	})
	msgs, res = strictProc.ProcessBatch(tCtx, msg)
	require.Empty(t, msgs)
	require.Error(t, res)
	assert.Error(t, res, "invalid character 'o' in literal null (expecting 'u')")
}
