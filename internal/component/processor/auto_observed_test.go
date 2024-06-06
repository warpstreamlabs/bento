package processor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/message"
)

type fnProcessor struct {
	fn     func(context.Context, *message.Part) ([]*message.Part, error)
	closed bool

	sync.Mutex
}

func (p *fnProcessor) Process(ctx context.Context, msg *message.Part) ([]*message.Part, error) {
	return p.fn(ctx, msg)
}

func (p *fnProcessor) Close(ctx context.Context) error {
	p.Lock()
	p.closed = true
	p.Unlock()
	return nil
}

func TestProcessorAirGapShutdown(t *testing.T) {
	rp := &fnProcessor{}
	agrp := NewAutoObservedProcessor("foo", rp, component.NoopObservability())

	ctx, done := context.WithTimeout(context.Background(), time.Microsecond*5)
	defer done()

	err := agrp.Close(ctx)
	assert.NoError(t, err)
	rp.Lock()
	assert.True(t, rp.closed)
	rp.Unlock()
}

func TestProcessorAirGapOneToOne(t *testing.T) {
	tCtx := context.Background()

	agrp := NewAutoObservedProcessor("foo", &fnProcessor{
		fn: func(c context.Context, m *message.Part) ([]*message.Part, error) {
			if b := m.AsBytes(); string(b) != "unchanged" {
				return nil, errors.New("nope")
			}
			newPart := m.ShallowCopy()
			newPart.SetBytes([]byte("changed"))
			return []*message.Part{newPart}, nil
		},
	}, component.NoopObservability())

	msg := message.QuickBatch([][]byte{[]byte("unchanged")})
	msgs, res := agrp.ProcessBatch(tCtx, msg)
	require.NoError(t, res)
	require.Len(t, msgs, 1)
	assert.Equal(t, 1, msgs[0].Len())
	assert.Equal(t, "changed", string(msgs[0].Get(0).AsBytes()))
	assert.Equal(t, "unchanged", string(msg.Get(0).AsBytes()))
}

func TestProcessorAirGapOneToError(t *testing.T) {
	tCtx := context.Background()

	agrp := NewAutoObservedProcessor("foo", &fnProcessor{
		fn: func(c context.Context, m *message.Part) ([]*message.Part, error) {
			_, err := m.AsStructuredMut()
			return nil, err
		},
	}, component.NoopObservability())

	msg := message.QuickBatch([][]byte{[]byte("not a structured doc")})
	msgs, res := agrp.ProcessBatch(tCtx, msg)
	require.NoError(t, res)
	require.Len(t, msgs, 1)
	assert.Equal(t, 1, msgs[0].Len())
	assert.Equal(t, "not a structured doc", string(msgs[0].Get(0).AsBytes()))
	assert.Equal(t, "not a structured doc", string(msgs[0].Get(0).AsBytes()))
	assert.EqualError(t, msgs[0].Get(0).ErrorGet(), "invalid character 'o' in literal null (expecting 'u')")
}

func TestProcessorAirGapOneToMany(t *testing.T) {
	tCtx := context.Background()

	agrp := NewAutoObservedProcessor("foo", &fnProcessor{
		fn: func(c context.Context, m *message.Part) ([]*message.Part, error) {
			if b := m.AsBytes(); string(b) != "unchanged" {
				return nil, errors.New("nope")
			}
			first := m.ShallowCopy()
			second := m.ShallowCopy()
			third := m.ShallowCopy()
			first.SetBytes([]byte("changed 1"))
			second.SetBytes([]byte("changed 2"))
			third.SetBytes([]byte("changed 3"))
			return []*message.Part{first, second, third}, nil
		},
	}, component.NoopObservability())

	msg := message.QuickBatch([][]byte{[]byte("unchanged")})
	msgs, res := agrp.ProcessBatch(tCtx, msg)
	require.NoError(t, res)
	require.Len(t, msgs, 1)
	assert.Equal(t, 3, msgs[0].Len())
	assert.Equal(t, "changed 1", string(msgs[0].Get(0).AsBytes()))
	assert.Equal(t, "changed 2", string(msgs[0].Get(1).AsBytes()))
	assert.Equal(t, "changed 3", string(msgs[0].Get(2).AsBytes()))
	assert.Equal(t, "unchanged", string(msg.Get(0).AsBytes()))
}

//------------------------------------------------------------------------------

type fnBatchProcessor struct {
	fn     func(*BatchProcContext, message.Batch) ([]message.Batch, error)
	closed bool
}

func (p *fnBatchProcessor) ProcessBatch(ctx *BatchProcContext, batch message.Batch) ([]message.Batch, error) {
	return p.fn(ctx, batch)
}

func (p *fnBatchProcessor) Close(ctx context.Context) error {
	p.closed = true
	return nil
}

func TestBatchProcessorAirGapShutdown(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Millisecond*5)
	defer done()

	rp := &fnBatchProcessor{}
	agrp := NewAutoObservedBatchedProcessor("foo", rp, component.NoopObservability())

	err := agrp.Close(tCtx)
	assert.NoError(t, err)
	assert.True(t, rp.closed)
}

func TestBatchProcessorAirGapOneToOne(t *testing.T) {
	tCtx := context.Background()

	agrp := NewAutoObservedBatchedProcessor("foo", &fnBatchProcessor{
		fn: func(c *BatchProcContext, msgs message.Batch) ([]message.Batch, error) {
			if b := msgs.Get(0).AsBytes(); string(b) != "unchanged" {
				return nil, errors.New("nope")
			}
			newMsg := msgs.Get(0).ShallowCopy()
			newMsg.SetBytes([]byte("changed"))
			return []message.Batch{{newMsg}}, nil
		},
	}, component.NoopObservability())

	msg := message.QuickBatch([][]byte{[]byte("unchanged")})
	msgs, res := agrp.ProcessBatch(tCtx, msg)
	require.NoError(t, res)
	require.Len(t, msgs, 1)
	assert.Equal(t, 1, msgs[0].Len())
	assert.Equal(t, "changed", string(msgs[0].Get(0).AsBytes()))
	assert.Equal(t, "unchanged", string(msg.Get(0).AsBytes()))
}

func TestBatchProcessorAirGapOneToError(t *testing.T) {
	tCtx := context.Background()

	agrp := NewAutoObservedBatchedProcessor("foo", &fnBatchProcessor{
		fn: func(c *BatchProcContext, msgs message.Batch) ([]message.Batch, error) {
			_, err := msgs.Get(0).AsStructuredMut()
			return nil, err
		},
	}, component.NoopObservability())

	msg := message.QuickBatch([][]byte{[]byte("not a structured doc")})
	msgs, res := agrp.ProcessBatch(tCtx, msg)
	require.NoError(t, res)
	require.Len(t, msgs, 1)
	assert.Equal(t, 1, msgs[0].Len())
	assert.Equal(t, "not a structured doc", string(msgs[0].Get(0).AsBytes()))
	assert.Equal(t, "not a structured doc", string(msgs[0].Get(0).AsBytes()))
	assert.EqualError(t, msgs[0].Get(0).ErrorGet(), "invalid character 'o' in literal null (expecting 'u')")
}

func TestBatchProcessorAirGapOneToMany(t *testing.T) {
	tCtx := context.Background()

	agrp := NewAutoObservedBatchedProcessor("foo", &fnBatchProcessor{
		fn: func(c *BatchProcContext, msgs message.Batch) ([]message.Batch, error) {
			if b := msgs.Get(0).AsBytes(); string(b) != "unchanged" {
				return nil, errors.New("nope")
			}
			first := msgs.Get(0).ShallowCopy()
			second := msgs.Get(0).ShallowCopy()
			third := msgs.Get(0).ShallowCopy()
			first.SetBytes([]byte("changed 1"))
			second.SetBytes([]byte("changed 2"))
			third.SetBytes([]byte("changed 3"))

			firstBatch := message.Batch{first, second}
			secondBatch := message.Batch{third}
			return []message.Batch{firstBatch, secondBatch}, nil
		},
	}, component.NoopObservability())

	msg := message.QuickBatch([][]byte{[]byte("unchanged")})
	msgs, res := agrp.ProcessBatch(tCtx, msg)
	require.NoError(t, res)
	require.Len(t, msgs, 2)
	assert.Equal(t, "unchanged", string(msg.Get(0).AsBytes()))

	assert.Equal(t, 2, msgs[0].Len())
	assert.Equal(t, "changed 1", string(msgs[0].Get(0).AsBytes()))
	assert.Equal(t, "changed 2", string(msgs[0].Get(1).AsBytes()))

	assert.Equal(t, 1, msgs[1].Len())
	assert.Equal(t, "changed 3", string(msgs[1].Get(0).AsBytes()))
}

func TestBatchProcessorAirGapIndividualErrors(t *testing.T) {
	tCtx := context.Background()

	agrp := NewAutoObservedBatchedProcessor("foo", &fnBatchProcessor{
		fn: func(c *BatchProcContext, msgs message.Batch) ([]message.Batch, error) {
			for i, m := range msgs {
				if _, err := m.AsStructuredMut(); err != nil {
					c.OnError(err, i, nil)
				}
			}
			return []message.Batch{msgs}, nil
		},
	}, component.NoopObservability())

	msg := message.QuickBatch([][]byte{
		[]byte("not a structured doc"),
		[]byte(`{"foo":"bar"}`),
		[]byte("abcdefg"),
	})

	msgs, err := agrp.ProcessBatch(tCtx, msg)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Len(t, msgs[0], 3)

	assert.Equal(t, "not a structured doc", string(msgs[0][0].AsBytes()))
	assert.Equal(t, `{"foo":"bar"}`, string(msgs[0][1].AsBytes()))
	assert.Equal(t, "abcdefg", string(msgs[0][2].AsBytes()))

	assert.EqualError(t, msgs[0][0].ErrorGet(), "invalid character 'o' in literal null (expecting 'u')")
	assert.NoError(t, msgs[0][1].ErrorGet())
	assert.EqualError(t, msgs[0][2].ErrorGet(), "invalid character 'a' looking for beginning of value")
}
