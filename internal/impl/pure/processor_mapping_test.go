package pure

import (
	"context"
	"testing"
	"time"

	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/public/bloblang"
)

func TestMappingCreateCrossfire(t *testing.T) {
	tCtx := context.Background()

	inMsg := message.NewPart(nil)
	inMsg.SetStructuredMut(map[string]any{
		"foo": map[string]any{
			"bar": map[string]any{
				"baz": "original value",
				"qux": "dont change",
			},
		},
	})
	inMsg.MetaSetMut("foo", "orig1")
	inMsg.MetaSetMut("bar", "orig2")

	inMsg2 := message.NewPart([]byte(`{}`))

	exec, err := bloblang.Parse(`
foo = json("foo").from(0)
foo.bar_new = "this is swapped now"
foo.bar.baz = "and this changed"
meta foo = meta("foo").from(0)
meta bar = meta("bar").from(0)
meta baz = "new meta"
`)
	require.NoError(t, err)

	proc := newMapping(exec, nil)

	inBatch := message.Batch{inMsg, inMsg2}
	outBatches, err := proc.ProcessBatch(processor.TestBatchProcContext(tCtx, nil, inBatch), inBatch)
	require.NoError(t, err)
	require.Len(t, outBatches, 1)
	require.Len(t, outBatches[0], 2)

	msgBytes := inMsg.AsBytes()
	assert.Equal(t, `{"foo":{"bar":{"baz":"original value","qux":"dont change"}}}`, string(msgBytes))
	v, _ := inMsg.MetaGetMut("foo")
	assert.Equal(t, "orig1", v)
	v, _ = inMsg.MetaGetMut("bar")
	assert.Equal(t, "orig2", v)
	_, exists := inMsg.MetaGetMut("baz")
	assert.False(t, exists)

	msgBytes = inMsg2.AsBytes()
	assert.Equal(t, `{}`, string(msgBytes))
	_, exists = inMsg2.MetaGetMut("foo")
	assert.False(t, exists)
	_, exists = inMsg2.MetaGetMut("bar")
	assert.False(t, exists)
	_, exists = inMsg2.MetaGetMut("baz")
	assert.False(t, exists)

	msgBytes = outBatches[0][0].AsBytes()
	assert.Equal(t, `{"foo":{"bar":{"baz":"and this changed","qux":"dont change"},"bar_new":"this is swapped now"}}`, string(msgBytes))
	v, _ = outBatches[0][0].MetaGetMut("foo")
	assert.Equal(t, "orig1", v)
	v, _ = outBatches[0][0].MetaGetMut("bar")
	assert.Equal(t, "orig2", v)
	v, _ = outBatches[0][0].MetaGetMut("baz")
	assert.Equal(t, "new meta", v)

	msgBytes = outBatches[0][1].AsBytes()
	assert.Equal(t, `{"foo":{"bar":{"baz":"and this changed","qux":"dont change"},"bar_new":"this is swapped now"}}`, string(msgBytes))
	v, _ = outBatches[0][1].MetaGetMut("foo")
	assert.Equal(t, "orig1", v)
	v, _ = outBatches[0][1].MetaGetMut("bar")
	assert.Equal(t, "orig2", v)
	v, _ = outBatches[0][1].MetaGetMut("baz")
	assert.Equal(t, "new meta", v)
}

func TestMappingCreateCustomObject(t *testing.T) {
	tCtx := context.Background()

	part := message.NewPart(nil)

	gObj := gabs.New()
	_, _ = gObj.ArrayOfSize(3, "foos")

	gObjEle := gabs.New()
	_, _ = gObjEle.Set("FROM NEW OBJECT", "foo")

	_, _ = gObj.S("foos").SetIndex(gObjEle.Data(), 0)
	_, _ = gObj.S("foos").SetIndex(5, 1)

	part.SetStructuredMut(gObj.Data())

	exec, err := bloblang.Parse(`root.foos = this.foos`)
	require.NoError(t, err)

	proc := newMapping(exec, nil)

	outBatches, err := proc.ProcessBatch(processor.TestBatchProcContext(tCtx, nil, []*message.Part{part}), message.Batch{part})
	require.NoError(t, err)
	require.Len(t, outBatches, 1)
	require.Len(t, outBatches[0], 1)

	resPartBytes := outBatches[0][0].AsBytes()
	assert.Equal(t, `{"foos":[{"foo":"FROM NEW OBJECT"},5,null]}`, string(resPartBytes))
}

func TestMappingCreateFiltering(t *testing.T) {
	tCtx := context.Background()

	inBatch := message.Batch{
		message.NewPart([]byte(`{"foo":{"delete":true}}`)),
		message.NewPart([]byte(`{"foo":{"dont":"delete me"}}`)),
		message.NewPart([]byte(`{"bar":{"delete":true}}`)),
		message.NewPart([]byte(`{"bar":{"dont":"delete me"}}`)),
	}

	exec, err := bloblang.Parse(`
root = match {
  (foo | bar).delete.or(false) => deleted(),
}
`)
	require.NoError(t, err)

	proc := newMapping(exec, nil)

	outBatches, err := proc.ProcessBatch(processor.TestBatchProcContext(tCtx, nil, inBatch), inBatch)
	require.NoError(t, err)
	require.Len(t, outBatches, 1)
	require.Len(t, outBatches[0], 2)

	assert.NoError(t, outBatches[0][0].ErrorGet())
	assert.NoError(t, outBatches[0][1].ErrorGet())

	msgBytes := outBatches[0][0].AsBytes()
	assert.Equal(t, `{"foo":{"dont":"delete me"}}`, string(msgBytes))

	msgBytes = outBatches[0][1].AsBytes()
	assert.Equal(t, `{"bar":{"dont":"delete me"}}`, string(msgBytes))
}

func TestMappingCreateFilterAll(t *testing.T) {
	tCtx := context.Background()

	inBatch := message.Batch{
		message.NewPart([]byte(`{"foo":{"delete":true}}`)),
		message.NewPart([]byte(`{"foo":{"dont":"delete me"}}`)),
		message.NewPart([]byte(`{"bar":{"delete":true}}`)),
		message.NewPart([]byte(`{"bar":{"dont":"delete me"}}`)),
	}

	exec, err := bloblang.Parse(`root = deleted()`)
	require.NoError(t, err)

	proc := newMapping(exec, nil)

	outBatches, err := proc.ProcessBatch(processor.TestBatchProcContext(tCtx, nil, inBatch), inBatch)
	assert.NoError(t, err)
	assert.Empty(t, outBatches)
}

func TestMappingCreateJSONError(t *testing.T) {
	tCtx := context.Background()

	msg := message.Batch{
		message.NewPart([]byte(`this is not valid json`)),
	}

	exec, err := bloblang.Parse(`foo = json().bar`)
	require.NoError(t, err)

	proc := newMapping(exec, nil)

	outBatches, err := proc.ProcessBatch(processor.TestBatchProcContext(tCtx, nil, msg), msg)
	require.NoError(t, err)
	require.Len(t, outBatches, 1)
	require.Len(t, outBatches[0], 1)

	msgBytes := outBatches[0][0].AsBytes()
	assert.Equal(t, `this is not valid json`, string(msgBytes))

	err = outBatches[0][0].ErrorGet()
	require.Error(t, err)
	assert.Equal(t, `failed assignment (line 1): invalid character 'h' in literal true (expecting 'r')`, err.Error())
}

func BenchmarkMappingBasic(b *testing.B) {
	blobl, err := bloblang.Parse(`
root = this
root.sum = this.a + this.b
`)
	require.NoError(b, err)

	proc := newMapping(blobl, nil)

	tCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	tmpMsg := message.NewPart(nil)
	tmpMsg.SetStructured(map[string]any{
		"a": 5,
		"b": 7,
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		resBatches, err := proc.ProcessBatch(processor.TestBatchProcContext(tCtx, nil, nil), message.Batch{tmpMsg.ShallowCopy()})
		require.NoError(b, err)
		require.Len(b, resBatches, 1)
		require.Len(b, resBatches[0], 1)

		v, err := resBatches[0][0].AsStructured()
		require.NoError(b, err)
		assert.Equal(b, int64(12), v.(map[string]any)["sum"])
	}

	require.NoError(b, proc.Close(tCtx))
}
