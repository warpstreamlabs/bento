package pure_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"
)

func TestDedupe(t *testing.T) {
	doc1 := []byte("hello world")
	doc2 := []byte("hello world")
	doc3 := []byte("hello world 2")

	mgr := mock.NewManager()
	mgr.Caches["foocache"] = map[string]mock.CacheItem{}

	conf, err := testutil.ProcessorFromYAML(`
dedupe:
  cache: foocache
  key: ${! content() }
`)
	require.NoError(t, err)

	proc, err := mgr.NewProcessor(conf)
	require.NoError(t, err)

	msgIn := message.QuickBatch([][]byte{doc1})
	msgOut, err := proc.ProcessBatch(context.Background(), msgIn)
	require.NoError(t, err)
	require.Len(t, msgOut, 1)

	msgIn = message.QuickBatch([][]byte{doc2})
	msgOut, err = proc.ProcessBatch(context.Background(), msgIn)
	require.NoError(t, err)
	require.Empty(t, msgOut)

	msgIn = message.QuickBatch([][]byte{doc3})
	msgOut, err = proc.ProcessBatch(context.Background(), msgIn)
	require.NoError(t, err)
	require.Len(t, msgOut, 1)

	mgr.Caches["foocache"] = map[string]mock.CacheItem{}

	proc, err = mgr.NewProcessor(conf)
	require.NoError(t, err)

	msgIn = message.QuickBatch([][]byte{doc1, doc2, doc3})
	msgOut, err = proc.ProcessBatch(context.Background(), msgIn)
	require.NoError(t, err)
	require.Len(t, msgOut, 1)
	assert.Equal(t, 2, msgOut[0].Len())
}

func TestDedupeLIFO(t *testing.T) {
	doc1 := []byte(`{"key": "foo", "value": "bar1"}`)
	doc2 := []byte(`{"key": "foo", "value": "bar2"}`)
	doc3 := []byte(`{"key": "foo", "value": "bar3"}`)

	mgr := mock.NewManager()
	mgr.Caches["foocache"] = map[string]mock.CacheItem{}

	conf, err := testutil.ProcessorFromYAML(`
dedupe:
  cache: foocache
  key: ${!json("key")}
  strategy: LIFO
`)
	require.NoError(t, err)

	proc, err := mgr.NewProcessor(conf)
	require.NoError(t, err)

	ctx := context.Background()
	msgIn := message.QuickBatch([][]byte{doc1, doc2, doc3, doc4})
	msgOut, err := proc.ProcessBatch(ctx, msgIn)
	require.NoError(t, err)
	require.Len(t, msgOut, 1)
	require.Len(t, msgOut[0], 1)
	require.Equal(t, string(doc3), string(msgOut[0].Get(0).AsBytes()))
}

func TestDedupeBadCache(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
dedupe:
  cache: foocache
`)
	require.NoError(t, err)

	mgr := mock.NewManager()
	_, err = mgr.NewProcessor(conf)
	require.Error(t, err)
}

func TestDedupeCacheErrors(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
dedupe:
  cache: foocache
  key: ${! content() }
`)
	require.NoError(t, err)

	mgr := mock.NewManager()
	mgr.Caches["foocache"] = map[string]mock.CacheItem{}

	proc, err := mgr.NewProcessor(conf)
	require.NoError(t, err)

	delete(mgr.Caches, "foocache")

	msgs, err := proc.ProcessBatch(context.Background(), message.QuickBatch([][]byte{[]byte("foo"), []byte("bar")}))
	require.NoError(t, err)
	assert.Empty(t, msgs)

	conf, err = testutil.ProcessorFromYAML(`
dedupe:
  cache: foocache
  key: ${! content() }
  drop_on_err: false
`)
	require.NoError(t, err)
	mgr.Caches["foocache"] = map[string]mock.CacheItem{}

	proc, err = mgr.NewProcessor(conf)
	require.NoError(t, err)

	delete(mgr.Caches, "foocache")

	msgs, err = proc.ProcessBatch(context.Background(), message.QuickBatch([][]byte{[]byte("foo"), []byte("bar")}))
	require.NoError(t, err)
	assert.Len(t, msgs, 1)
}
