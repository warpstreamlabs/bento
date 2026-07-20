package pure

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

func readCacheInputItems(t *testing.T, in *cacheInput) map[string]string {
	t.Helper()

	ctx := context.Background()
	received := map[string]string{}
	for {
		msg, ackFn, err := in.Read(ctx)
		if errors.Is(err, service.ErrEndOfInput) {
			return received
		}
		require.NoError(t, err)

		b, err := msg.AsBytes()
		require.NoError(t, err)

		key, ok := msg.MetaGet("cache_key")
		require.True(t, ok)

		received[key] = string(b)
		require.NoError(t, ackFn(ctx, nil))
	}
}

func TestCacheInputReadsAllItems(t *testing.T) {
	ctx := context.Background()

	res := service.MockResources(service.MockResourcesOptAddCache("foocache"))
	require.NoError(t, res.AccessCache(ctx, "foocache", func(c service.Cache) {
		require.NoError(t, c.Set(ctx, "a", []byte("1"), nil))
		require.NoError(t, c.Set(ctx, "b", []byte("2"), nil))
		require.NoError(t, c.Set(ctx, "c", []byte("3"), nil))
	}))

	conf, err := cacheInputSpec().ParseYAML(`resource: foocache`, nil)
	require.NoError(t, err)

	in, err := newCacheInputFromConfig(conf, res)
	require.NoError(t, err)
	require.NoError(t, in.Connect(ctx))

	assert.Equal(t, map[string]string{
		"a": "1",
		"b": "2",
		"c": "3",
	}, readCacheInputItems(t, in))
	require.NoError(t, in.Close(ctx))
}

func TestCacheInputSkipsItemsDeletedAfterConnect(t *testing.T) {
	ctx := context.Background()

	res := service.MockResources(service.MockResourcesOptAddCache("foocache"))
	require.NoError(t, res.AccessCache(ctx, "foocache", func(c service.Cache) {
		require.NoError(t, c.Set(ctx, "a", []byte("1"), nil))
		require.NoError(t, c.Set(ctx, "b", []byte("2"), nil))
	}))

	conf, err := cacheInputSpec().ParseYAML(`resource: foocache`, nil)
	require.NoError(t, err)

	in, err := newCacheInputFromConfig(conf, res)
	require.NoError(t, err)
	require.NoError(t, in.Connect(ctx))

	require.NoError(t, res.AccessCache(ctx, "foocache", func(c service.Cache) {
		require.NoError(t, c.Delete(ctx, "b"))
	}))

	assert.Equal(t, map[string]string{
		"a": "1",
	}, readCacheInputItems(t, in))
}

func TestCacheInputResourceMissing(t *testing.T) {
	conf, err := cacheInputSpec().ParseYAML(`resource: foocache`, nil)
	require.NoError(t, err)

	_, err = newCacheInputFromConfig(conf, service.MockResources())
	require.EqualError(t, err, "cache resource 'foocache' was not found")
}

func TestCacheInputStream(t *testing.T) {
	sb := service.NewStreamBuilder()
	require.NoError(t, sb.SetYAML(`
input:
  cache:
    resource: foocache

output:
  drop: {}

cache_resources:
  - label: foocache
    memory:
      init_values:
        foo: foo_value
        bar: bar_value
`))

	var mu sync.Mutex
	received := map[string]string{}
	require.NoError(t, sb.AddConsumerFunc(func(ctx context.Context, m *service.Message) error {
		b, err := m.AsBytes()
		if err != nil {
			return err
		}
		key, _ := m.MetaGet("cache_key")
		mu.Lock()
		received[key] = string(b)
		mu.Unlock()
		return nil
	}))

	strm, err := sb.Build()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	require.NoError(t, strm.Run(ctx))

	assert.Equal(t, map[string]string{
		"foo": "foo_value",
		"bar": "bar_value",
	}, received)
}
