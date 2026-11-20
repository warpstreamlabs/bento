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

	ctx := t.Context()
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
	ctx := t.Context()

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
	ctx := t.Context()

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

	ctx, cancel := context.WithTimeout(t.Context(), time.Second*30)
	defer cancel()
	require.NoError(t, strm.Run(ctx))

	assert.Equal(t, map[string]string{
		"foo": "foo_value",
		"bar": "bar_value",
	}, received)
}

func TestCacheInputStreamDeleteAck(t *testing.T) {
	listableTestCache := &listableTestCache{
		cache: map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		},
	}

	env := service.NewEnvironment()
	require.NoError(t, env.RegisterCache("test_listable_cache", testCacheConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			return listableTestCache, nil
		},
	))

	sb := env.NewStreamBuilder()
	require.NoError(t, sb.AddCacheYAML(`
label: foocache
test_listable_cache: {}
`))
	require.NoError(t, sb.AddInputYAML(`
cache:
  resource: foocache
  delete_items: true
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

	ctx, cancel := context.WithTimeout(t.Context(), time.Second*30)
	defer cancel()
	require.NoError(t, strm.Run(ctx))
	require.Equal(t, map[string]string{"a": "1", "b": "2", "c": "3"}, received)

	assert.Equal(t, map[string]string{}, listableTestCache.cache)
}

func TestCacheInputStreamDeleteNack(t *testing.T) {
	listableTestCache := &listableTestCache{
		cache: map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		},
	}

	env := service.NewEnvironment()
	require.NoError(t, env.RegisterCache("test_listable_cache", testCacheConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			return listableTestCache, nil
		},
	))

	sb := env.NewStreamBuilder()
	require.NoError(t, sb.AddCacheYAML(`
label: foocache
test_listable_cache: {}
`))
	require.NoError(t, sb.AddInputYAML(`
cache:
  resource: foocache
  delete_items: true
  auto_replay_nacks: false
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
		return errors.New("delivery failure")
	}))

	strm, err := sb.Build()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second*20)
	defer cancel()

	require.NoError(t, strm.Run(ctx))
	mu.Lock()
	assert.Equal(t, map[string]string{"a": "1", "b": "2", "c": "3"}, received)
	mu.Unlock()
	assert.Equal(t, map[string]string{"a": "1", "b": "2", "c": "3"}, listableTestCache.cache)
}

//------------------------------------------------------------------------------

var testCacheConfigSpec = service.NewConfigSpec().Summary("A listable test cache")

type listableTestCache struct {
	cache   map[string]string
	cacheMu sync.Mutex
}

func (ltc *listableTestCache) Get(ctx context.Context, key string) ([]byte, error) {
	ltc.cacheMu.Lock()
	defer ltc.cacheMu.Unlock()
	return []byte(ltc.cache[key]), nil
}

func (ltc *listableTestCache) Exists(ctx context.Context, key string) (bool, error) {
	return false, nil
}

func (ltc *listableTestCache) Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	return nil
}

func (ltc *listableTestCache) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	return nil
}

func (ltc *listableTestCache) Keys(_ context.Context) service.KeyIterator {
	ltc.cacheMu.Lock()
	defer ltc.cacheMu.Unlock()
	return func(yield func(string, error) bool) {
		for k := range ltc.cache {
			if !yield(k, nil) {
				return
			}
		}
	}
}

func (ltc *listableTestCache) Delete(ctx context.Context, key string) error {
	ltc.cacheMu.Lock()
	defer ltc.cacheMu.Unlock()
	delete(ltc.cache, key)
	return nil
}

func (ltc *listableTestCache) Close(ctx context.Context) error {
	return nil
}
