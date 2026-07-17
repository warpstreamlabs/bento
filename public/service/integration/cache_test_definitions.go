package integration

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component"
	componentcache "github.com/warpstreamlabs/bento/internal/component/cache"
)

// CacheTestOpenClose checks that the cache can be started, an item added, and
// then stopped.
func CacheTestOpenClose() CacheTestDefinition {
	return namedCacheTest(
		"can open and close",
		func(t *testing.T, env *cacheTestEnvironment) {
			cache := initCache(t, env)
			t.Cleanup(func() {
				closeCache(t, cache)
			})

			require.NoError(t, cache.Add(env.ctx, "foo", []byte("bar"), nil))

			res, err := cache.Get(env.ctx, "foo")
			require.NoError(t, err)
			assert.Equal(t, "bar", string(res))
		},
	)
}

// CacheTestMissingKey checks that we get an error on missing key.
func CacheTestMissingKey() CacheTestDefinition {
	return namedCacheTest(
		"return consistent error on missing key",
		func(t *testing.T, env *cacheTestEnvironment) {
			cache := initCache(t, env)
			t.Cleanup(func() {
				closeCache(t, cache)
			})

			_, err := cache.Get(env.ctx, "missingkey")
			assert.EqualError(t, err, "key does not exist")
		},
	)
}

// CacheTestDoubleAdd ensures that a double add returns an error.
func CacheTestDoubleAdd() CacheTestDefinition {
	return namedCacheTest(
		"add with duplicate key fails",
		func(t *testing.T, env *cacheTestEnvironment) {
			cache := initCache(t, env)
			t.Cleanup(func() {
				closeCache(t, cache)
			})

			require.NoError(t, cache.Add(env.ctx, "addkey", []byte("first"), nil))

			assert.Eventually(t, func() bool {
				return errors.Is(cache.Add(env.ctx, "addkey", []byte("second"), nil), component.ErrKeyAlreadyExists)
			}, time.Minute, time.Second)

			res, err := cache.Get(env.ctx, "addkey")
			require.NoError(t, err)
			assert.Equal(t, "first", string(res))
		},
	)
}

// CacheTestDelete checks that deletes work.
func CacheTestDelete() CacheTestDefinition {
	return namedCacheTest(
		"can set and delete keys",
		func(t *testing.T, env *cacheTestEnvironment) {
			cache := initCache(t, env)
			t.Cleanup(func() {
				closeCache(t, cache)
			})

			require.NoError(t, cache.Add(env.ctx, "addkey", []byte("first"), nil))

			res, err := cache.Get(env.ctx, "addkey")
			require.NoError(t, err)
			assert.Equal(t, "first", string(res))

			require.NoError(t, cache.Delete(env.ctx, "addkey"))

			_, err = cache.Get(env.ctx, "addkey")
			require.EqualError(t, err, "key does not exist")
		},
	)
}

// CacheTestListKeys checks that a cache implementing the optional KeyLister
// interface enumerates the keys it holds after n items are set.
func CacheTestListKeys(n int) CacheTestDefinition {
	return namedCacheTest(
		"can list keys",
		func(t *testing.T, env *cacheTestEnvironment) {
			cache := initCache(t, env)
			t.Cleanup(func() {
				closeCache(t, cache)
			})

			kl, ok := cache.(componentcache.KeyLister)
			require.True(t, ok, "cache does not support listing keys")

			expKeys := make([]string, 0, n)
			for i := range n {
				key := fmt.Sprintf("listkey%v", i)
				require.NoError(t, cache.Set(env.ctx, key, fmt.Appendf(nil, "value%v", i), nil))
				expKeys = append(expKeys, key)
			}

			keys, err := kl.ListKeys(env.ctx)
			require.NoError(t, err)
			assert.ElementsMatch(t, expKeys, keys)
		},
	)
}

// CacheTestGetAndSet checks that we can set and then get n items.
func CacheTestGetAndSet(n int) CacheTestDefinition {
	return namedCacheTest(
		"can get and set",
		func(t *testing.T, env *cacheTestEnvironment) {
			cache := initCache(t, env)
			t.Cleanup(func() {
				closeCache(t, cache)
			})

			for i := range n {
				key := fmt.Sprintf("key%v", i)
				value := fmt.Sprintf("value%v", i)
				require.NoError(t, cache.Set(env.ctx, key, []byte(value), nil))
			}

			for i := range n {
				key := fmt.Sprintf("key%v", i)
				value := fmt.Sprintf("value%v", i)

				res, err := cache.Get(env.ctx, key)
				require.NoError(t, err)
				assert.Equal(t, value, string(res))
			}
		},
	)
}
