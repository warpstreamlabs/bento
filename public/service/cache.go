package service

import (
	"context"
	"errors"
	"time"

	"github.com/warpstreamlabs/bento/internal/component"
	"github.com/warpstreamlabs/bento/internal/component/cache"
	"github.com/warpstreamlabs/bento/internal/component/metrics"
)

// Errors returned by cache types.
var (
	ErrKeyAlreadyExists = errors.New("key already exists")
	ErrKeyNotFound      = errors.New("key does not exist")
)

// Cache is an interface implemented by Bento caches.
type Cache interface {
	// Get a cache item.
	Get(ctx context.Context, key string) ([]byte, error)

	// Set a cache item, specifying an optional TTL. It is okay for caches to
	// ignore the ttl parameter if it isn't possible to implement.
	Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error

	// Add is the same operation as Set except that it returns an error if the
	// key already exists. It is okay for caches to return nil on duplicates if
	// it isn't possible to implement.
	Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error

	// Delete attempts to remove a key. If the key does not exist then it is
	// considered correct to return an error, however, for cache implementations
	// where it is difficult to determine this then it is acceptable to return
	// nil.
	Delete(ctx context.Context, key string) error

	Closer
}

// ListableCache is an optional interface that can be implemented by caches
// that are capable of enumerating the keys they hold. Not all caches are able
// to support this, so in order to detect whether a cache obtained from
// *Resources.AccessCache is listable perform a type assertion against this
// interface.
type ListableCache interface {
	Cache

	// ListKeys returns a slice of all keys currently held by the cache. The
	// order of the returned keys is not guaranteed.
	ListKeys(ctx context.Context) ([]string, error)
}

// CacheItem represents an individual cache item.
type CacheItem struct {
	Key   string
	Value []byte
	TTL   *time.Duration
}

// batchedCache represents a cache where the underlying implementation is able
// to benefit from batched set requests. This interface is optional for caches
// and when implemented will automatically be utilised where possible.
type batchedCache interface {
	// SetMulti attempts to set multiple cache items in as few requests as
	// possible.
	SetMulti(ctx context.Context, keyValues ...CacheItem) error
}

//------------------------------------------------------------------------------

// Implements types.Cache.
type airGapCache struct {
	c  Cache
	cm batchedCache
}

func newAirGapCache(c Cache, stats metrics.Type) cache.V1 {
	ag := &airGapCache{c: c, cm: nil}
	ag.cm, _ = c.(batchedCache)
	var v1 cache.V1 = ag
	if lc, ok := c.(ListableCache); ok {
		v1 = &listableAirGapCache{airGapCache: ag, l: lc}
	}
	return cache.MetricsForCache(v1, stats)
}

func (a *airGapCache) Get(ctx context.Context, key string) ([]byte, error) {
	b, err := a.c.Get(ctx, key)
	if errors.Is(err, ErrKeyNotFound) {
		err = component.ErrKeyNotFound
	}
	return b, err
}

func (a *airGapCache) Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	return a.c.Set(ctx, key, value, ttl)
}

func (a *airGapCache) SetMulti(ctx context.Context, keyValues map[string]cache.TTLItem) error {
	if a.cm != nil {
		items := make([]CacheItem, 0, len(keyValues))
		for k, v := range keyValues {
			items = append(items, CacheItem{
				Key:   k,
				Value: v.Value,
				TTL:   v.TTL,
			})
		}
		return a.cm.SetMulti(ctx, items...)
	}
	for k, v := range keyValues {
		if err := a.c.Set(ctx, k, v.Value, v.TTL); err != nil {
			return err
		}
	}
	return nil
}

func (a *airGapCache) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	err := a.c.Add(ctx, key, value, ttl)
	if errors.Is(err, ErrKeyAlreadyExists) {
		err = component.ErrKeyAlreadyExists
	}
	return err
}

func (a *airGapCache) Delete(ctx context.Context, key string) error {
	return a.c.Delete(ctx, key)
}

func (a *airGapCache) Close(ctx context.Context) error {
	return a.c.Close(ctx)
}

// Implements types.Cache and cache.KeyLister around a ListableCache.
type listableAirGapCache struct {
	*airGapCache
	l ListableCache
}

func (a *listableAirGapCache) ListKeys(ctx context.Context) ([]string, error) {
	return a.l.ListKeys(ctx)
}

//------------------------------------------------------------------------------

// Implements Cache around a types.Cache.
type reverseAirGapCache struct {
	c cache.V1
}

func newReverseAirGapCache(c cache.V1) Cache {
	rag := &reverseAirGapCache{c}
	if kl, ok := c.(cache.KeyLister); ok {
		return &reverseAirGapListableCache{reverseAirGapCache: rag, kl: kl}
	}
	return rag
}

func (r *reverseAirGapCache) Get(ctx context.Context, key string) ([]byte, error) {
	b, err := r.c.Get(ctx, key)
	if errors.Is(err, component.ErrKeyNotFound) {
		err = ErrKeyNotFound
	}
	return b, err
}

func (r *reverseAirGapCache) Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	return r.c.Set(ctx, key, value, ttl)
}

func (r *reverseAirGapCache) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) (err error) {
	if err = r.c.Add(ctx, key, value, ttl); errors.Is(err, component.ErrKeyAlreadyExists) {
		err = ErrKeyAlreadyExists
	}
	return
}

func (r *reverseAirGapCache) Delete(ctx context.Context, key string) error {
	return r.c.Delete(ctx, key)
}

func (r *reverseAirGapCache) Close(ctx context.Context) error {
	return r.c.Close(ctx)
}

// Implements ListableCache around a types.Cache that also implements
// cache.KeyLister.
type reverseAirGapListableCache struct {
	*reverseAirGapCache
	kl cache.KeyLister
}

func (r *reverseAirGapListableCache) ListKeys(ctx context.Context) ([]string, error) {
	return r.kl.ListKeys(ctx)
}
