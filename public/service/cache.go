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
	ErrKeyAlreadyExists       = errors.New("key already exists")
	ErrKeyNotFound            = errors.New("key does not exist")
	ErrKeyListingNotSupported = errors.New("cache does not support listing keys")
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

// CacheItem represents an individual cache item.
type CacheItem struct {
	Key   string
	Value []byte
	TTL   *time.Duration
}

// KeyIterator is an iterator over the keys held by a cache, as returned by the
// optional Keys method. Iteration stops after the first non-nil error.
type KeyIterator = cache.KeyIterator

// batchedCache represents a cache where the underlying implementation is able
// to benefit from batched set requests. This interface is optional for caches
// and when implemented will automatically be utilised where possible.
type batchedCache interface {
	// SetMulti attempts to set multiple cache items in as few requests as
	// possible.
	SetMulti(ctx context.Context, keyValues ...CacheItem) error
}

// existsCache represents a cache where the underlying implementation is able
// to benefit from exists requests. This interface is optional for caches
// and when implemented will automatically be utilised where possible.
type existsCache interface {
	// Check if a cache item exists.
	Exists(ctx context.Context, key string) (bool, error)
}

// listableCache represents a cache where the underlying implementation is able
// to enumerate the keys it holds. This interface is optional for caches and
// when implemented will automatically be utilised where possible, otherwise
// calls to Keys yield ErrKeyListingNotSupported.
type listableCache interface {
	// Keys returns an iterator over all keys currently held by the cache.
	// Iteration stops after the first non-nil error.
	Keys(ctx context.Context) KeyIterator
}

// PrefetchKeys builds a Keys iterator that runs the provided produce
// function in a background goroutine, allowing it to fetch subsequent pages of
// keys while the caller consumes the keys already found. Up to readAhead keys
// are buffered; sizing this at or above a backend's page size lets the next
// page be fetched while the current one is being yielded.
//
// The produce function should call emit once for each key. emit returns false
// once the caller has stopped consuming (an early break, or a downstream
// error), after which produce should stop and return - typically nil.
// Returning a non-nil error from produce terminates iteration after yielding
// that error to the caller. If ctx is cancelled iteration stops without
// yielding a further error; produce should return promptly when ctx is done.
//
// This is a convenience for implementing the optional key-listing behaviour of
// a paginated cache; it manages the goroutine lifecycle and cancellation on
// early termination so implementations only need to express their paging loop.
func PrefetchKeys(ctx context.Context, readAhead int, produce func(ctx context.Context, emit func(key string) bool) error) KeyIterator {
	return func(yield func(string, error) bool) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		type result struct {
			key string
			err error
		}
		results := make(chan result, readAhead)

		go func() {
			defer close(results)
			err := produce(ctx, func(key string) bool {
				select {
				case results <- result{key: key}:
					return true
				case <-ctx.Done():
					return false
				}
			})
			// Only surface an error while the caller is still listening and
			// neither side has cancelled - a cancellation is the caller's
			// signal to stop, not an error to report.
			if err != nil && ctx.Err() == nil {
				select {
				case results <- result{err: err}:
				case <-ctx.Done():
				}
			}
		}()

		for r := range results {
			if r.err != nil {
				yield("", r.err)
				return
			}
			if !yield(r.key, nil) {
				// Cancelling (via the deferred cancel) unblocks the producer's
				// pending emit so it observes the stop and exits.
				return
			}
		}
	}
}

//------------------------------------------------------------------------------

// Implements cache.V1.
type airGapCache struct {
	c  Cache
	cm batchedCache
	ce existsCache
}

func newAirGapCache(c Cache, stats metrics.Type) cache.V1 {
	ag := &airGapCache{c: c}
	ag.cm, _ = c.(batchedCache)
	ag.ce, _ = c.(existsCache)

	if cl, ok := c.(listableCache); ok {
		lag := &listableAirGapCache{
			airGapCache: ag,
			cl:          cl,
		}
		return cache.MetricsForListableCache(lag, stats)
	}

	return cache.MetricsForCache(ag, stats)
}

func (a *airGapCache) Get(ctx context.Context, key string) ([]byte, error) {
	b, err := a.c.Get(ctx, key)
	if errors.Is(err, ErrKeyNotFound) {
		err = component.ErrKeyNotFound
	}
	return b, err
}

func (a *airGapCache) Exists(ctx context.Context, key string) (bool, error) {
	if a.ce != nil {
		return a.ce.Exists(ctx, key)
	}
	_, err := a.Get(ctx, key)
	if err != nil {
		if errors.Is(err, component.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
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

//------------------------------------------------------------------------------

// Implements cache.Listable.
type listableAirGapCache struct {
	*airGapCache
	cl listableCache
}

func (a *listableAirGapCache) Keys(ctx context.Context) KeyIterator {
	return func(yield func(string, error) bool) {
		for key, err := range a.cl.Keys(ctx) {
			if errors.Is(err, ErrKeyListingNotSupported) {
				err = component.ErrKeyListingNotSupported
			}
			if !yield(key, err) {
				return
			}
		}
	}
}

//------------------------------------------------------------------------------

// Implements Cache around a types.Cache.
type reverseAirGapCache struct {
	c cache.V1
}

func newReverseAirGapCache(c cache.V1) Cache {
	rag := reverseAirGapCache{c: c}

	if cl, ok := c.(cache.Listable); ok {
		return &listableReverseAirGapCache{
			reverseAirGapCache: &rag,
			cl:                 cl,
		}
	}

	return &rag
}

func (r *reverseAirGapCache) Get(ctx context.Context, key string) ([]byte, error) {
	b, err := r.c.Get(ctx, key)
	if errors.Is(err, component.ErrKeyNotFound) {
		err = ErrKeyNotFound
	}
	return b, err
}

func (r *reverseAirGapCache) Exists(ctx context.Context, key string) (bool, error) {
	return r.c.Exists(ctx, key)
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

//------------------------------------------------------------------------------

// Implements listableCache around a cache.Listable.
type listableReverseAirGapCache struct {
	*reverseAirGapCache
	cl cache.Listable
}

func (r *listableReverseAirGapCache) Keys(ctx context.Context) KeyIterator {
	return func(yield func(string, error) bool) {
		for key, err := range r.cl.Keys(ctx) {
			if errors.Is(err, component.ErrKeyListingNotSupported) {
				err = ErrKeyListingNotSupported
			}
			if !yield(key, err) {
				return
			}
		}
	}
}
