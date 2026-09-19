package parser

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/bloblang/query"
)

// withBuildCounter temporarily installs a hook that increments a counter
// every time queryParser actually builds a new parser tree (cache miss). It
// returns a pointer to the counter and automatically restores the previous
// hook when the test finishes. This is necessary because comparing Func[T]
// values via reflect.ValueOf(fn).Pointer() is unreliable: Go may report the
// same code pointer for structurally identical closures even when they
// capture different environments, so pointer comparison cannot be used to
// detect cache hits/misses.
func withBuildCounter(t *testing.T) *int {
	t.Helper()
	count := 0
	prev := queryParserBuiltHook
	queryParserBuiltHook = func() { count++ }
	t.Cleanup(func() { queryParserBuiltHook = prev })
	return &count
}

// TestQueryParserCacheReusesSameKey verifies that calling queryParser
// multiple times with an equivalent Context (same Functions/Methods/
// namedContext identity) only builds the parser tree once; subsequent calls
// must be served from cache.
func TestQueryParserCacheReusesSameKey(t *testing.T) {
	count := withBuildCounter(t)
	pCtx := GlobalContext()

	_ = queryParser(pCtx)
	_ = queryParser(pCtx)
	_ = queryParser(pCtx)

	assert.Equal(t, 1, *count,
		"expected queryParser to build only once for an equivalent context")
}

// TestQueryParserCacheIsolatesDifferentKeys verifies that contexts with
// different identity (different Functions/Methods sets, or different
// namedContext chains) each trigger their own build instead of sharing a
// cache entry, and that previously built contexts are still served from
// cache afterwards.
func TestQueryParserCacheIsolatesDifferentKeys(t *testing.T) {
	count := withBuildCounter(t)

	globalCtx := GlobalContext()
	emptyCtx := EmptyContext()

	_ = queryParser(globalCtx)
	assert.Equal(t, 1, *count, "expected first context to trigger a build")

	_ = queryParser(emptyCtx)
	assert.Equal(t, 2, *count, "expected a different Functions/Methods identity to trigger its own build")

	namedCtx := globalCtx.WithNamedContext("foo")
	_ = queryParser(namedCtx)
	assert.Equal(t, 3, *count, "expected a namedContext change to trigger its own build")

	// Re-requesting any of the three previously built contexts must not
	// trigger additional builds.
	_ = queryParser(globalCtx)
	_ = queryParser(emptyCtx)
	_ = queryParser(namedCtx)
	assert.Equal(t, 3, *count, "expected previously built contexts to be served from cache")
}

// TestQueryParserCacheNamedContextStillParsesCorrectly ensures that caching
// doesn't break the semantics of named contexts: a query referencing a named
// context must resolve against the correct field function, both before and
// after the fix to lambdaExpressionParser's variable shadowing.
func TestQueryParserCacheNamedContextStillParsesCorrectly(t *testing.T) {
	baseCtx := GlobalContext()

	// Prime the cache with the base (non-named) context first.
	primeRes := queryParser(baseCtx)([]rune("this"))
	require.Nil(t, primeRes.Err)

	namedCtx := baseCtx.WithNamedContext("foo")

	res := queryParser(namedCtx)([]rune("foo"))
	require.Nil(t, res.Err)

	fn, ok := res.Payload.(query.Function)
	require.True(t, ok)
	assert.Contains(t, fn.Annotation(), "foo")
}

// TestQueryParserCacheDeactivatedContextIsolated ensures a Deactivated
// context (which swaps Functions/Methods for deactivated variants) does not
// share cached parsers with its originating context, and still parses
// (though it cannot execute) correctly.
func TestQueryParserCacheDeactivatedContextIsolated(t *testing.T) {
	count := withBuildCounter(t)

	activeCtx := GlobalContext()
	deactivatedCtx := activeCtx.Deactivated()

	_ = queryParser(activeCtx)
	assert.Equal(t, 1, *count)

	_ = queryParser(deactivatedCtx)
	assert.Equal(t, 2, *count, "expected Deactivated() context to trigger its own build, not reuse the active context's cache")

	res := queryParser(deactivatedCtx)([]rune(`this.foo.uppercase()`))
	require.Nil(t, res.Err)
	assert.Equal(t, 2, *count, "expected re-parsing with the deactivated context to be served from cache")
}

// TestContextCacheKeyIsComparableAndStable verifies the cacheKey type itself:
// it must be a valid, comparable map key (usable as map[cacheKeyT]any),
// equal for contexts with identical identity, and different when
// Functions/Methods identity or the namedContext chain differs. This guards
// the internal representation independently of queryParser's build-counter
// behaviour, so a future change to cacheKey's type can't silently break
// equality semantics without a test failing here first.
func TestContextCacheKeyIsComparableAndStable(t *testing.T) {
	globalCtx := GlobalContext()
	globalCtx2 := GlobalContext()
	emptyCtx := EmptyContext()

	// Same identity (same Functions/Methods pointers) must produce equal
	// keys even across independently constructed Context values that both
	// point at the shared global function/method sets.
	assert.Equal(t, globalCtx.cacheKey(), globalCtx2.cacheKey(),
		"two GlobalContext() calls share the same underlying Functions/Methods pointers and must produce equal cache keys")

	// Different Functions/Methods identity must produce different keys.
	assert.NotEqual(t, globalCtx.cacheKey(), emptyCtx.cacheKey())

	// A namedContext chain must affect the key, and nesting order matters.
	fooCtx := globalCtx.WithNamedContext("foo")
	barCtx := globalCtx.WithNamedContext("bar")
	fooBarCtx := fooCtx.WithNamedContext("bar")
	barFooCtx := barCtx.WithNamedContext("foo")

	assert.NotEqual(t, globalCtx.cacheKey(), fooCtx.cacheKey())
	assert.NotEqual(t, fooCtx.cacheKey(), barCtx.cacheKey())
	assert.NotEqual(t, fooBarCtx.cacheKey(), barFooCtx.cacheKey(),
		"nesting order of named contexts must produce distinct keys since it affects shadow-detection semantics")

	// The key type must be safe to use as a map key (this would fail to
	// compile if cacheKey() returned a non-comparable type).
	m := map[any]bool{}
	m[globalCtx.cacheKey()] = true
	assert.True(t, m[globalCtx2.cacheKey()],
		"cache key must be usable as a map key and be equal for equivalent contexts")
}

// TestQueryParserCacheConcurrentAccessIsSafe ensures the parser cache can be
// read/written concurrently without triggering the race detector, and that
// concurrently parsing distinct lambda expressions (which exercise the
// previously racy code path) against a shared cached parser is now safe
// (run with -race).
func TestQueryParserCacheConcurrentAccessIsSafe(t *testing.T) {
	pCtx := GlobalContext()

	names := []string{"v", "item", "ele", "num", "patron", "foo", "bar", "baz"}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		for _, n := range names {
			n := n
			wg.Add(1)
			go func() {
				defer wg.Done()
				p := queryParser(pCtx)
				res := p([]rune(n + " -> " + n + `.uppercase()`))
				assert.Nil(t, res.Err)
			}()
		}
	}
	wg.Wait()
}

// TestQueryParserCacheProducesWorkingParser is a behavioural sanity check
// ensuring the cached parser still parses a representative query correctly,
// guarding against the cache silently returning a stale/incorrect parser.
func TestQueryParserCacheProducesWorkingParser(t *testing.T) {
	pCtx := GlobalContext()

	for i := 0; i < 3; i++ {
		res := queryParser(pCtx)([]rune(`this.foo.uppercase()`))
		require.Nil(t, res.Err)
		_, ok := res.Payload.(query.Function)
		require.True(t, ok)
	}
}
