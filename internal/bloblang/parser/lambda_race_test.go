package parser

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestLambdaExpressionParserConcurrentSharedClosure reproduces a data race
// in lambdaExpressionParser independent of any caching mechanism: it builds
// the parser closure exactly once (as a future Context-level cache would)
// and then invokes that single closure instance from multiple goroutines
// concurrently, each parsing a different lambda expression with a distinct
// context label.
//
// lambdaExpressionParser's returned closure currently does:
//
//	pCtx = pCtx.WithNamedContext(name)
//
// which reassigns the pCtx variable captured from the enclosing
// lambdaExpressionParser(pCtx) call. When the same closure instance is
// invoked concurrently, this is a write to a shared variable racing against
// concurrent reads (HasNamedContext checks) and writes from other
// goroutines, corrupting the named-context chain used for shadow detection.
//
// Run with -race to observe the failure.
func TestLambdaExpressionParserConcurrentSharedClosure(t *testing.T) {
	pCtx := GlobalContext()

	// Build the closure ONCE, simulating what a Context-level cache for
	// queryParser would do: construct lambdaExpressionParser(pCtx) a single
	// time and have all callers share that one instance.
	sharedParser := lambdaExpressionParser(pCtx)

	names := []string{"v", "item", "ele", "num", "patron", "foo", "bar", "baz"}

	var wg sync.WaitGroup
	errs := make([]error, len(names))
	for i, n := range names {
		i, n := i, n
		wg.Add(1)
		go func() {
			defer wg.Done()
			expr := fmt.Sprintf(`%s -> %s.uppercase()`, n, n)
			res := sharedParser([]rune(expr))
			if res.Err != nil {
				errs[i] = fmt.Errorf("name=%s: %s", n, res.Err.Error())
			}
		}()
	}
	wg.Wait()

	for _, err := range errs {
		assert.NoError(t, err, "a lambda expression with its own distinct context label must never fail with a shadow error when parsed concurrently against a shared closure instance")
	}
}
