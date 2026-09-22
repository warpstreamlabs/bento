package cache

import "context"

// PrefetchKeys builds a Keys iterator that runs the provided produce function in
// a background goroutine, allowing it to fetch subsequent pages of keys while
// the caller consumes the keys already found. Up to readAhead keys are
// buffered; sizing this at or above a backend's page size lets the next page be
// fetched while the current one is being yielded.
//
// The produce function should call emit once for each key. emit returns false
// once the caller has stopped consuming (an early break, or a downstream
// error), after which produce should stop and return - typically nil.
// Returning a non-nil error from produce terminates iteration after yielding
// that error to the caller. If ctx is cancelled iteration stops without
// yielding a further error; produce should return promptly when ctx is done.
//
// This is a helper for implementing the optional key-listing behaviour of a
// paginated cache; it manages the goroutine lifecycle and cancellation on early
// termination so implementations only need to express their paging loop. It is
// intended for use by bento's own cache components.
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
