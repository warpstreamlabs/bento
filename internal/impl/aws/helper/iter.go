package helper

import (
	"iter"
)

// TokenIterator creates an iterator from a token-based pagination function `fn`.
// Iteration stops when nextToken is nil or an error occurs.
func TokenIterator[V any](
	fn func(token *string) (value V, nextToken *string, err error),
) iter.Seq2[V, error] {
	return func(yield func(V, error) bool) {
		var prev *string
		for {
			val, next, err := fn(prev)
			if err != nil {
				var zero V
				yield(zero, err)
				return
			}

			if !yield(val, nil) {
				return
			}

			if next == nil {
				return
			}
			prev = next
		}
	}
}

// Collect takes an iterator of slices and returns a concatentation of all results.
func Collect[V any](seq iter.Seq2[[]V, error]) ([]V, error) {
	var result []V
	for vals, err := range seq {
		if err != nil {
			return nil, err
		}
		result = append(result, vals...)
	}
	return result, nil
}
