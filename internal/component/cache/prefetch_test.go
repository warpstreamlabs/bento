package cache

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrefetchKeys(t *testing.T) {
	src := []string{"a", "b", "c", "d", "e"}
	seq := PrefetchKeys(t.Context(), 2, func(ctx context.Context, emit func(string) bool) error {
		for _, k := range src {
			if !emit(k) {
				return nil
			}
		}
		return nil
	})

	var keys []string
	for k, err := range seq {
		require.NoError(t, err)
		keys = append(keys, k)
	}
	assert.Equal(t, src, keys)
}

func TestPrefetchKeysError(t *testing.T) {
	errBoom := errors.New("boom")
	seq := PrefetchKeys(t.Context(), 2, func(ctx context.Context, emit func(string) bool) error {
		if !emit("a") {
			return nil
		}
		return errBoom
	})

	var keys []string
	var gotErr error
	for k, err := range seq {
		if err != nil {
			gotErr = err
			break
		}
		keys = append(keys, k)
	}
	assert.Equal(t, []string{"a"}, keys)
	assert.ErrorIs(t, gotErr, errBoom)
}

func TestPrefetchKeysEarlyBreak(t *testing.T) {
	stopped := make(chan struct{})
	seq := PrefetchKeys(t.Context(), 1, func(ctx context.Context, emit func(string) bool) error {
		defer close(stopped)
		for i := 0; ; i++ {
			if !emit(strconv.Itoa(i)) {
				return nil
			}
		}
	})

	var keys []string
	for k := range seq {
		keys = append(keys, k)
		if len(keys) == 3 {
			break
		}
	}
	assert.Equal(t, []string{"0", "1", "2"}, keys)

	// The producer must observe the early break and exit rather than leak.
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("producer did not stop after early break")
	}
}

func TestPrefetchKeysContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stopped := make(chan struct{})
	seq := PrefetchKeys(ctx, 1, func(ctx context.Context, emit func(string) bool) error {
		defer close(stopped)
		for i := 0; ; i++ {
			if !emit(strconv.Itoa(i)) {
				return ctx.Err()
			}
		}
	})

	var keys []string
	for k, err := range seq {
		require.NoError(t, err) // cancellation stops iteration without an error
		keys = append(keys, k)
		if len(keys) == 2 {
			cancel()
		}
	}

	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("producer did not stop after context cancellation")
	}
}
