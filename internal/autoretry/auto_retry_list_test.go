package autoretry

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errCustomEOF = errors.New("custom EOF")

func TestRetryListAllAcks(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	var acked []string

	data := []string{"foo", "bar", "baz"}
	l := NewList(func(ctx context.Context) (t string, aFn AckFunc, err error) {
		if len(data) == 0 {
			err = errCustomEOF
			return
		}
		next := data[0]
		data = data[1:]
		return next, func(ctx context.Context, err error) error {
			acked = append(acked, next)
			return nil
		}, nil
	}, nil)

	res, fooFn, err := l.Shift(tCtx, true)
	require.NoError(t, err)
	assert.Equal(t, "foo", res)

	res, barFn, err := l.Shift(tCtx, true)
	require.NoError(t, err)
	assert.Equal(t, "bar", res)

	res, bazFn, err := l.Shift(tCtx, true)
	require.NoError(t, err)
	assert.Equal(t, "baz", res)

	_, _, err = l.Shift(tCtx, true)
	require.Equal(t, errCustomEOF, err)

	assert.NoError(t, bazFn(tCtx, nil))
	assert.NoError(t, barFn(tCtx, nil))
	assert.NoError(t, fooFn(tCtx, nil))

	assert.Equal(t, []string{
		"baz", "bar", "foo",
	}, acked)

	fmt.Println("last shift")
	_, _, err = l.Shift(tCtx, false)
	assert.Equal(t, ErrExhausted, err)

	require.NoError(t, l.Close(tCtx))
}

func TestRetryListNacks(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	var acked []string

	data := []string{"foo", "bar", "baz"}
	l := NewList(func(ctx context.Context) (t string, aFn AckFunc, err error) {
		if len(data) == 0 {
			err = errCustomEOF
			return
		}
		next := data[0]
		data = data[1:]
		return next, func(ctx context.Context, err error) error {
			acked = append(acked, next)
			return nil
		}, nil
	}, nil)

	v, fooFn, err := l.Shift(tCtx, true)
	require.NoError(t, err)
	assert.Equal(t, "foo", v)

	v, barFn, err := l.Shift(tCtx, true)
	require.NoError(t, err)
	assert.Equal(t, "bar", v)

	v, bazFn, err := l.Shift(tCtx, true)
	require.NoError(t, err)
	assert.Equal(t, "baz", v)

	_, _, err = l.Shift(tCtx, true)
	require.Equal(t, errCustomEOF, err)

	assert.NoError(t, bazFn(tCtx, errors.New("baz nope")))
	assert.NoError(t, barFn(tCtx, errors.New("bar nope")))
	assert.NoError(t, fooFn(tCtx, errors.New("foo nope")))

	assert.Equal(t, []string(nil), acked)

	v, bazFn, err = l.Shift(tCtx, false)
	require.NoError(t, err)
	assert.Equal(t, "baz", v)

	v, barFn, err = l.Shift(tCtx, false)
	require.NoError(t, err)
	assert.Equal(t, "bar", v)
	assert.NoError(t, barFn(tCtx, errors.New("bar nope again")))

	v, fooFn, err = l.Shift(tCtx, false)
	require.NoError(t, err)
	assert.Equal(t, "foo", v)

	assert.NoError(t, fooFn(tCtx, nil))
	assert.NoError(t, bazFn(tCtx, nil))

	assert.Equal(t, []string{
		"foo", "baz",
	}, acked)

	v, barFn, err = l.Shift(tCtx, false)
	require.NoError(t, err)
	assert.Equal(t, "bar", v)

	cancelledCtx, done := context.WithTimeout(tCtx, time.Millisecond*50)
	defer done()

	_, _, err = l.Shift(cancelledCtx, false)
	assert.Equal(t, cancelledCtx.Err(), err)

	assert.NoError(t, barFn(tCtx, nil))

	assert.Equal(t, []string{
		"foo", "baz", "bar",
	}, acked)

	_, _, err = l.Shift(tCtx, false)
	assert.Equal(t, ErrExhausted, err)

	require.NoError(t, l.Close(tCtx))
}

func TestRetryListNackMutator(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	var acked []string

	data := []string{"foo"}
	l := NewList(func(ctx context.Context) (t string, aFn AckFunc, err error) {
		if len(data) == 0 {
			err = errCustomEOF
			return
		}
		next := data[0]
		data = data[1:]
		return next, func(ctx context.Context, err error) error {
			acked = append(acked, next)
			return nil
		}, nil
	}, func(t string, err error) string {
		return t + " and " + err.Error()
	})

	v, fooFn, err := l.Shift(tCtx, true)
	require.NoError(t, err)
	assert.Equal(t, "foo", v)

	_, _, err = l.Shift(tCtx, true)
	require.Equal(t, errCustomEOF, err)

	assert.NoError(t, fooFn(tCtx, errors.New("first error")))
	assert.Equal(t, []string(nil), acked)

	v, fooFn, err = l.Shift(tCtx, false)
	require.NoError(t, err)
	assert.Equal(t, "foo and first error", v)

	assert.NoError(t, fooFn(tCtx, errors.New("second error")))
	assert.Equal(t, []string(nil), acked)

	v, fooFn, err = l.Shift(tCtx, false)
	require.NoError(t, err)
	assert.Equal(t, "foo and first error and second error", v)

	assert.NoError(t, fooFn(tCtx, errors.New("third error")))
	assert.Equal(t, []string(nil), acked)

	v, fooFn, err = l.Shift(tCtx, false)
	require.NoError(t, err)
	assert.Equal(t, "foo and first error and second error and third error", v)

	assert.NoError(t, fooFn(tCtx, nil))

	assert.Equal(t, []string{
		"foo",
	}, acked)

	_, _, err = l.Shift(tCtx, false)
	assert.Equal(t, ErrExhausted, err)

	require.NoError(t, l.Close(tCtx))
}

func TestRetryListShiftCancelledDuringBackoff(t *testing.T) {
	// The missed wake-up depends on goroutine scheduling, so repeat the scenario.
	for i := 0; i < 100 && !t.Failed(); i++ {
		synctest.Test(t, testShiftCancelledDuringBackoff)
	}
}

func testShiftCancelledDuringBackoff(t *testing.T) {
	var read bool
	l := NewList(func(ctx context.Context) (string, AckFunc, error) {
		if !read {
			read = true
			return "foo", func(context.Context, error) error { return nil }, nil
		}
		<-ctx.Done()
		return "", nil, ctx.Err()
	}, nil)
	defer func() {
		require.NoError(t, l.Close(context.Background()))
	}()

	// The first two retries skip the backoff, so the next shift sleeps in it.
	_, fooFn, err := l.Shift(t.Context(), true)
	require.NoError(t, err)
	for range 2 {
		require.NoError(t, fooFn(t.Context(), errors.New("nope")))
		_, fooFn, err = l.Shift(t.Context(), true)
		require.NoError(t, err)
	}
	require.NoError(t, fooFn(t.Context(), errors.New("nope")))

	// synctest does not count mutex waits as durable, so let the earlier
	// shifts' cancel goroutines exit before the next shift holds the lock.
	synctest.Wait()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	shifted := make(chan error, 1)
	go func() {
		_, _, err := l.Shift(ctx, true)
		shifted <- err
	}()
	synctest.Wait()

	cancel()
	synctest.Wait()
	select {
	case err := <-shifted:
		assert.ErrorIs(t, err, context.Canceled)
	default:
		t.Error("Shift is still blocked after its context was cancelled during backoff")
	}
}
