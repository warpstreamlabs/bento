package periodic

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCancellation(t *testing.T) {
	counter := atomic.Int32{}
	p := New(time.Hour, func() {
		counter.Add(1)
	})
	p.Start()
	require.Equal(t, int32(0), counter.Load())
	p.Stop()
	require.Equal(t, int32(0), counter.Load())
}

func TestWorks(t *testing.T) {
	counter := atomic.Int32{}
	p := New(time.Millisecond, func() {
		counter.Add(1)
	})
	p.Start()
	require.Eventually(t, func() bool { return counter.Load() > 5 }, time.Second, time.Millisecond)
	p.Stop()
	snapshot := counter.Load()
	time.Sleep(time.Millisecond * 250)
	require.Equal(t, snapshot, counter.Load())
}

func TestWorksWithContext(t *testing.T) {
	active := atomic.Bool{}
	p := NewWithContext(time.Millisecond, func(ctx context.Context) {
		active.Store(true)
		// Block until context is cancelled
		<-ctx.Done()
		active.Store(false)
	})
	p.Start()
	require.Eventually(t, func() bool { return active.Load() }, 10*time.Millisecond, time.Millisecond)
	p.Stop()
	require.False(t, active.Load())
}
