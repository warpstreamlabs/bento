package aws

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGlobalPendingPool_AcquireRelease(t *testing.T) {
	pool := newGlobalPendingPool(100)

	// Acquire some space
	assert.True(t, pool.Acquire(context.Background(), 50))
	assert.Equal(t, 50, pool.Current())

	// Acquire more space
	assert.True(t, pool.Acquire(context.Background(), 30))
	assert.Equal(t, 80, pool.Current())

	// Release some space
	pool.Release(20)
	assert.Equal(t, 60, pool.Current())

	// Release all
	pool.Release(60)
	assert.Equal(t, 0, pool.Current())
}

func TestGlobalPendingPool_AcquireExceedsMax(t *testing.T) {
	pool := newGlobalPendingPool(100)

	// Trying to acquire more than max should fail immediately
	assert.False(t, pool.Acquire(context.Background(), 150))
	assert.Equal(t, 0, pool.Current())
}

func TestGlobalPendingPool_AcquireBlocksUntilSpace(t *testing.T) {
	pool := newGlobalPendingPool(100)

	// Fill the pool
	assert.True(t, pool.Acquire(context.Background(), 100))

	// Start a goroutine that will try to acquire more
	acquired := make(chan bool, 1)
	go func() {
		acquired <- pool.Acquire(context.Background(), 50)
	}()

	// Give the goroutine time to start waiting
	time.Sleep(50 * time.Millisecond)

	// Release some space
	pool.Release(50)

	// The acquire should succeed now
	select {
	case result := <-acquired:
		assert.True(t, result)
	case <-time.After(time.Second):
		t.Fatal("Acquire should have succeeded after Release")
	}

	assert.Equal(t, 100, pool.Current())
}

func TestGlobalPendingPool_AcquireContextCancellation(t *testing.T) {
	pool := newGlobalPendingPool(100)

	// Fill the pool
	assert.True(t, pool.Acquire(context.Background(), 100))

	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())

	// Start a goroutine that will try to acquire more
	acquired := make(chan bool, 1)
	go func() {
		acquired <- pool.Acquire(ctx, 50)
	}()

	// Give the goroutine time to start waiting
	time.Sleep(50 * time.Millisecond)

	// Cancel the context
	cancel()

	// The acquire should fail due to cancellation
	select {
	case result := <-acquired:
		assert.False(t, result)
	case <-time.After(time.Second):
		t.Fatal("Acquire should have returned after context cancellation")
	}

	// Pool should still have the original 100
	assert.Equal(t, 100, pool.Current())
}

func TestGlobalPendingPool_WaitForSpace(t *testing.T) {
	pool := newGlobalPendingPool(100)

	// With space available, should return immediately
	result := pool.WaitForSpace(context.Background(), 0)
	assert.Equal(t, WaitForSpaceOK, result)

	// Fill the pool
	assert.True(t, pool.Acquire(context.Background(), 100))

	// Start a goroutine that will wait for space
	waitResult := make(chan WaitForSpaceResult, 1)
	go func() {
		waitResult <- pool.WaitForSpace(context.Background(), 0)
	}()

	// Give the goroutine time to start waiting
	time.Sleep(50 * time.Millisecond)

	// Release some space
	pool.Release(10)

	// The wait should succeed now
	select {
	case result := <-waitResult:
		assert.Equal(t, WaitForSpaceOK, result)
	case <-time.After(time.Second):
		t.Fatal("WaitForSpace should have returned after Release")
	}
}

func TestGlobalPendingPool_WaitForSpaceTimeout(t *testing.T) {
	pool := newGlobalPendingPool(100)

	// Fill the pool
	assert.True(t, pool.Acquire(context.Background(), 100))

	// Wait with a short timeout
	start := time.Now()
	result := pool.WaitForSpace(context.Background(), 100*time.Millisecond)
	elapsed := time.Since(start)

	assert.Equal(t, WaitForSpaceTimeout, result)
	assert.GreaterOrEqual(t, elapsed, 100*time.Millisecond)
	assert.Less(t, elapsed, 200*time.Millisecond) // Should not take too long
}

func TestGlobalPendingPool_WaitForSpaceContextCancellation(t *testing.T) {
	pool := newGlobalPendingPool(100)

	// Fill the pool
	assert.True(t, pool.Acquire(context.Background(), 100))

	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())

	// Start a goroutine that will wait for space
	waitResult := make(chan WaitForSpaceResult, 1)
	go func() {
		waitResult <- pool.WaitForSpace(ctx, 0)
	}()

	// Give the goroutine time to start waiting
	time.Sleep(50 * time.Millisecond)

	// Cancel the context
	cancel()

	// The wait should return cancelled
	select {
	case result := <-waitResult:
		assert.Equal(t, WaitForSpaceCancelled, result)
	case <-time.After(time.Second):
		t.Fatal("WaitForSpace should have returned after context cancellation")
	}
}

func TestGlobalPendingPool_ReleaseBelowZero(t *testing.T) {
	pool := newGlobalPendingPool(100)

	// Release more than current (should clamp to 0)
	pool.Release(50)
	assert.Equal(t, 0, pool.Current())

	// Acquire and release more than acquired
	assert.True(t, pool.Acquire(context.Background(), 30))
	pool.Release(50)
	assert.Equal(t, 0, pool.Current())
}

func TestGlobalPendingPool_ConcurrentAccess(t *testing.T) {
	pool := newGlobalPendingPool(1000)

	var wg sync.WaitGroup
	acquireCount := 100
	acquireSize := 10

	// Start many goroutines acquiring and releasing
	for i := 0; i < acquireCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.True(t, pool.Acquire(context.Background(), acquireSize))
			time.Sleep(10 * time.Millisecond)
			pool.Release(acquireSize)
		}()
	}

	wg.Wait()
	assert.Equal(t, 0, pool.Current())
}

func TestGlobalPendingPool_WaitForSpaceVsAcquire(t *testing.T) {
	// Test that WaitForSpace returns as soon as current < max,
	// but Acquire might still need to wait if current + count > max
	pool := newGlobalPendingPool(100)

	// Fill to 90
	assert.True(t, pool.Acquire(context.Background(), 90))

	// WaitForSpace should return OK (there's space)
	result := pool.WaitForSpace(context.Background(), 100*time.Millisecond)
	assert.Equal(t, WaitForSpaceOK, result)

	// But Acquire of 20 should block until more space is released
	acquired := make(chan bool, 1)
	go func() {
		acquired <- pool.Acquire(context.Background(), 20)
	}()

	// Give the goroutine time to start waiting
	time.Sleep(50 * time.Millisecond)

	// Release 10 more to make room
	pool.Release(10)

	// Now Acquire should succeed
	select {
	case result := <-acquired:
		assert.True(t, result)
	case <-time.After(time.Second):
		t.Fatal("Acquire should have succeeded after Release")
	}

	assert.Equal(t, 100, pool.Current())
}
