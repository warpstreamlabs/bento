package aws

import (
	"context"
	"sync"
	"time"
)

// globalPendingPool limits the total number of pending records across all shards.
// Each shard must acquire space from this pool before accepting records from Kinesis,
// ensuring bounded memory usage regardless of shard count.
type globalPendingPool struct {
	mu      sync.Mutex
	cond    *sync.Cond
	current int
	max     int
}

// newGlobalPendingPool creates a new pool with the specified maximum capacity.
func newGlobalPendingPool(max int) *globalPendingPool {
	p := &globalPendingPool{
		max: max,
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// Acquire acquires space for count records, blocking if necessary until space is available.
// Returns false immediately if count > max (impossible to satisfy) or if ctx is cancelled.
func (p *globalPendingPool) Acquire(ctx context.Context, count int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	// If the requested count exceeds the pool's maximum capacity, it can never be satisfied.
	// Return false immediately to avoid blocking indefinitely.
	if count > p.max {
		return false
	}

	// Start a goroutine to handle context cancellation by broadcasting to wake up waiters
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			p.cond.Broadcast()
		case <-done:
		}
	}()

	for p.current+count > p.max {
		// Check if context is cancelled before waiting
		if ctx.Err() != nil {
			return false
		}
		p.cond.Wait()
	}

	// Final check after waking up
	if ctx.Err() != nil {
		return false
	}

	p.current += count
	return true
}

// WaitForSpaceResult indicates the outcome of WaitForSpace.
type WaitForSpaceResult int

const (
	// WaitForSpaceOK indicates space is available.
	WaitForSpaceOK WaitForSpaceResult = iota
	// WaitForSpaceCancelled indicates the context was cancelled.
	WaitForSpaceCancelled
	// WaitForSpaceTimeout indicates the timeout was reached while waiting.
	WaitForSpaceTimeout
)

// WaitForSpace blocks until there is any space available in the pool.
// This is used to apply backpressure before fetching new data from Kinesis.
// Returns WaitForSpaceOK if space is available, WaitForSpaceCancelled if
// context is cancelled, or WaitForSpaceTimeout if the timeout is reached.
// A timeout of 0 means no timeout (wait indefinitely).
func (p *globalPendingPool) WaitForSpace(ctx context.Context, timeout time.Duration) WaitForSpaceResult {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Set up deadline if timeout is specified
	var deadline time.Time
	var timer *time.Timer
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
		timer = time.NewTimer(timeout)
		defer timer.Stop()
	}

	// Start a goroutine to handle context cancellation and timeout by broadcasting
	done := make(chan struct{})
	defer close(done)
	go func() {
		if timer != nil {
			select {
			case <-ctx.Done():
				p.cond.Broadcast()
			case <-timer.C:
				p.cond.Broadcast()
			case <-done:
			}
		} else {
			select {
			case <-ctx.Done():
				p.cond.Broadcast()
			case <-done:
			}
		}
	}()

	for p.current >= p.max {
		// Check if context is cancelled before waiting
		if ctx.Err() != nil {
			return WaitForSpaceCancelled
		}

		// Check if we've exceeded the timeout
		if timeout > 0 && time.Now().After(deadline) {
			return WaitForSpaceTimeout
		}

		p.cond.Wait()
	}

	// Final checks after waking up
	if ctx.Err() != nil {
		return WaitForSpaceCancelled
	}
	if timeout > 0 && time.Now().After(deadline) {
		return WaitForSpaceTimeout
	}

	return WaitForSpaceOK
}

// Release returns count records worth of space back to the pool.
func (p *globalPendingPool) Release(count int) {
	p.mu.Lock()
	p.current -= count
	if p.current < 0 {
		p.current = 0
	}
	p.cond.Broadcast()
	p.mu.Unlock()
}

// Current returns the current number of records in the pool (for monitoring/debugging).
func (p *globalPendingPool) Current() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.current
}
