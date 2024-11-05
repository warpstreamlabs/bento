package periodic

import (
	"context"
	"time"
)

// Periodic holds a background goroutine that can do periodic work.
//
// The work here cannot communicate errors directly, so it must
// communicate with channels or swallow errors.
//
// NOTE: It's expected that Start and Stop are called on the same
// goroutine or be externally synchronized as to not race.
type Periodic struct {
	duration time.Duration
	work     func(context.Context)

	cancel context.CancelFunc
	done   chan any
}

// New creates new background work that runs every `duration` and performs `work`.
func New(duration time.Duration, work func()) *Periodic {
	return &Periodic{
		duration: duration,
		work:     func(context.Context) { work() },
	}
}

// NewWithContext creates new background work that runs every `duration` and performs `work`.
//
// Work is passed a context that is cancelled when the overall periodic is cancelled.
func NewWithContext(duration time.Duration, work func(context.Context)) *Periodic {
	return &Periodic{
		duration: duration,
		work:     work,
	}
}

// Start starts the `Periodic` work.
//
// It does not do work immedately, only after the time has passed.
func (p *Periodic) Start() {
	if p.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan any)
	go runBackgroundLoop(ctx, p.duration, done, p.work)
	p.cancel = cancel
	p.done = done
}

func runBackgroundLoop(ctx context.Context, d time.Duration, done chan any, work func(context.Context)) {
	refreshTimer := time.NewTicker(d)
	defer func() {
		refreshTimer.Stop()
		close(done)
	}()
	for ctx.Err() == nil {
		select {
		case <-refreshTimer.C:
			work(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// Stop stops the periodic work and waits for the background goroutine to exit.
func (p *Periodic) Stop() {
	if p.cancel == nil {
		return
	}
	p.cancel()
	<-p.done
	p.done = nil
	p.cancel = nil
}
