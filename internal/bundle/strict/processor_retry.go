package strict

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/warpstreamlabs/bento/internal/bundle"
	iprocessor "github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/log"
	"github.com/warpstreamlabs/bento/internal/message"
)

func wrapWithRetry(p iprocessor.V1, mgr bundle.NewManagement, maxRetries int) iprocessor.V1 {
	backoffCtor := func() backoff.BackOff {
		boff := backoff.NewExponentialBackOff()
		boff.InitialInterval = time.Millisecond * 100
		boff.MaxInterval = time.Second * 60
		boff.MaxElapsedTime = 0
		return boff
	}

	t := &retryProcessor{
		wrapped:     p,
		log:         mgr.Logger(),
		enabled:     true,
		maxRetries:  maxRetries,
		backoffCtor: backoffCtor,

		currentRetries:  0,
		backoffDuration: 0,
	}
	return t
}

//------------------------------------------------------------------------------

// retryProcessor retries a batch processing step if any message contains an error.
type retryProcessor struct {
	enabled bool
	wrapped iprocessor.V1
	log     log.Modular

	backoffCtor func() backoff.BackOff
	backoff     backoff.BackOff

	maxRetries int

	currentRetries  int
	backoffDuration time.Duration

	m sync.RWMutex
}

func (r *retryProcessor) ProcessBatch(ctx context.Context, b message.Batch) ([]message.Batch, error) {
	if !r.enabled {
		return r.wrapped.ProcessBatch(ctx, b)
	}

	if r.backoff == nil {
		r.backoff = r.backoffCtor()
		r.reset()
	}

	// Clear all previous errors prior to checking
	_ = b.Iter(func(i int, p *message.Part) error {
		p.ErrorSet(nil)
		return nil
	})

	resBatches, err := r.wrapped.ProcessBatch(ctx, b)
	if err != nil {
		return nil, err
	}

	hasFailed := false

errorChecks:
	for _, b := range resBatches {
		for _, m := range b {
			if err = m.ErrorGet(); err != nil {
				hasFailed = true
				break errorChecks
			}
		}
	}

	if !hasFailed {
		r.reset()
		return resBatches, nil
	}

	r.m.Lock()
	defer r.m.Unlock()
	r.currentRetries++
	if r.maxRetries > 0 && r.currentRetries > r.maxRetries {
		r.reset()
		r.log.With("error", err).Debug("Error occurred and maximum number of retries was reached.")

		// drop messages with errors
		filteredBatches := make([]message.Batch, 0, len(resBatches))
		for _, batch := range resBatches {
			validMessages := make([]*message.Part, 0, len(batch))

			for _, msg := range batch {
				if msg.ErrorGet() == nil {
					validMessages = append(validMessages, msg)
				}
			}

			// only add batch if non-empty
			if len(validMessages) > 0 {
				filteredBatches = append(filteredBatches, validMessages)
			}
		}

		return filteredBatches, nil
	}

	nextSleep := r.backoff.NextBackOff()
	r.backoffDuration += nextSleep
	if nextSleep == backoff.Stop {
		r.reset()
		r.log.With("error", err).Debug("Error occurred and maximum wait period was reached.")
		return resBatches, nil
	}

	r.log.With(
		"error", err,
		"retry", r.currentRetries,
		"backoff", nextSleep,
		"total_backoff", r.backoffDuration,
	).Trace("Error occurred, sleeping for next backoff period.")

	select {
	case <-time.After(nextSleep):
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}

}

func (r *retryProcessor) Close(ctx context.Context) error {
	return r.wrapped.Close(ctx)
}

func (r *retryProcessor) UnwrapProc() iprocessor.V1 {
	return r.wrapped
}

func (r *retryProcessor) reset() {
	r.currentRetries = 0
	r.backoffDuration = 0
	if r.backoff != nil {
		r.backoff.Reset()
	}
}
