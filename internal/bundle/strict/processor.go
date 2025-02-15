package strict

import (
	"context"
	"sync/atomic"

	"github.com/warpstreamlabs/bento/internal/batch"
	iprocessor "github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/message"
)

func wrapWithStrict(p iprocessor.V1, opts ...func(*strictProcessor)) *strictProcessor {
	enabled := atomic.Bool{}
	enabled.Store(true)

	s := &strictProcessor{
		wrapped: p,
		enabled: &enabled,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// setAtomicStrictFlag sets the enabled flag of the strict processor to an atomic boolean
// that can be globally set for all configured strict processors.
func setAtomicStrictFlag(enabled *atomic.Bool) func(*strictProcessor) {
	return func(sp *strictProcessor) {
		sp.enabled = enabled
	}
}

//------------------------------------------------------------------------------

// strictProcessor fails batch processing if any message contains an error.
type strictProcessor struct {
	wrapped iprocessor.V1
	enabled *atomic.Bool
}

func (s *strictProcessor) ProcessBatch(ctx context.Context, b message.Batch) ([]message.Batch, error) {
	if !s.enabled.Load() {
		return s.wrapped.ProcessBatch(ctx, b)
	}

	batches, err := s.wrapped.ProcessBatch(ctx, b)
	if err != nil {
		return nil, err
	}

	// Iterate through all messages and populate a batch.Error type, calling Failed()
	// for each errored message. Otherwise, every message in the batch is treated as a failure.
	for _, msg := range batches {
		var batchErr *batch.Error
		_ = msg.Iter(func(i int, p *message.Part) error {
			mErr := p.ErrorGet()
			if mErr == nil {
				return nil
			}
			if batchErr == nil {
				batchErr = batch.NewError(msg, mErr)
			}
			batchErr.Failed(i, mErr)
			return nil
		})
		if batchErr != nil {
			return nil, batchErr
		}
	}

	return batches, nil
}

func (s *strictProcessor) Close(ctx context.Context) error {
	return s.wrapped.Close(ctx)
}

func (s *strictProcessor) UnwrapProc() iprocessor.V1 {
	return s.wrapped
}
