package strict

import (
	"context"

	"math/rand/v2"

	"github.com/warpstreamlabs/bento/internal/bundle"
	iprocessor "github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/log"
	"github.com/warpstreamlabs/bento/internal/message"
)

func wrapWithErrorSampling(p iprocessor.V1, mgr bundle.NewManagement, rate float64) iprocessor.V1 {
	t := &errSamplingProcessor{
		wrapped:       p,
		log:           mgr.Logger(),
		errSampleRate: rate,
		enabled:       true,
	}
	return t
}

//------------------------------------------------------------------------------

func shouldSampleError(sampleRate float64) bool {
	if sampleRate <= 0 {
		return false
	}
	return rand.Float64() <= sampleRate
}

//------------------------------------------------------------------------------

// errSamplingProcessor randomly samples from a batch of errored messages and logs them.
type errSamplingProcessor struct {
	log           log.Modular
	wrapped       iprocessor.V1
	errSampleRate float64
	enabled       bool
}

func (s *errSamplingProcessor) ProcessBatch(ctx context.Context, b message.Batch) ([]message.Batch, error) {
	if !s.enabled {
		return s.wrapped.ProcessBatch(ctx, b)
	}

	batches, err := s.wrapped.ProcessBatch(ctx, b)
	if err != nil {
		return nil, err
	}

	for _, msg := range batches {
		_ = msg.Iter(func(i int, p *message.Part) error {
			mErr := p.ErrorGet()
			if mErr == nil {
				return nil
			}

			if shouldSampleError(s.errSampleRate) {
				sobj, err := p.AsStructured()
				if err != nil {
					s.log.Error("failed to extract sample event: %w", err)
				} else {
					s.log.Error("sample payload from failed message: %v", sobj)
				}
			}
			return nil
		})
	}

	return batches, nil
}

func (s *errSamplingProcessor) Close(ctx context.Context) error {
	return s.wrapped.Close(ctx)
}

func (s *errSamplingProcessor) UnwrapProc() iprocessor.V1 {
	return s.wrapped
}
