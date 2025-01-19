package strict

import (
	"context"
	"encoding/json"

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
				fields := map[string]string{
					"error":       mErr.Error(),
					"raw_payload": string(p.AsBytes()),
				}

				sobj, parseErr := p.AsStructured()
				if parseErr != nil {
					fields["parse_payload_error"] = parseErr.Error()
				} else {
					// Convert to string representation
					if jsonBytes, err := json.Marshal(sobj); err == nil {
						fields["structured_payload"] = string(jsonBytes)
					} else {
						fields["structured_payload_error"] = "failed to marshal structured payload"
					}
				}

				s.log.WithFields(fields).Error("sample error payload")
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
