package errorsampling

import (
	"context"
	"math/rand/v2"
	"sort"

	iprocessor "github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/errorhandling"
	"github.com/warpstreamlabs/bento/internal/log"
	"github.com/warpstreamlabs/bento/internal/message"
)

func wrapWithErrorLogger(p iprocessor.V1, l log.Modular, conf errorhandling.Config) iprocessor.V1 {
	t := &processorWithErrorLogger{
		wrapped:       p,
		log:           l,
		errSampleRate: conf.Log.SamplingRatio,
		addPayload:    conf.Log.AddPayload,
		enabled:       conf.Log.Enabled,
	}
	return t
}

//------------------------------------------------------------------------------

// errSamplingProcessor randomly samples from a batch of errored messages and logs them.
type processorWithErrorLogger struct {
	log           log.Modular
	wrapped       iprocessor.V1
	errSampleRate float64
	addPayload    bool
	enabled       bool
}

func (s *processorWithErrorLogger) ProcessBatch(ctx context.Context, b message.Batch) ([]message.Batch, error) {
	if !s.enabled {
		return s.wrapped.ProcessBatch(ctx, b)
	}

	batches, err := s.wrapped.ProcessBatch(ctx, b)
	if err != nil {
		return nil, err
	}

	for _, batch := range batches {
		var errorMessageIDs []int

		batchSize := len(batch)
		// shuffle the batch indices to allow random sampling when retrieving message information
		shuffledMessageIDs := rand.Perm(batchSize)

		for _, id := range shuffledMessageIDs {
			part := batch.Get(id)
			mErr := part.ErrorGet()
			if mErr == nil {
				continue
			}
			errorMessageIDs = append(errorMessageIDs, id)
		}

		if len(errorMessageIDs) == 0 {
			continue
		}

		errCount := len(errorMessageIDs)
		sampleSize := min(int(float64(errCount)*s.errSampleRate), len(batch))

		// subset the message IDs when sample size is set
		errorMessageIDs = errorMessageIDs[:sampleSize]
		// sort the subset of message IDs so we get ordered logging
		sort.Ints(errorMessageIDs)

		for _, id := range errorMessageIDs {
			part := batch.Get(id)
			pErr := part.ErrorGet()

			if pErr == nil {
				// shouldn't reach this
				continue
			}

			fields := map[string]string{
				"error": pErr.Error(),
			}

			if s.addPayload {
				fields["raw_payload"] = string(part.AsBytes())
			}

			s.log.WithFields(fields).Error("failed processing")
		}
	}

	return batches, nil
}

func (s *processorWithErrorLogger) Close(ctx context.Context) error {
	return s.wrapped.Close(ctx)
}

func (s *processorWithErrorLogger) UnwrapProc() iprocessor.V1 {
	return s.wrapped
}
