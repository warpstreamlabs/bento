package processor

import (
	"context"

	"github.com/warpstreamlabs/bento/internal/batch"
	"github.com/warpstreamlabs/bento/internal/message"
)

// WrapWithStrictErrorHandling returns a processor that fails batch processing
// if any message in the batch contains an error.
func WrapWithStrictErrorHandling(proc V1) V1 {
	return &strictProcessor{proc}
}

// strictProcessor fails batch processing if any message contains an error.
type strictProcessor struct {
	V1
}

func (p *strictProcessor) ProcessBatch(ctx context.Context, b message.Batch) ([]message.Batch, error) {
	batches, err := p.V1.ProcessBatch(ctx, b)
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
