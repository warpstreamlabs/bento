package processor

import (
	"context"

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

	for _, msg := range batches {
		err = msg.Iter(func(i int, p *message.Part) error {
			if mErr := p.ErrorGet(); mErr != nil {
				return mErr
			}
			return nil
		})
		if err != nil {
			return nil, err
		}

	}

	return batches, nil
}
