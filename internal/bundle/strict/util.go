package strict

import (
	"github.com/warpstreamlabs/bento/internal/batch"
	"github.com/warpstreamlabs/bento/internal/message"
)

func hasErr(msgBatch message.Batch) error {
	// Iterate through all messages and populate a batch.Error type, calling Failed()
	// for each errored message. Otherwise, every message in the batch is treated as a failure.

	var batchErr *batch.Error
	_ = msgBatch.Iter(func(i int, p *message.Part) error {
		mErr := p.ErrorGet()
		if mErr == nil {
			return nil
		}
		if batchErr == nil {
			batchErr = batch.NewError(msgBatch, mErr)
		}
		batchErr.Failed(i, mErr)
		return nil
	})
	if batchErr != nil {
		return batchErr
	}

	return nil

}
