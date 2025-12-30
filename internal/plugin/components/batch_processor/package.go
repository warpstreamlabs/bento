//go:build wasm

package batch_processor

import (
	"context"

	"github.com/warpstreamlabs/bento/internal/component/interop/private"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/plugin/model"
	"github.com/warpstreamlabs/bento/public/service"
)

//go:wasmexport process_batch
func process_batch(batchBuffer uint64) model.Buffer {
	proc, err := plugin.GetInstance()
	if err != nil {
		panic(err)
	}

	buffer := model.Buffer(batchBuffer)
	slice := buffer.Slice()

	batch, err := message.UnmarshalBatchFromProto(slice)
	if err != nil {
		panic(err.Error())
	}

	inBatch := make(service.MessageBatch, len(batch))
	for i, msg := range batch {
		inBatch[i] = service.NewInternalMessage(msg)
	}

	ctx := context.Background()

	// FIXME(gregfurman): This internal <-> public casting needs a better solution.
	result, err := proc.ProcessBatch(ctx, inBatch)
	if err != nil {
		panic(err)
	}

	if len(result) == 0 {
		return model.FromSlice([]byte{})
	}

	outBatches := make([]message.Batch, len(result))
	for i, batch := range result {
		outBatches[i] = private.FromInternal(batch.XUnwrap())
	}

	resultData, err := message.MarshalBatchesToProto(outBatches)
	if err != nil {
		panic(err.Error())
	}

	return model.FromSlice(resultData)
}
