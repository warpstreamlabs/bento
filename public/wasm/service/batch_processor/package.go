//go:build wasm

package batch_processor

import (
	"context"

	messagepb "github.com/warpstreamlabs/bento/internal/message/messagepblite"
	"github.com/warpstreamlabs/bento/internal/plugin/wasm"
	"github.com/warpstreamlabs/bento/public/wasm/service"
)

//go:wasmexport process_batch
func process_batch(address, length uint32) wasm.Buffer {
	res := &messagepb.ProcessBatch_Response{}
	batches, err := processBatch(wasm.NewBuffer(address, length).Slice())

	if err != nil {
		res.Error = &messagepb.Error{Message: err.Error()}
	} else {
		res.Batches = toProtoBatches(batches)
	}

	output, err := res.MarshalVT()
	if err != nil {
		panic(err)
	}
	return wasm.Retain(output)
}

func processBatch(input []byte) ([]service.MessageBatch, error) {
	proc, err := plugin.GetInstance()
	if err != nil {
		return nil, err
	}

	req := &messagepb.ProcessBatch_Request{}
	if err := req.UnmarshalVT(input); err != nil {
		return nil, err
	}

	return proc.ProcessBatch(context.Background(), fromProtoBatch(req.GetBatch()))
}

func fromProtoBatch(batch *messagepb.Batch) service.MessageBatch {
	result := make(service.MessageBatch, len(batch.GetParts()))
	for i, part := range batch.GetParts() {
		result[i] = service.NewMessageFromProto(part)
	}
	return result
}

func toProtoBatches(batches []service.MessageBatch) *messagepb.Batches {
	out := &messagepb.Batches{Batches: make([]*messagepb.Batch, len(batches))}
	for i, batch := range batches {
		out.Batches[i] = toProtoBatch(batch)
	}
	return out
}

func toProtoBatch(batch service.MessageBatch) *messagepb.Batch {
	parts := make([]*messagepb.Part, len(batch))
	for i, msg := range batch {
		parts[i] = msg.XUnwrap().(*messagepb.Part)
	}
	return &messagepb.Batch{Parts: parts}
}
