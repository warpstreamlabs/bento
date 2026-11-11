//go:build wasm

package batch_processor

import (
	"context"
	"fmt"

	messagepb "github.com/warpstreamlabs/bento/internal/message/messagepblite"
	"github.com/warpstreamlabs/bento/internal/plugin/wasm"
	"github.com/warpstreamlabs/bento/public/wasm/service"
)

//go:wasmexport process_batch
func process_batch(address, length uint32) wasm.Buffer {
	res := &messagepb.ProcessBatch_Response{}
	batches, err := processBatch(wasm.NewBuffer(address, length).Slice())
	if err == nil {
		res.Batches, err = toProtoBatches(batches)
	}
	if err != nil {
		res.Error = &messagepb.Error{Message: err.Error()}
		res.Batches = nil
	}

	output := wasm.Alloc(uint32(res.SizeVT()))
	if _, err := res.MarshalToSizedBufferVT(output.Slice()); err != nil {
		panic(err)
	}
	return output
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

func toProtoBatches(batches []service.MessageBatch) (*messagepb.Batches, error) {
	result := &messagepb.Batches{Batches: make([]*messagepb.Batch, len(batches))}
	for i, batch := range batches {
		b, err := toProtoBatch(batch)
		if err != nil {
			return nil, err
		}
		result.Batches[i] = b
	}
	return result, nil
}

func toProtoBatch(batch service.MessageBatch) (*messagepb.Batch, error) {
	parts := make([]*messagepb.Part, len(batch))
	for i, msg := range batch {
		part, ok := msg.XUnwrap().(*messagepb.Part)
		if !ok {
			return nil, fmt.Errorf("message %d: unexpected type %T", i, msg.XUnwrap())
		}
		parts[i] = part
	}
	return &messagepb.Batch{Parts: parts}, nil
}
