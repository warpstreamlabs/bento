//go:build wasm

package batch_processor

import (
	"context"

	"github.com/warpstreamlabs/bento/internal/plugin/wasm"
	"github.com/warpstreamlabs/bento/public/wasm/service"
)

//go:wasmexport process_batch
func process_batch(input wasm.Buffer) wasm.Buffer {
	output, err := service.MarshalProcessBatchResponseToProto(processBatch(input.Slice()))
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

	batch, err := service.UnmarshalBatchFromProto(input)
	if err != nil {
		return nil, err
	}

	return proc.ProcessBatch(context.Background(), batch)
}
