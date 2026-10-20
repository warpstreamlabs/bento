//go:build wasm

package batch_processor

import (
	"context"

	"github.com/warpstreamlabs/bento/internal/wasm"
	"github.com/warpstreamlabs/bento/public/wasm/service"
)

//go:wasmexport process_batch
func process_batch(input wasm.Buffer) *wasm.Result {
	result := new(wasm.Result)
	contents, err := processBatch(input.Slice())
	if err != nil {
		result.Error = wasm.FromError(err)
		return result
	}

	result.Output = wasm.Retain(contents)
	return result
}

func processBatch(input []byte) ([]byte, error) {
	proc, err := plugin.GetInstance()
	if err != nil {
		return nil, err
	}

	batch, err := service.UnmarshalBatchFromProto(input)
	if err != nil {
		return nil, err
	}

	results, err := proc.ProcessBatch(context.Background(), batch)
	if err != nil {
		return nil, err
	}

	return service.MarshalBatchesToProto(results)
}
