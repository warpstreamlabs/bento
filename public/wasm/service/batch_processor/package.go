//go:build wasm

package batch_processor

import (
	"context"

	"github.com/warpstreamlabs/bento/internal/wasm"
	"github.com/warpstreamlabs/bento/public/wasm/service"
)

// result is returned by process_batch, and lives as long as the instance. An
// instance makes one call at a time, so one is enough.
var result wasm.Result

//go:wasmexport process_batch
func process_batch(input wasm.Buffer) *wasm.Result {
	result.Set(processBatch(input.Slice()))
	return &result
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
