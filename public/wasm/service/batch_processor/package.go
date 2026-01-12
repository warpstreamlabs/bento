//go:build wasm

package batch_processor

import (
	"context"

	"github.com/extism/go-pdk"
	"github.com/warpstreamlabs/bento/public/wasm/service"
)

//go:wasmexport process_batch
func process_batch() int32 {
	proc, err := plugin.GetInstance()
	if err != nil {
		pdk.SetError(err)
		return 1
	}

	input := pdk.Input()
	batch, err := service.UnmarshalBatchFromProto(input)
	if err != nil {
		pdk.SetError(err)
		return 1
	}

	ctx := context.Background()
	results, err := proc.ProcessBatch(ctx, batch)
	if err != nil {
		pdk.SetError(err)
		return 1
	}

	result, err := service.MarshalBatchesToProto(results)
	if err != nil {
		pdk.SetError(err)
		return 1
	}

	pdk.Output(result)
	return 0
}
