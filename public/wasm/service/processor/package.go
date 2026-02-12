//go:build wasm

package processor

import (
	"context"

	"github.com/extism/go-pdk"
	"github.com/warpstreamlabs/bento/public/wasm/service"
)

//go:wasmexport process
func process() int32 {
	proc, err := plugin.GetInstance()
	if err != nil {
		pdk.SetError(err)
		return 1
	}

	input := pdk.Input()
	msg, err := service.UnmarshalMessageFromProto(input)
	if err != nil {
		pdk.SetError(err)
		return 1
	}

	ctx := context.Background()
	results, err := proc.Process(ctx, msg)
	if err != nil {
		pdk.SetError(err)
		return 1
	}

	result, err := service.MarshalBatchToProto(results)
	if err != nil {
		pdk.SetError(err)
		return 1
	}

	pdk.Output(result)
	return 0
}
