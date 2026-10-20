//go:build wasm

package processor

import (
	"context"

	"github.com/warpstreamlabs/bento/internal/wasm"
	"github.com/warpstreamlabs/bento/public/wasm/service"
)

//go:wasmexport process
func process(input wasm.Buffer) *wasm.Result {
	result := new(wasm.Result)
	contents, err := processMessage(input.Slice())
	if err != nil {
		result.Error = wasm.FromError(err)
		return result
	}

	result.Output = wasm.Retain(contents)
	return result
}

func processMessage(input []byte) ([]byte, error) {
	proc, err := plugin.GetInstance()
	if err != nil {
		return nil, err
	}

	msg, err := service.UnmarshalMessageFromProto(input)
	if err != nil {
		return nil, err
	}

	results, err := proc.Process(context.Background(), msg)
	if err != nil {
		return nil, err
	}

	return service.MarshalBatchToProto(results)
}
