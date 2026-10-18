//go:build wasm

package processor

import (
	"context"

	"github.com/warpstreamlabs/bento/internal/wasm"
	"github.com/warpstreamlabs/bento/public/wasm/service"
)

// result is shared by all process calls in this instance.
// Its address remains valid for the instance's lifetime.
// The host must consume its contents before the next process call.
// Calls must not overlap or reenter process.
var result wasm.Result

//go:wasmexport process
func process(input wasm.Buffer) *wasm.Result {
	result.Set(processMessage(input.Slice()))
	return &result
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
