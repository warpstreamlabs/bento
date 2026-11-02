//go:build wasm

package processor

import (
	"context"

	"github.com/warpstreamlabs/bento/internal/plugin/wasm"
	"github.com/warpstreamlabs/bento/public/wasm/service"
)

//go:wasmexport process
func process(input wasm.Buffer) wasm.Buffer {
	output, err := service.MarshalProcessResponseToProto(processMessage(input.Slice()))
	if err != nil {
		panic(err)
	}
	return wasm.Retain(output)
}

func processMessage(input []byte) (service.MessageBatch, error) {
	proc, err := plugin.GetInstance()
	if err != nil {
		return nil, err
	}

	msg, err := service.UnmarshalMessageFromProto(input)
	if err != nil {
		return nil, err
	}

	return proc.Process(context.Background(), msg)
}
