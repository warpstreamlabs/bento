//go:build wasm

package processor

import (
	"context"

	messagepb "github.com/warpstreamlabs/bento/internal/message/messagepblite"
	"github.com/warpstreamlabs/bento/internal/plugin/wasm"
	"github.com/warpstreamlabs/bento/public/wasm/service"
)

//go:wasmexport process
func process(input wasm.Buffer) wasm.Buffer {
	res := &messagepb.Process_Response{}

	batch, err := processMessage(input.Slice())
	if err != nil {
		res.Error = &messagepb.Error{Message: err.Error()}
	} else {
		res.Batch = toProtoBatch(batch)
	}

	output, err := res.MarshalVT()
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

	req := &messagepb.Process_Request{}
	if err := req.UnmarshalVT(input); err != nil {
		return nil, err
	}

	return proc.Process(context.Background(), service.NewMessageFromProto(req.GetPart()))
}

func toProtoBatch(batch service.MessageBatch) *messagepb.Batch {
	parts := make([]*messagepb.Part, len(batch))
	for i, msg := range batch {
		parts[i] = msg.XUnwrap().(*messagepb.Part)
	}
	return &messagepb.Batch{Parts: parts}
}
