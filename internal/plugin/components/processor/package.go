//go:build wasm

package processor

import (
	"context"

	"github.com/warpstreamlabs/bento/internal/component/interop/private"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/plugin/model"
	"github.com/warpstreamlabs/bento/public/service"
)

//go:wasmexport process
func process(msgBuffer uint64) model.Buffer {
	proc, err := plugin.GetInstance()
	if err != nil {
		panic(err)
	}

	buffer := model.Buffer(msgBuffer)

	msg := message.NewPart(buffer.Slice())

	ctx := context.Background()
	result, err := proc.Process(ctx, service.NewInternalMessage(msg))
	if err != nil {
		panic(err)
	}

	if len(result) == 0 {
		return model.FromSlice([]byte{})
	}

	outBatch := private.FromInternal(result.XUnwrap())
	resultData, _ := message.MarshalBatchToProto(outBatch)
	return model.FromSlice(resultData)
}
