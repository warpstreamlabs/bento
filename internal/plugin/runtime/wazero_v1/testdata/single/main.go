//go:build wasm

// Command single is a processor plugin that only exports process, so the host
// calls it once per message.
package main

import (
	"bytes"
	"context"
	"errors"

	"github.com/warpstreamlabs/bento/public/wasm/service"
	plugin "github.com/warpstreamlabs/bento/public/wasm/service/processor"
)

func init() {
	if err := plugin.RegisterProcessor(func(_ *struct{}, _ *service.Resources) (service.Processor, error) {
		return &proc{}, nil
	}); err != nil {
		panic(err)
	}
}

type proc struct{}

func (p *proc) Process(_ context.Context, msg *service.Message) (service.MessageBatch, error) {
	b, _ := msg.AsBytes()
	switch string(b) {
	case "fail":
		msg.SetBytes([]byte("changed"))
		return nil, errors.New("failed")
	case "drop":
		return nil, nil
	case "trap":
		panic("trapped")
	}
	return service.MessageBatch{msg, service.NewMessage(bytes.ToUpper(b))}, nil
}

func (p *proc) Close(context.Context) error {
	return nil
}

func main() {}
