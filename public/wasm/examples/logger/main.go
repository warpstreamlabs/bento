//go:build wasm

package main

import (
	"context"

	"github.com/warpstreamlabs/bento/public/wasm/service"
	plugin "github.com/warpstreamlabs/bento/public/wasm/service/processor"
)

func init() {
	if err := plugin.RegisterProcessor(newLoggerProcessor); err != nil {
		panic(err)
	}
}

func newLoggerProcessor(_ *struct{}, res *service.Resources) (service.Processor, error) {
	logger := res.Logger()
	logger.Info("logger plugin initialised")
	return &loggerProc{logger: logger}, nil
}

type loggerProc struct {
	logger *service.Logger
}

func (p *loggerProc) Process(_ context.Context, msg *service.Message) (service.MessageBatch, error) {
	b, _ := msg.AsBytes()
	p.logger.Info("processing message: " + string(b))
	return service.MessageBatch{msg}, nil
}

func main() {}
