package main

import (
	"context"
	"slices"

	"github.com/warpstreamlabs/bento/public/service"
	plugin "github.com/warpstreamlabs/bento/public/wasm/plugin/batch_processor"
)

func init() {
	plugin.RegisterBatchProcessor("reverse", service.NewConfigSpec(), newReverseProcessor)
}

func newReverseProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	return &reverseProc{}, nil
}

type reverseProc struct{}

func (p *reverseProc) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	for _, msg := range batch {
		data, _ := msg.AsBytes()
		slices.Reverse(data)
		msg.SetBytes(data)
	}
	return []service.MessageBatch{batch}, nil
}

func (p *reverseProc) Close(ctx context.Context) error {
	return nil
}

func main() {
	if !plugin.IsRegistered() {
		panic("Plugin not registered")
	}
}
