package main

import (
	"bytes"
	"context"

	"github.com/warpstreamlabs/bento/public/service"
	plugin "github.com/warpstreamlabs/bento/public/wasm/plugin/batch_processor"
)

func init() {
	plugin.RegisterBatchProcessor("uppercase", service.NewConfigSpec().Field(service.NewBoolField("is_enabled")), newUppercaseProcessor)
}

func newUppercaseProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	enabled, _ := conf.FieldBool("is_enabled")

	return &uppercaseProcessor{enabled: enabled}, nil
}

type uppercaseProcessor struct {
	enabled bool
}

func (p *uppercaseProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	if !p.enabled {
		return []service.MessageBatch{batch}, nil
	}

	outBatch := make(service.MessageBatch, len(batch))
	for i, msg := range batch {
		data, _ := msg.AsBytes()
		upperData := bytes.ToUpper(data)
		outBatch[i] = service.NewMessage(upperData)
	}
	return []service.MessageBatch{outBatch}, nil
}

func (p *uppercaseProcessor) Close(ctx context.Context) error {
	return nil
}

func main() {
	if !plugin.IsRegistered() {
		panic("Plugin not registered")
	}

	println("Registered plugin: [ " + plugin.PluginName() + " ]")
}
