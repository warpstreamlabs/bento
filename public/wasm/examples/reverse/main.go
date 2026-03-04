//go:build wasm

package main

import (
	"context"
	"fmt"
	"slices"

	"github.com/warpstreamlabs/bento/public/wasm/service"
	plugin "github.com/warpstreamlabs/bento/public/wasm/service/batch_processor"
)

func init() {
	plugin.RegisterBatchProcessor(newReverseProcessor)
}

func newReverseProcessor(_ *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	return &reverseProc{}, nil
}

type reverseProc struct{}

func (p *reverseProc) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	for i, part := range batch {
		part.MetaSet("reversed_position", fmt.Sprintf("%d", i))
		part.MetaSet("processed_by", "reverse_wasm")
	}

	slices.Reverse(batch)
	return []service.MessageBatch{batch}, nil
}

func (p *reverseProc) Close(ctx context.Context) error {
	return nil
}

func main() {}
