//go:build wasm

package main

import (
	"context"
	"strings"

	"github.com/warpstreamlabs/bento/public/wasm/service"
	plugin "github.com/warpstreamlabs/bento/public/wasm/service/batch_processor"
)

type config struct {
	Prefix string `json:"prefix"`
}

func init() {
	plugin.RegisterBatchProcessor(newUppercaseProcessor)
}

func newUppercaseProcessor(cfg *config, mgr *service.Resources) (service.BatchProcessor, error) {
	return &uppercaseProc{prefix: cfg.Prefix}, nil
}

type uppercaseProc struct {
	prefix string
}

func (p *uppercaseProc) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	for _, part := range batch {
		if bytes, _ := part.AsBytes(); len(bytes) > 0 {
			uppercase := strings.ToUpper(string(bytes))
			if p.prefix != "" {
				uppercase = p.prefix + uppercase
			}
			part.SetBytes([]byte(uppercase))
			part.MetaSet("processed_by", "uppercase_wasm")
		}
	}
	return []service.MessageBatch{batch}, nil
}

func (p *uppercaseProc) Close(ctx context.Context) error {
	return nil
}

func main() {}
