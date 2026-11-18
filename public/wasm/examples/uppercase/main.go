//go:build wasm

package main

import (
	"context"
	"strings"

	"github.com/warpstreamlabs/bento/public/wasm/service"
	plugin "github.com/warpstreamlabs/bento/public/wasm/service/processor"
)

type config struct {
	Prefix string `json:"prefix"`
}

func init() {
	if err := plugin.RegisterProcessor(newUppercaseProcessor); err != nil {
		panic(err)
	}
}

func newUppercaseProcessor(cfg *config, mgr *service.Resources) (service.Processor, error) {
	return &uppercaseProc{prefix: cfg.Prefix}, nil
}

type uppercaseProc struct {
	prefix string
}

func (p *uppercaseProc) Process(ctx context.Context, part *service.Message) (service.MessageBatch, error) {
	bytes, _ := part.AsBytes()
	if len(bytes) == 0 {
		return nil, nil
	}

	uppercase := strings.ToUpper(string(bytes))
	if p.prefix != "" {
		uppercase = p.prefix + uppercase
	}
	part.SetBytes([]byte(uppercase))
	part.MetaSet("processed_by", "uppercase_wasm")

	return service.MessageBatch{part}, nil
}

func main() {}
