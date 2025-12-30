package main

import (
	"bytes"
	"context"
	"sort"

	"github.com/warpstreamlabs/bento/public/bloblang"
	"github.com/warpstreamlabs/bento/public/service"
	plugin "github.com/warpstreamlabs/bento/public/wasm/plugin/batch_processor"
)

func sortProcSpec() *service.ConfigSpec {
	return service.NewConfigSpec().Field(
		service.NewBloblangField("query"),
	)
}

func init() {
	plugin.RegisterBatchProcessor("sort",
		sortProcSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			sortFn, err := conf.FieldBloblang("query")
			if err != nil {
				return nil, err
			}

			return &sortProc{
				exec: sortFn,
			}, nil
		})
}

type sortProc struct {
	exec *bloblang.Executor
}

func (p *sortProc) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	outBatch := make(service.MessageBatch, len(batch))
	copy(outBatch, batch)

	sort.Slice(outBatch, func(i, j int) bool {
		aMsg, err := outBatch[i].BloblangQuery(p.exec)
		if err != nil {
			return false
		}
		aVal, err := aMsg.AsBytes()
		if err != nil {
			return false
		}

		bMsg, err := outBatch[j].BloblangQuery(p.exec)
		if err != nil {
			return false
		}

		bVal, err := bMsg.AsBytes()
		if err != nil {
			return false
		}

		return bytes.Compare(aVal, bVal) < 0
	})

	return []service.MessageBatch{outBatch}, nil
}

func (p *sortProc) Close(ctx context.Context) error {
	return nil
}

func main() {
	if !plugin.IsRegistered() {
		panic("Plugin not registered")
	}
}
