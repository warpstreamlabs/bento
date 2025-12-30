//go:build !wasm

package runtime

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/tetratelabs/wazero/api"
	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/plugin"
	"github.com/warpstreamlabs/bento/internal/plugin/model"
)

type WasmPooledProcessor struct {
	pool plugin.Pool[api.Module]
	conf []byte
}

func newWasmPooledProcessor(pconf *docs.ParsedConfig, pool plugin.Pool[api.Module]) (*WasmPooledProcessor, error) {
	conf := []byte{}
	if pluginConf, ok := pconf.Field(); ok {
		rawConf, err := json.Marshal(pluginConf)
		if err != nil {
			return nil, err
		}
		conf = rawConf
	}

	return &WasmPooledProcessor{
		pool: pool,
		conf: conf,
	}, nil
}

func (p *WasmPooledProcessor) ProcessBatch(ctx context.Context, batch message.Batch) ([]message.Batch, error) {
	module, err := p.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer p.pool.Put(module)

	// Note: This should be idempotent
	if err := initComponent(ctx, module, p.conf); err != nil {
		return nil, err
	}

	processFn := module.ExportedFunction("process_batch")
	if processFn == nil {
		return nil, fmt.Errorf("process_batch function not exported")
	}

	serialized, err := message.MarshalBatchToProto(batch)
	if err != nil {
		return nil, err
	}

	buffer, err := model.Malloc(ctx, module, serialized)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate memory: %w", err)
	}
	defer model.Free(ctx, module, buffer)

	results, err := processFn.Call(ctx, uint64(buffer))
	if err != nil {
		return nil, fmt.Errorf("process call failed: %w", err)
	}

	resultBuffer := model.Buffer(results[0])
	buff := model.LoadBytes(module, resultBuffer)

	result, err := message.UnmarshalBatchesFromProto(buff)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (p *WasmPooledProcessor) Process(ctx context.Context, msg *message.Part) (message.Batch, error) {
	module, err := p.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer p.pool.Put(module)

	if err := initComponent(ctx, module, p.conf); err != nil {
		return nil, err
	}

	processFn := module.ExportedFunction("process")
	if processFn == nil {
		return nil, fmt.Errorf("process function not exported")
	}

	serialized := msg.AsBytes()

	buffer, err := model.Malloc(ctx, module, serialized)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate memory: %w", err)
	}
	defer model.Free(ctx, module, buffer)

	results, err := processFn.Call(ctx, uint64(buffer))
	if err != nil {
		return nil, fmt.Errorf("process call failed: %w", err)
	}

	resultBuffer := model.Buffer(results[0])
	buff := model.LoadBytes(module, resultBuffer)

	return message.FromBytes(buff)
}

func (p *WasmPooledProcessor) Close(ctx context.Context) error {
	return p.pool.Close(ctx)
}

func initComponent(ctx context.Context, module api.Module, config []byte) error {
	initFn := module.ExportedFunction("init_component")
	if initFn == nil {
		return fmt.Errorf("init_component function not exported")
	}

	buffer, err := model.Malloc(ctx, module, config)
	if err != nil {
		return fmt.Errorf("failed to write config to WASM memory: %w", err)
	}
	defer model.Free(ctx, module, buffer)

	results, err := initFn.Call(ctx, uint64(buffer))
	if err != nil {
		return fmt.Errorf("failed to call init_component: %w", err)
	}

	if len(results) == 0 {
		return nil
	}

	if results[0] == 0 {
		return nil
	}

	errMsg := model.LoadString(module, model.String(results[0]))
	if len(errMsg) > 0 {
		return fmt.Errorf("init_component failed: %s", errMsg)
	}

	return nil
}
