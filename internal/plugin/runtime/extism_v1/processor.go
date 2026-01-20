package extismv1

import (
	"context"
	"encoding/json"

	extism "github.com/extism/go-sdk"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/plugin/runtime"
)

const (
	pluginInitFn         = "init_plugin"
	pluginProcessFn      = "process"
	pluginProcessBatchFn = "process_batch"
)

type wasmProcessor struct {
	compiled *extism.CompiledPlugin
	pool     runtime.Pool[*extism.Plugin]
}

func newWasmProcessor(
	pConf *docs.ParsedConfig,
	compiled *extism.CompiledPlugin,
) (*wasmProcessor, error) {
	var conf []byte
	if pluginConf, ok := pConf.Field(); ok {
		rawConf, err := json.Marshal(pluginConf)
		if err != nil {
			return nil, err
		}
		conf = rawConf
	}

	pool := runtime.NewInstancePool(func(ctx context.Context) (*extism.Plugin, error) {
		instance, err := compiled.Instance(ctx, extism.PluginInstanceConfig{})
		if err != nil {
			return nil, err
		}

		if _, _, err = instance.CallWithContext(ctx, pluginInitFn, conf); err != nil {
			return nil, err
		}

		return instance, nil
	})

	return &wasmProcessor{compiled: compiled, pool: pool}, nil
}

func (p *wasmProcessor) ProcessBatch(ctx *processor.BatchProcContext, batch message.Batch) ([]message.Batch, error) {
	batchBytes, err := message.MarshalBatchToProto(batch)
	if err != nil {
		return nil, err
	}

	wasmPlugin, err := p.pool.Get(ctx.Context())
	if err != nil {
		return nil, err
	}
	defer p.pool.Put(wasmPlugin)

	_, rawBatches, err := wasmPlugin.CallWithContext(ctx.Context(), pluginProcessBatchFn, batchBytes)
	if err != nil {
		return nil, err
	}

	batches, err := message.UnmarshalBatchesFromProto(rawBatches)
	if err != nil {
		return nil, err
	}

	return batches, nil
}

func (p *wasmProcessor) Process(ctx *processor.BatchProcContext, msg *message.Part) (message.Batch, error) {
	batchBytes, err := message.MarshalToProto(msg)
	if err != nil {
		return nil, err
	}

	wasmPlugin, err := p.pool.Get(ctx.Context())
	if err != nil {
		return nil, err
	}
	defer p.pool.Put(wasmPlugin)

	_, rawBatches, err := wasmPlugin.CallWithContext(ctx.Context(), pluginProcessFn, batchBytes)
	if err != nil {
		return nil, err
	}

	batch, err := message.UnmarshalBatchFromProto(rawBatches)
	if err != nil {
		return nil, err
	}

	return batch, nil
}

func (p *wasmProcessor) Close(ctx context.Context) error {
	if err := p.compiled.Close(ctx); err != nil {
		return err
	}

	return p.pool.Close(ctx)
}
