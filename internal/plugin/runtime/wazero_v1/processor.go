package wazerov1

import (
	"context"
	"encoding/json"

	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/plugin/runtime"
)

type wasmProcessor struct {
	pool runtime.Pool[*instance]
}

func newWasmProcessor(pConf *docs.ParsedConfig, cm *compiledPlugin) (*wasmProcessor, error) {
	var conf []byte
	if pluginConf, ok := pConf.Field(); ok {
		rawConf, err := json.Marshal(pluginConf)
		if err != nil {
			return nil, err
		}
		conf = rawConf
	}

	pool := runtime.NewInstancePool(func(ctx context.Context) (*instance, error) {
		return newInstance(ctx, cm, conf)
	})

	return &wasmProcessor{pool: pool}, nil
}

func (p *wasmProcessor) Process(ctx context.Context, part *message.Part) ([]*message.Part, error) {
	input, err := message.MarshalToProto(part)
	if err != nil {
		return nil, err
	}

	output, err := p.call(ctx, input)
	if err != nil {
		return nil, err
	}

	batch, err := message.UnmarshalBatchFromProto(output)
	if err != nil {
		return nil, err
	}

	for i, out := range batch {
		batch[i] = out.WithContext(part.GetContext())
	}
	return batch, nil
}

func (p *wasmProcessor) ProcessBatch(ctx *processor.BatchProcContext, batch message.Batch) ([]message.Batch, error) {
	input, err := message.MarshalBatchToProto(batch)
	if err != nil {
		return nil, err
	}

	output, err := p.call(ctx.Context(), input)
	if err != nil {
		return nil, err
	}

	return message.UnmarshalBatchesFromProto(output)
}

func (p *wasmProcessor) call(ctx context.Context, input []byte) ([]byte, error) {
	inst, err := p.pool.Get(ctx)
	if err != nil {
		return nil, err
	}

	output, pluginErr, err := inst.call(ctx, input)
	if err != nil {
		_ = inst.Close(ctx)
		return nil, err
	}

	p.pool.Put(inst)
	if pluginErr != nil {
		return nil, pluginErr
	}
	return output, nil
}

func (p *wasmProcessor) Close(ctx context.Context) error {
	return p.pool.Close(ctx)
}
