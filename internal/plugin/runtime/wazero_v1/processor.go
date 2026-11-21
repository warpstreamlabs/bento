package wazerov1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/experimental"
	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/message"
	pb "github.com/warpstreamlabs/bento/internal/message/messagepb"
	"github.com/warpstreamlabs/bento/internal/plugin/runtime"
)

type wasmProcessor struct {
	pool  runtime.Pool[*instance]
	hosts map[string]api.Module
}

func newWasmProcessor(pConf *docs.ParsedConfig, cm *compiledPlugin, mgr bundle.NewManagement) (*wasmProcessor, error) {
	proc := &wasmProcessor{}

	var conf []byte
	if pluginConf, ok := pConf.Field(); ok {
		rawConf, err := json.Marshal(pluginConf)
		if err != nil {
			return nil, err
		}
		conf = rawConf
	}

	proc.hosts = make(map[string]api.Module, len(cm.hosts))
	for _, provider := range cm.hosts {
		mod, err := provider.Instantiate(context.TODO(), cm.runtime, mgr)
		if err != nil {
			_ = proc.Close(context.TODO())
			return nil, fmt.Errorf("failed to instantiate host module %q: %w", provider.Name(), err)
		}
		proc.hosts[provider.Name()] = mod
	}

	proc.pool = runtime.NewInstancePool(func(ctx context.Context) (*instance, error) {
		if len(proc.hosts) > 0 {
			ctx = experimental.WithImportResolver(ctx, func(name string) api.Module {
				return proc.hosts[name]
			})
		}
		return newInstance(ctx, cm, conf)
	})

	return proc, nil
}

func (p *wasmProcessor) Process(ctx context.Context, part *message.Part) ([]*message.Part, error) {
	protoPart, err := message.PartToProto(part)
	if err != nil {
		return nil, err
	}

	input, err := proto.Marshal(&pb.Process_Request{Part: protoPart})
	if err != nil {
		return nil, err
	}

	inst, err := p.pool.Get(ctx)
	if err != nil {
		return nil, err
	}

	res := &pb.Process_Response{}
	if err := inst.call(ctx, input, res); err != nil {
		_ = inst.Close(ctx)
		return nil, err
	}
	p.pool.Put(inst)

	if protoErr := res.GetError(); protoErr != nil {
		return nil, errors.New(protoErr.GetMessage())
	}

	batch, err := message.BatchFromProto(res.GetBatch())
	if err != nil {
		return nil, err
	}

	for i, out := range batch {
		batch[i] = out.WithContext(part.GetContext())
	}
	return batch, nil
}

func (p *wasmProcessor) ProcessBatch(ctx *processor.BatchProcContext, batch message.Batch) ([]message.Batch, error) {
	protoBatch, err := message.BatchToProto(batch)
	if err != nil {
		return nil, err
	}

	input, err := proto.Marshal(&pb.ProcessBatch_Request{Batch: protoBatch})
	if err != nil {
		return nil, err
	}

	inst, err := p.pool.Get(ctx.Context())
	if err != nil {
		return nil, err
	}

	res := &pb.ProcessBatch_Response{}
	if err := inst.call(ctx.Context(), input, res); err != nil {
		_ = inst.Close(ctx.Context())
		return nil, err
	}
	p.pool.Put(inst)

	if protoErr := res.GetError(); protoErr != nil {
		return nil, errors.New(protoErr.GetMessage())
	}

	return message.BatchesFromProto(res.GetBatches())
}

func (p *wasmProcessor) Close(ctx context.Context) error {
	var errs []error
	if p.pool != nil {
		errs = append(errs, p.pool.Close(ctx))
	}

	for _, mod := range p.hosts {
		errs = append(errs, mod.Close(ctx))
	}

	return errors.Join(errs...)
}
