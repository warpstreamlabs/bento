package wazerov1

import (
	"context"
	"encoding/json"
	"errors"

	"google.golang.org/protobuf/proto"

	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/message"
	pb "github.com/warpstreamlabs/bento/internal/message/messagepb"
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

	output, err := inst.call(ctx, input)
	if err != nil {
		_ = inst.Close(ctx)
		return nil, err
	}
	p.pool.Put(inst)

	res := &pb.Process_Response{}
	if err := proto.Unmarshal(output, res); err != nil {
		return nil, err
	}
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

	output, err := inst.call(ctx.Context(), input)
	if err != nil {
		_ = inst.Close(ctx.Context())
		return nil, err
	}
	p.pool.Put(inst)

	res := &pb.ProcessBatch_Response{}
	if err := proto.Unmarshal(output, res); err != nil {
		return nil, err
	}

	if protoErr := res.GetError(); protoErr != nil {
		return nil, errors.New(protoErr.GetMessage())
	}

	return message.BatchesFromProto(res.GetBatches())
}

func (p *wasmProcessor) Close(ctx context.Context) error {
	return p.pool.Close(ctx)
}
