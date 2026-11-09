package wazerov1

import (
	"context"
	"errors"
	"fmt"

	"github.com/tetratelabs/wazero/api"
	"google.golang.org/protobuf/proto"

	"github.com/warpstreamlabs/bento/internal/plugin/wasm"
	"github.com/warpstreamlabs/bento/internal/plugin/wasm/host"
)

type instance struct {
	api.Module

	malloc  api.Function
	free    api.Function
	process api.Function
}

func newInstance(ctx context.Context, cm *compiledPlugin, conf []byte) (*instance, error) {
	mod, err := cm.runtime.InstantiateModule(ctx, cm.compiled, cm.config)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate plugin: %w", err)
	}

	inst := &instance{
		Module:  mod,
		malloc:  mod.ExportedFunction(pluginMallocFn),
		free:    mod.ExportedFunction(pluginFreeFn),
		process: mod.ExportedFunction(cm.processFn),
	}

	if init := mod.ExportedFunction(pluginInitFn); init != nil {
		if err := inst.init(ctx, init, conf); err != nil {
			_ = mod.Close(ctx)
			return nil, err
		}
	}
	return inst, nil
}

func (inst *instance) init(ctx context.Context, fn api.Function, conf []byte) error {
	in, err := host.Malloc(ctx, inst.Memory(), inst.malloc, conf)
	if err != nil {
		return fmt.Errorf("failed to write plugin config: %w", err)
	}

	results, err := fn.Call(ctx, uint64(in.Address()), uint64(in.Length()))
	if err != nil {
		return err
	}

	out := wasm.Buffer(results[0])
	message, err := host.Read(inst.Memory(), out)
	if err != nil {
		return err
	}
	if len(message) > 0 {
		err = errors.New(string(message))
	}
	return errors.Join(err, host.Free(ctx, inst.free, out), host.Free(ctx, inst.free, in))
}

func (inst *instance) call(ctx context.Context, input []byte, res proto.Message) error {
	in, err := host.Malloc(ctx, inst.Memory(), inst.malloc, input)
	if err != nil {
		return fmt.Errorf("failed to write plugin input: %w", err)
	}

	defer func() {
		if freeErr := host.Free(ctx, inst.free, in); freeErr != nil {
			err = errors.Join(err, fmt.Errorf("free plugin input: %w", freeErr))
		}
	}()

	results, err := inst.process.Call(ctx, uint64(in.Address()), uint64(in.Length()))
	if err != nil {
		return err
	}

	out := wasm.Buffer(results[0])
	output, err := host.Read(inst.Memory(), out)
	if err != nil {
		return err
	}
	return errors.Join(proto.Unmarshal(output, res), host.Free(ctx, inst.free, out))
}
