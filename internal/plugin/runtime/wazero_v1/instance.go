package wazerov1

import (
	"context"
	"errors"
	"fmt"

	"github.com/tetratelabs/wazero/api"

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

	results, err := fn.Call(ctx, uint64(in))
	if err != nil {
		return err
	}

	message, err := host.Take(ctx, inst.Memory(), inst.free, wasm.Buffer(results[0]))
	if err != nil {
		return err
	}

	if err := host.Free(ctx, inst.free, in); err != nil {
		return err
	}

	if len(message) > 0 {
		return errors.New(string(message))
	}
	return nil
}

func (inst *instance) call(ctx context.Context, input []byte) (_ []byte, err error) {
	in, err := host.Malloc(ctx, inst.Memory(), inst.malloc, input)
	if err != nil {
		return nil, fmt.Errorf("failed to write plugin input: %w", err)
	}

	defer func() {
		if freeErr := host.Free(ctx, inst.free, in); freeErr != nil {
			err = errors.Join(err, fmt.Errorf("free plugin input: %w", freeErr))
		}
	}()

	results, err := inst.process.Call(ctx, uint64(in))
	if err != nil {
		return nil, err
	}

	return host.Take(ctx, inst.Memory(), inst.free, wasm.Buffer(results[0]))
}
