package wazerov1

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/tetratelabs/wazero/api"

	"github.com/warpstreamlabs/bento/internal/wasm"
	"github.com/warpstreamlabs/bento/internal/wasm/host"
)

// resultSize is the size of the result area that process returns the address
// of, which holds the buffers of its output and error.
const resultSize = 16

type instance struct {
	api.Module

	alloc   api.Function
	release api.Function
	process api.Function
}

func newInstance(ctx context.Context, cm *compiledPlugin, conf []byte) (*instance, error) {
	mod, err := cm.runtime.InstantiateModule(ctx, cm.compiled, cm.config)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate plugin: %w", err)
	}

	inst := &instance{
		Module:  mod,
		alloc:   mod.ExportedFunction(pluginAllocFn),
		release: mod.ExportedFunction(pluginReleaseFn),
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
	in, err := host.Malloc(ctx, inst, inst.alloc, conf)
	if err != nil {
		return fmt.Errorf("failed to write plugin config: %w", err)
	}

	results, err := fn.Call(ctx, uint64(in))
	if err != nil {
		return err
	}

	message, err := inst.take(ctx, wasm.Buffer(results[0]))
	if err != nil {
		return err
	}

	if err := inst.free(ctx, in); err != nil {
		return err
	}

	if len(message) > 0 {
		return errors.New(string(message))
	}

	return nil
}

func (inst *instance) call(ctx context.Context, input []byte) (output []byte, pluginErr error, err error) {
	in, err := host.Malloc(ctx, inst, inst.alloc, input)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to write plugin input: %w", err)
	}

	defer func() {
		if freeErr := inst.free(ctx, in); freeErr != nil {
			err = errors.Join(err, fmt.Errorf("free plugin input: %w", freeErr))
		}
	}()

	results, err := inst.process.Call(ctx, uint64(in))
	if err != nil {
		return nil, nil, err
	}

	address := api.DecodeU32(results[0])
	data, ok := inst.Memory().Read(address, resultSize)
	if !ok {
		return nil, nil, fmt.Errorf(
			"plugin returned an out of bounds result at %d", address,
		)
	}

	var result wasm.Result
	if _, err := binary.Decode(data, binary.LittleEndian, &result); err != nil {
		return nil, nil, fmt.Errorf("decode plugin result: %w", err)
	}

	if output, err = inst.take(ctx, result.Output); err != nil {
		return nil, nil, err
	}

	message, err := inst.take(ctx, result.Error)
	if err != nil {
		return nil, nil, err
	}

	if len(message) > 0 {
		return nil, errors.New(string(message)), nil
	}
	return output, nil, nil

}

// take copies a buffer returned by the plugin, and releases it.
func (inst *instance) take(ctx context.Context, buffer wasm.Buffer) ([]byte, error) {
	data, ok := inst.Memory().Read(buffer.Address(), buffer.Length())
	if !ok {
		return nil, fmt.Errorf("plugin returned an out of bounds buffer of %d bytes at %d", buffer.Length(), buffer.Address())
	}

	data = bytes.Clone(data)
	if err := inst.free(ctx, buffer); err != nil {
		return nil, err
	}
	return data, nil
}

// free releases a buffer, unless it is empty.
func (inst *instance) free(ctx context.Context, buffer wasm.Buffer) error {
	if buffer == 0 {
		return nil
	}

	if err := host.Free(ctx, inst.release, buffer); err != nil {
		return fmt.Errorf("failed to release plugin buffer: %w", err)
	}

	return nil
}
