//go:build !wasm

package model

import (
	"context"
	"fmt"

	"github.com/tetratelabs/wazero/api"
)

func Malloc(ctx context.Context, module api.Module, data []byte) (Buffer, error) {
	results, err := module.ExportedFunction("malloc").Call(ctx, uint64(len(data)))
	if err != nil {
		return 0, err
	}
	buffer := Buffer(results[0])
	if !module.Memory().Write(buffer.Address(), data) {
		return 0, fmt.Errorf("write to memory out of range")
	}
	return buffer, nil
}

func Free(ctx context.Context, module api.Module, buffer Buffer) error {
	_, err := module.ExportedFunction("free").Call(ctx, uint64(buffer))
	if err != nil {
		return err
	}

	return nil
}

func LoadString(module api.Module, value String) string {
	return string(LoadBytes(module, Buffer(value)))
}

func LoadBytes(module api.Module, value Buffer) []byte {
	if value == 0 {
		return nil
	}
	data, ok := module.Memory().Read(value.Address(), value.Length())
	if !ok {
		panic("memory read out of bounds")
	}
	return data
}
