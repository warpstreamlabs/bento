package host

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/tetratelabs/wazero/api"

	"github.com/warpstreamlabs/bento/internal/plugin/wasm"
)

// Malloc copies data into a buffer allocated with the malloc export of the
// plugin, which must be released with Free.
func Malloc(ctx context.Context, memory api.Memory, malloc api.Function, data []byte) (wasm.Buffer, error) {
	if len(data) == 0 {
		return 0, nil
	}

	results, err := malloc.Call(ctx, uint64(len(data)))
	if err != nil {
		return 0, err
	}

	buffer := wasm.NewBuffer(api.DecodeU32(results[0]), uint32(len(data)))
	if buffer.Address() == 0 {
		return 0, fmt.Errorf("malloc of %d bytes failed", len(data))
	}

	if !memory.Write(buffer.Address(), data) {
		return 0, errors.New("write to memory out of range")
	}
	return buffer, nil
}

// Free releases a buffer with the free export of the plugin, unless its
// address is 0. It passes the buffer's length only when free takes one, as
// C's free(address) doesn't.
func Free(ctx context.Context, free api.Function, buffer wasm.Buffer) error {
	if buffer.Address() == 0 {
		return nil
	}

	params := []uint64{uint64(buffer.Address()), uint64(buffer.Length())}
	params = params[:len(free.Definition().ParamTypes())]
	if _, err := free.Call(ctx, params...); err != nil {
		return fmt.Errorf("failed to release plugin buffer: %w", err)
	}
	return nil
}

// Take copies a buffer returned by the plugin, and releases it.
func Take(ctx context.Context, memory api.Memory, free api.Function, buffer wasm.Buffer) ([]byte, error) {
	data, ok := memory.Read(buffer.Address(), buffer.Length())
	if !ok {
		return nil, fmt.Errorf("plugin returned an out of bounds buffer of %d bytes at %d", buffer.Length(), buffer.Address())
	}

	data = bytes.Clone(data)
	if err := Free(ctx, free, buffer); err != nil {
		return nil, err
	}
	return data, nil
}
