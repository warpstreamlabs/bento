package host

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/tetratelabs/wazero/api"

	"github.com/warpstreamlabs/bento/internal/wasm"
)

// Malloc copies data into a buffer allocated with the alloc export of the
// plugin, which must be released with Free.
func Malloc(ctx context.Context, memory api.Memory, alloc api.Function, data []byte) (wasm.Buffer, error) {
	if len(data) == 0 {
		return 0, nil
	}

	results, err := alloc.Call(ctx, uint64(len(data)))
	if err != nil {
		return 0, err
	}

	buffer := wasm.NewBuffer(api.DecodeU32(results[0]), uint32(len(data)))
	if buffer.Address() == 0 {
		return 0, fmt.Errorf("alloc of %d bytes failed", len(data))
	}

	if !memory.Write(buffer.Address(), data) {
		return 0, fmt.Errorf("write to memory out of range")
	}
	return buffer, nil
}

// Free releases a buffer with the release export of the plugin, unless it is
// empty.
func Free(ctx context.Context, release api.Function, buffer wasm.Buffer) error {
	if buffer == 0 {
		return nil
	}

	if _, err := release.Call(ctx, uint64(buffer.Address()), uint64(buffer.Length())); err != nil {
		return fmt.Errorf("failed to release plugin buffer: %w", err)
	}
	return nil
}

// Take copies a buffer returned by the plugin, and releases it.
func Take(ctx context.Context, memory api.Memory, release api.Function, buffer wasm.Buffer) ([]byte, error) {
	data, ok := memory.Read(buffer.Address(), buffer.Length())
	if !ok {
		return nil, fmt.Errorf("plugin returned an out of bounds buffer of %d bytes at %d", buffer.Length(), buffer.Address())
	}

	data = bytes.Clone(data)
	if err := Free(ctx, release, buffer); err != nil {
		return nil, err
	}
	return data, nil
}

// resultSize of a given [wasm.Result]. While this should always be 16-bytes (two uint64 attributes), let's rather
// define this at runtime incase we decide to edit this field.
var resultSize = uint32(binary.Size(wasm.Result{}))

// TakeResult reads the [wasm.Result] area at a given address address.
func TakeResult(memory api.Memory, address uint32) (wasm.Result, error) {
	data, ok := memory.Read(address, resultSize)
	if !ok {
		return wasm.Result{}, fmt.Errorf("plugin returned an out of bounds result at %d", address)
	}

	var result wasm.Result
	if _, err := binary.Decode(data, binary.LittleEndian, &result); err != nil {
		return wasm.Result{}, fmt.Errorf("decode plugin result: %w", err)
	}
	return result, nil
}
