package host

import (
	"context"
	"fmt"

	"github.com/tetratelabs/wazero/api"

	"github.com/warpstreamlabs/bento/internal/wasm"
)

// Malloc copies data into a buffer allocated with the alloc export of the
// plugin, which must be released with Free.
func Malloc(ctx context.Context, module api.Module, alloc api.Function, data []byte) (wasm.Buffer, error) {
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

	if !module.Memory().Write(buffer.Address(), data) {
		return 0, fmt.Errorf("write to memory out of range")
	}
	return buffer, nil
}

// Free releases a buffer with the release export of the plugin.
func Free(ctx context.Context, release api.Function, buffer wasm.Buffer) error {
	_, err := release.Call(ctx, uint64(buffer.Address()), uint64(buffer.Length()))
	return err
}
