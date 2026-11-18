//go:build wasm

package components

import (
	"github.com/warpstreamlabs/bento/internal/plugin/wasm"
)

//go:wasmexport init_plugin
func init_plugin(address, length uint32) wasm.Buffer {
	return wasm.FromError(plugin.Init(wasm.NewBuffer(address, length).Slice()))
}
