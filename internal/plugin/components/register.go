//go:build wasm

package components

import (
	"github.com/warpstreamlabs/bento/internal/wasm"
)

//go:wasmexport init_plugin
func init_plugin(conf wasm.Buffer) wasm.Buffer {
	return wasm.FromError(plugin.Init(conf.Slice()))
}
