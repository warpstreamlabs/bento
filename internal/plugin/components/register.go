//go:build wasm

package components

import (
	"github.com/extism/go-pdk"
)

//go:wasmexport plugin_init
func plugin_init() int32 {
	err := plugin.Init(pdk.Input())
	if err != nil {
		pdk.SetError(err)
		return 1
	}

	return 0
}
