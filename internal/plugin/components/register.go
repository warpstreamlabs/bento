//go:build wasm

package components

import (
	"github.com/extism/go-pdk"
)

//go:wasmexport init_plugin
func init_plugin() int32 {
	err := plugin.Init(pdk.Input())
	if err != nil {
		pdk.SetError(err)
		return 1
	}

	return 0
}
