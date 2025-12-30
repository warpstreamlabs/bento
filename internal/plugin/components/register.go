//go:build wasm

package components

import (
	"github.com/warpstreamlabs/bento/internal/plugin/model"
)

//go:wasmexport get_plugin_spec
func get_plugin_spec() model.Buffer {
	return plugin.Spec()
}

//go:wasmexport init_component
func init_component(conf uint64) model.String {
	if plugin.IsRegistered() {
		return 0
	}

	err := plugin.Init(model.Buffer(conf).String())
	if err != nil {
		return model.FromString(err.Error())
	}
	return 0
}
