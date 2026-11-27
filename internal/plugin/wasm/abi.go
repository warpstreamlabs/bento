//go:build wasm

package wasm

// bentoABIv1 declares that the plugin is built against version 1 of the ABI
// defined in resources/wasm/abi.wat. Bento checks for it before running a
// plugin.
//
//go:wasmexport bento_abi_v1
func bentoABIv1() {}
