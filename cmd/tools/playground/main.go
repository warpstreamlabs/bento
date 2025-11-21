//go:build wasm

package main

import (
	"syscall/js"

	"github.com/warpstreamlabs/bento/internal/cli/blobl"
	_ "github.com/warpstreamlabs/bento/public/components/pure"
)

// main initializes and exposes Bloblang playground functions to JavaScript via WebAssembly
func main() {
	// Create namespaced object for JS accessibility
	bloblangAPI := js.Global().Get("Object").New()
	for _, fn := range blobl.RegisteredWasmFunctions {
		// Register each function to be accessible in JS via window.bloblangApi
		bloblangAPI.Set(fn.Name, fn.Func)
	}
	js.Global().Set("bloblangApi", bloblangAPI)

	// Indicate WASM module as ready for use
	js.Global().Set("wasmReady", js.ValueOf(true))

	// Keep the Go runtime alive to handle JS calls
	select {}
}
