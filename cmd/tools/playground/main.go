//go:build wasm

package main

import (
	"syscall/js"

	"github.com/warpstreamlabs/bento/internal/cli/blobl"
	_ "github.com/warpstreamlabs/bento/public/components/pure"
)

// main initializes and exposes Bloblang playground functions to JavaScript via WebAssembly
func main() {
	// Executes a Bloblang mapping on a JSON input string and returns the result or error
	js.Global().Set("executeBloblangMapping", blobl.ExecuteBloblangMapping())
	// Returns Bloblang syntax metadata for editor tooling
	js.Global().Set("generateBloblangSyntax", blobl.GenerateBloblangSyntax())
	// Indicates when the WASM module is ready for use
	js.Global().Set("wasmReady", js.ValueOf(true))

	// Keep the Go runtime alive to handle JS calls
	select {}
}
