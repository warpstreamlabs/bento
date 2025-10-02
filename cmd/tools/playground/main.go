//go:build wasm

package main

import (
	"syscall/js"

	"github.com/warpstreamlabs/bento/internal/cli/blobl"
	_ "github.com/warpstreamlabs/bento/public/components/pure"
)

type WASMFunction struct {
	Name string
	Func js.Func
}

// main initializes and exposes Bloblang playground functions to JavaScript via WebAssembly
func main() {
	// Register WASM functions
	wasmFunctions := []WASMFunction{
		// Executes a Bloblang mapping on a JSON input string and returns the result or error
		{"execute", blobl.ExportExecute()},
		// Returns Bloblang syntax metadata for editor tooling
		{"syntax", blobl.ExportSyntax()},
		// Formats Bloblang mappings
		{"format", blobl.ExportFormat()},
	}

	// Create namespaced object for JS accessibility
	bloblangAPI := js.Global().Get("Object").New()
	for _, fn := range wasmFunctions {
		// Register each function to be accessible in JS via window.bloblangApi
		bloblangAPI.Set(fn.Name, fn.Func)
	}
	js.Global().Set("bloblangApi", bloblangAPI)

	// Indicate WASM module as ready for use
	js.Global().Set("wasmReady", js.ValueOf(true))

	// Keep the Go runtime alive to handle JS calls
	select {}
}
