//go:build js && wasm

// WASM entry point for executing Bloblang mappings from JavaScript.
// Exposes:
//   1. executeBloblang(input: string, mapping: string): object
//      - Runs a Bloblang mapping on a JSON input string.
//   2. getBloblangSyntax(): object
//      - Returns Bloblang syntax info for editor tooling.
//
// Signals readiness with `wasmReady` and exposes build metadata via `wasmMetadata`.
// Keeps running to serve JS calls.

package main

import (
	"encoding/json"
	"syscall/js"
	"time"

	"github.com/warpstreamlabs/bento/internal/cli/blobl"
)

var env *blobl.BloblangEnvironment

// executeBloblang is exposed to JavaScript.
// Args: input JSON and Bloblang mapping as strings.
// Returns an object with "result", "parse_error", and "mapping_error".
func executeBloblang(_ js.Value, args []js.Value) any {
	if len(args) != 2 || args[0].Type() != js.TypeString || args[1].Type() != js.TypeString {
		return toJS(map[string]any{
			"mapping_error": "Invalid arguments: expected two strings (input, mapping)",
			"parse_error":   nil,
			"result":        nil,
		})
	}

	input, mapping := args[0].String(), args[1].String()
	result, err := env.ExecuteMapping(input, mapping)
	if err != nil {
		return toJS(map[string]any{
			"mapping_error": err.Error(),
			"parse_error":   nil,
			"result":        nil,
		})
	}

	return toJS(map[string]any{
		"mapping_error": result.MappingError,
		"parse_error":   result.ParseError,
		"result":        result.Result,
	})
}

// getBloblangSyntax is exposed to JavaScript.
// Returns Bloblang syntax info or an error.
func getBloblangSyntax(_ js.Value, args []js.Value) any {
	syntax, err := env.GetSyntax()
	if err != nil {
		return toJS(map[string]any{
			"error": err.Error(),
		})
	}

	return toJS(syntax)
}

// toJS converts Go data structures to JavaScript values for WASM compatibility.
func toJS(data any) js.Value {
	if data == nil {
		return js.Null()
	}

	b, err := json.Marshal(data)
	if err != nil {
		return js.ValueOf("error: " + err.Error())
	}

	return js.Global().Get("JSON").Call("parse", string(b))
}

// main registers WASM functions, exposes metadata, and signals readiness.
func main() {
	env = blobl.NewBloblangEnvironment()

	// Expose JavaScript globals
	js.Global().Set("executeBloblang", js.FuncOf(executeBloblang))
	js.Global().Set("getBloblangSyntax", js.FuncOf(getBloblangSyntax))
	js.Global().Set("wasmReady", js.ValueOf(true))
	js.Global().Set("wasmMetadata", map[string]any{
		"built": time.Now().UTC().Format(time.RFC3339),
	})

	// Keep the Go runtime alive to handle JS calls
	select {}
}
