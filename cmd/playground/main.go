//go:build js && wasm

// This file provides a WebAssembly (WASM) entry point for executing Bloblang mappings from JavaScript.
// It exposes two main functions to the JavaScript environment:
//   1. executeBloblang(input: string, mapping: string): object
//      - Parses and executes a Bloblang mapping on a JSON input string.
//      - Returns an object with "result", "parse_error", and "mapping_error" fields.
//   2. getBloblangSyntax(): object
//      - Returns Bloblang syntax information for editor tooling.
//
// The WASM module signals readiness by setting `wasmReady` to true in the global JavaScript context.
// Build metadata is exposed via `wasmMetadata`.
// The Go program runs indefinitely to serve JavaScript calls.

package main

import (
	"encoding/json"
	"syscall/js"
	"time"

	"github.com/warpstreamlabs/bento/internal/cli/blobl"
)

var env *blobl.BloblangEnvironment

// executeBloblang is exposed to JavaScript.
// It expects two string arguments: input JSON and Bloblang mapping.
// Returns an object with "result", "parse_error", and "mapping_error".
func executeBloblang(this js.Value, args []js.Value) any {
	if len(args) != 2 {
		return toJS(map[string]any{
			"mapping_error": "Invalid arguments: expected exactly two string parameters (input, mapping)",
			"parse_error":   nil,
			"result":        nil,
		})
	}

	if args[0].Type() != js.TypeString || args[1].Type() != js.TypeString {
		return toJS(map[string]any{
			"mapping_error": "Arguments must be strings (input, mapping)",
			"parse_error":   nil,
			"result":        nil,
		})
	}

	input := args[0].String()
	mapping := args[1].String()

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
// Returns Bloblang syntax information as an object, or an error.
func getBloblangSyntax(this js.Value, args []js.Value) any {
	syntax, err := env.GetSyntax()
	if err != nil {
		return toJS(map[string]any{
			"error": err.Error(),
		})
	}

	// Convert to JSON and back to ensure compatibility with JS
	jsonBytes, err := json.Marshal(syntax)
	if err != nil {
		return toJS(map[string]any{
			"error": "Failed to serialize syntax data: " + err.Error(),
		})
	}
	var result map[string]any
	if err := json.Unmarshal(jsonBytes, &result); err != nil {
		return toJS(map[string]any{
			"error": "Failed to deserialize syntax data: " + err.Error(),
		})
	}

	return toJS(result)
}

// toJS converts Go data structures to JavaScript values for WASM compatibility.
func toJS(data any) any {
	if data == nil {
		return js.Null()
	}

	switch v := data.(type) {
	case string, int, int32, int64, float32, float64, bool:
		// For simple types, use ValueOf directly
		return js.ValueOf(v)
	case map[string]any:
		// Convert map to JS object
		obj := js.Global().Get("Object").New()
		for key, value := range v {
			obj.Set(key, toJS(value))
		}
		return obj
	case []any:
		// Convert slice to JS array
		arr := js.Global().Get("Array").New(len(v))
		for i, value := range v {
			arr.SetIndex(i, toJS(value))
		}
		return arr
	default:
		// For complex types, serialize through JSON
		jsonBytes, err := json.Marshal(data)
		if err != nil {
			return js.ValueOf("error: " + err.Error())
		}
		// Parse as JavaScript object
		return js.Global().Get("JSON").Call("parse", string(jsonBytes))
	}
}

// main registers the WASM functions, exposes build metadata, and signals readiness to JavaScript.
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
