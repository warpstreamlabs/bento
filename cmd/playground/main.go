//go:build js && wasm

// This file provides a WebAssembly (WASM) entry point for executing Bloblang mappings from JavaScript.
// It exposes a single function, `executeBloblang`, to the JavaScript environment, allowing users to:
//   1. Parse a Bloblang mapping string.
//   2. Apply the mapping to a JSON input string.
//   3. Return the result or any errors encountered during parsing or execution.
//
// The main exported function, `executeBloblang`, expects two string arguments from JavaScript:
//   - The JSON input string.
//   - The Bloblang mapping string.
//
// The function returns a JavaScript object containing:
//   - "result":        The resulting JSON string if successful.
//   - "input_error":   Error message if the mapping could not be parsed.
//   - "mapping_error": Error message if the mapping failed during execution.
//
// The WASM module signals readiness by setting `wasmReady` to true in the global JavaScript context.
// The Go program remains running indefinitely to serve JavaScript calls.

package main

import (
	"encoding/json"
	"fmt"
	"syscall/js"
	"time"

	"github.com/warpstreamlabs/bento/internal/bloblang/parser"
	"github.com/warpstreamlabs/bento/public/bloblang"
)

type wasmResponse struct {
	Result       any `json:"result"`
	MappingError any `json:"mapping_error"`
	ParseError   any `json:"parse_error"`
}

// Function exposed to JavaScript
func executeBloblang(this js.Value, args []js.Value) any {
	res := wasmResponse{
		Result:       nil,
		MappingError: nil,
		ParseError:   nil,
	}

	if len(args) != 2 {
		res.MappingError = "Invalid arguments: expected exactly two string parameters (input, mapping)"
		return toJS(res)
	}

	input := args[0].String()
	if input == "" {
		res.MappingError = "Input JSON string cannot be empty"
		return toJS(res)
	}

	mapping := args[1].String()
	if mapping == "" {
		res.ParseError = "Mapping string cannot be empty"
		return toJS(res)
	}

	// Parse Bloblang mapping
	exec, err := bloblang.Parse(mapping)
	if err != nil {
		if perr, ok := err.(*parser.Error); ok {
			res.ParseError = fmt.Sprintf("failed to parse mapping: %v", perr.ErrorAtPositionStructured("", []rune(mapping)))
		} else {
			res.ParseError = fmt.Sprintf("mapping error: %v", err.Error())
		}
		return toJS(res)
	}

	// Apply mapping
	output, err := executeMapping(exec, input)
	if err != nil {
		res.MappingError = fmt.Sprintf("execution error: %v", err.Error())
		return toJS(res)
	}

	res.Result = output
	return toJS(res)
}

// Execute Bloblang mapping against a JSON input string and returns the JSON-encoded result
func executeMapping(exec *bloblang.Executor, input string) (string, error) {
	var inputData any
	if err := json.Unmarshal([]byte(input), &inputData); err != nil {
		return "", fmt.Errorf("failed to parse input as JSON: %w", err)
	}

	result, err := exec.Query(inputData)
	if err != nil {
		return "", err
	}

	outputBytes, err := json.Marshal(result)
	if err != nil {
		return "", fmt.Errorf("failed to marshal result: %w", err)
	}

	return string(outputBytes), nil
}

func toJS(res wasmResponse) map[string]any {
	return map[string]any{
		"mapping_error": res.MappingError,
		"parse_error":   res.ParseError,
		"result":        res.Result,
	}
}

// Register Bloblang execution function to the JavaScript global scope,
// signal readiness, expose build metadata, and keep the
// Go runtime alive to handle JavaScript calls.
//
// Exposed JavaScript globals:
//   - executeBloblang(mapping: string, input: string): object
//   - wasmReady: boolean
//   - wasmMetadata: { built: string }
func main() {
	// Register the Bloblang execution function for JavaScript
	js.Global().Set("executeBloblang", js.FuncOf(executeBloblang))

	// Signal to JavaScript that the WASM module is ready
	js.Global().Set("wasmReady", js.ValueOf(true))

	// Expose build metadata
	js.Global().Set("wasmMetadata", map[string]any{
		"built": time.Now().UTC().Format(time.RFC3339),
	})

	// Prevent the Go program from exiting, so it can continue serving JS calls.
	select {}
}
