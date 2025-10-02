//go:build wasm

package blobl

import (
	"encoding/json"
	"syscall/js"

	"github.com/warpstreamlabs/bento/internal/bloblang"
)

// ExportExecute returns a js.Func that executes the Bloblang mapping functionality on input JSON.
// This function is intended to be exposed to JavaScript via WebAssembly.
//
// Arguments:
//   - args[0]: input JSON string
//   - args[1]: Bloblang mapping string
//
// Returns a JS object with:
//   - "result":        the mapping result (any type, or nil on error)
//   - "parse_error":   error message if input JSON could not be parsed, else nil
//   - "mapping_error": error message if mapping failed, else nil
func ExportExecute() js.Func {
	return js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) != 2 || args[0].Type() != js.TypeString || args[1].Type() != js.TypeString {
			return ToJS(map[string]any{
				"mapping_error": "Invalid arguments: expected two strings (input, mapping)",
				"parse_error":   nil,
				"result":        nil,
			})
		}

		input, mapping := args[0].String(), args[1].String()
		result := evaluateMapping(bloblang.GlobalEnvironment(), input, mapping)

		return ToJS(map[string]any{
			"mapping_error": result.MappingError,
			"parse_error":   result.ParseError,
			"result":        result.Result,
		})
	})
}

// ExportSyntax returns a js.Func that provides Bloblang syntax metadata for editor tooling.
// This function is intended to be exposed to JavaScript via WebAssembly.
//
// Returns a JS object with syntax info, or an "error" field if retrieval fails.
func ExportSyntax() js.Func {
	return js.FuncOf(func(_ js.Value, args []js.Value) any {
		syntax, err := generateBloblangSyntax(bloblang.GlobalEnvironment())
		if err != nil {
			return ToJS(map[string]any{
				"error": err.Error(),
			})
		}
		return ToJS(syntax)
	})
}

// ExportFormat returns a js.Func that provides formatted Bloblang mappings.
// This function is intended to be exposed to JavaScript via WebAssembly.
//
// Arguments:
//   - args[0]: Bloblang mapping string to format
//
// Returns a JS object with:
//   - "formatted": the formatted mapping string
//   - "success":   true if formatting succeeded, false otherwise
//   - "error":     error message if formatting failed, else nil
func ExportFormat() js.Func {
	return js.FuncOf(func(this js.Value, args []js.Value) any {
		if len(args) != 1 || args[0].Type() != js.TypeString {
			return ToJS(FormatMappingResponse{
				Error:     "Invalid arguments: expected one string (mapping)",
				Formatted: "",
				Success:   false,
			})
		}

		mapping := args[0].String()
		response := FormatBloblangMapping(mapping)
		return ToJS(response)
	})
}

// ExportAutocomplete returns a js.Func that provides autocompletion suggestions for Bloblang code.
// This function is intended to be exposed to JavaScript via WebAssembly.
//
// Arguments:
//   - args[0]: JSON string containing AutocompletionRequest with line, column, prefix, beforeCursor
//
// Returns a JS object with:
//   - "completions": array of completion items with caption, value, meta, type, score, docHTML
//   - "success":     true if autocompletion succeeded, false otherwise
//   - "error":       error message if autocompletion failed, else nil
func ExportAutocomplete() js.Func {
	return js.FuncOf(func(this js.Value, args []js.Value) any {
		if len(args) != 1 || args[0].Type() != js.TypeString {
			return ToJS(AutocompletionResponse{
				Error:       "Invalid arguments: expected one string (request JSON)",
				Completions: []CompletionItem{},
				Success:     false,
			})
		}

		requestJSON := args[0].String()

		// Parse the request JSON
		var req AutocompletionRequest
		if err := json.Unmarshal([]byte(requestJSON), &req); err != nil {
			return ToJS(AutocompletionResponse{
				Error:       "Failed to parse request JSON: " + err.Error(),
				Completions: []CompletionItem{},
				Success:     false,
			})
		}

		// Generate autocompletion using the Bloblang environment
		response := GenerateAutocompletion(bloblang.GlobalEnvironment(), req)
		return ToJS(response)
	})
}

// ToJS marshals a Go value to a JavaScript value for WASM compatibility.
// If marshaling fails, returns a JS string with the error message.
func ToJS(data any) js.Value {
	if data == nil {
		return js.Null()
	}

	b, err := json.Marshal(data)
	if err != nil {
		return js.ValueOf("error: " + err.Error())
	}

	return js.Global().Get("JSON").Call("parse", string(b))
}
