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
			return ToJS(map[string]any{
				"error":     "Invalid arguments: expected one string (mapping)",
				"formatted": "",
				"success":   false,
			})
		}

		mapping := args[0].String()
		response := FormatBloblangMapping(mapping)
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
