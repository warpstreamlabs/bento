//go:build wasm

package blobl

import (
	"encoding/json"
	"syscall/js"

	"github.com/warpstreamlabs/bento/internal/bloblang"
)

// ExecuteBloblangMapping returns a js.Func that executes the Bloblang mapping functionality on input JSON.
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
func ExecuteBloblangMapping() js.Func {
	return js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) != 2 || args[0].Type() != js.TypeString || args[1].Type() != js.TypeString {
			return toJS(map[string]any{
				"mapping_error": "Invalid arguments: expected two strings (input, mapping)",
				"parse_error":   nil,
				"result":        nil,
			})
		}

		input, mapping := args[0].String(), args[1].String()
		result := evaluateMapping(bloblang.GlobalEnvironment(), input, mapping)

		return toJS(map[string]any{
			"mapping_error": result.MappingError,
			"parse_error":   result.ParseError,
			"result":        result.Result,
		})
	})
}

// GenerateBloblangSyntax returns a js.Func that provides Bloblang syntax metadata for editor tooling.
// This function is intended to be exposed to JavaScript via WebAssembly.
//
// Returns a JS object with syntax info, or an "error" field if retrieval fails.
func GenerateBloblangSyntax() js.Func {
	return js.FuncOf(func(_ js.Value, args []js.Value) any {
		syntax, err := generateBloblangSyntax(bloblang.GlobalEnvironment())
		if err != nil {
			return toJS(map[string]any{
				"error": err.Error(),
			})
		}
		return toJS(syntax)
	})
}

// toJS marshals a Go value to a JavaScript value for WASM compatibility.
// If marshaling fails, returns a JS string with the error message.
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
