//go:build wasm

package blobl

import (
	"encoding/json"
	"sync"
	"syscall/js"

	"github.com/warpstreamlabs/bento/internal/bloblang"
)

// getWasmEnvironment returns a Bloblang environment suitable for browser/WASM context.
// Excludes functions that depend on system resources (env vars, filesystem).
var getWasmEnvironment = sync.OnceValue(func() *bloblang.Environment {
	return bloblang.GlobalEnvironment().WithoutFunctions("env", "file")
})

type WASMFunction struct {
	Name string
	Func js.Func
}

var registeredWasmFunctions = []WASMFunction{
	{"execute", exportExecute()},
	{"validate", exportValidate()},
	{"syntax", exportSyntax()},
	{"format", exportFormat()},
	{"autocomplete", exportAutocomplete()},
}

// exportExecute returns a js.Func that executes Bloblang mappings on input JSON.
//
// Arguments:
//   - args[0]: input JSON string
//   - args[1]: Bloblang mapping string
//
// Returns a JS object with:
//   - "result":        the mapping result (any type, or nil on error)
//   - "parse_error":   error message if input JSON could not be parsed, else nil
//   - "mapping_error": error message if mapping failed, else nil
func exportExecute() js.Func {
	return js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) != 2 || args[0].Type() != js.TypeString || args[1].Type() != js.TypeString {
			return toJS(map[string]any{
				"mapping_error": "Invalid arguments: expected two strings (input, mapping)",
				"parse_error":   nil,
				"result":        nil,
			})
		}

		env := getWasmEnvironment()
		input, mapping := args[0].String(), args[1].String()
		response := executeBloblangMapping(env, input, mapping)

		return toJS(map[string]any{
			"mapping_error": response.MappingError,
			"parse_error":   response.ParseError,
			"result":        response.Result,
		})
	})
}

// exportValidate returns a js.Func that validates Bloblang mappings without executing them.
//
// Note: Not currently used by the playground UI ('execute' already validates)
//
// Arguments:
//   - args[0]: Bloblang mapping string to validate
//
// Returns a JS object with:
//   - "valid": true if mapping is valid, false otherwise
//   - "error": error message if validation failed, else nil
func exportValidate() js.Func {
	return js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) != 1 || args[0].Type() != js.TypeString {
			return toJS(ValidationResponse{
				Valid: false,
				Error: "Invalid arguments: expected one string (mapping)",
			})
		}

		env := getWasmEnvironment()
		mapping := args[0].String()
		response := validateBloblangMapping(env, mapping)

		return toJS(response)
	})
}

// exportSyntax returns a js.Func that provides Bloblang syntax metadata for editor tooling.
//
// Returns a JS object with syntax info, or an "error" field if retrieval fails.
func exportSyntax() js.Func {
	return js.FuncOf(func(_ js.Value, args []js.Value) any {
		env := getWasmEnvironment()
		response, err := generateBloblangSyntax(env)

		if err != nil {
			return toJS(map[string]any{
				"error": err.Error(),
			})
		}
		return toJS(response)
	})
}

// exportFormat returns a js.Func that formats Bloblang mappings with proper indentation.
//
// Arguments:
//   - args[0]: Bloblang mapping string to format
//
// Returns a JS object with:
//   - "formatted": the formatted mapping string
//   - "success":   true if formatting succeeded, false otherwise
//   - "error":     error message if formatting failed, else nil
func exportFormat() js.Func {
	return js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) != 1 || args[0].Type() != js.TypeString {
			return toJS(FormatMappingResponse{
				Error:     "Invalid arguments: expected one string (mapping)",
				Formatted: "",
				Success:   false,
			})
		}

		env := getWasmEnvironment()
		mapping := args[0].String()
		response := formatBloblangMapping(env, mapping)

		return toJS(response)
	})
}

// exportAutocomplete returns a js.Func that provides autocompletion suggestions for Bloblang code.
//
// Arguments:
//   - args[0]: JSON string containing AutocompletionRequest with line, column, prefix, beforeCursor
//
// Returns a JS object with:
//   - "completions": array of completion items with caption, value, meta, type, score, docHTML
//   - "success":     true if autocompletion succeeded, false otherwise
//   - "error":       error message if autocompletion failed, else nil
func exportAutocomplete() js.Func {
	return js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) != 1 || args[0].Type() != js.TypeString {
			return toJS(AutocompletionResponse{
				Error:       "Invalid arguments: expected one string (request JSON)",
				Completions: []CompletionItem{},
				Success:     false,
			})
		}

		requestJSON := args[0].String()

		var req AutocompletionRequest
		if err := json.Unmarshal([]byte(requestJSON), &req); err != nil {
			return toJS(AutocompletionResponse{
				Error:       "Failed to parse request JSON: " + err.Error(),
				Completions: []CompletionItem{},
				Success:     false,
			})
		}

		env := getWasmEnvironment()
		response := generateAutocompletion(env, req)

		return toJS(response)
	})
}

// toJS marshals a Go value to a JS value for WASM compatibility.
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

// InitializeWASM sets up the Bloblang API in the browser's global scope.
func InitializeWASM() {
	bloblangAPI := js.Global().Get("Object").New()
	for _, fn := range registeredWasmFunctions {
		bloblangAPI.Set(fn.Name, fn.Func)
	}
	js.Global().Set("bloblangApi", bloblangAPI)
	js.Global().Set("wasmReady", js.ValueOf(true))
}
