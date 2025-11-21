//go:build wasm

package blobl

import (
	"encoding/json"
	"syscall/js"

	"github.com/warpstreamlabs/bento/internal/bloblang"
)

type WASMFunction struct {
	Name string
	Func js.Func
}

// RegisteredWasmFunctions defines all Bloblang functions exposed to JavaScript via WASM.
// This list is used by playground/main.go to initialize window.bloblangApi.
// Add new exports here to make them available to the playground.
var RegisteredWasmFunctions = []WASMFunction{
	// Core execution
	// Executes a Bloblang mapping on a JSON input string and returns the result or error
	{"execute", ExportExecute()},
	// Validates Bloblang mappings
	{"validate", ExportValidate()},

	// Editor tooling
	// Returns Bloblang syntax metadata for editor tooling
	{"syntax", ExportSyntax()},
	// Formats Bloblang mappings
	{"format", ExportFormat()},
	// Provides Bloblang autocompletions
	{"autocomplete", ExportAutocomplete()},

	// JSON utilities
	// Formats JSON strings with indentation
	{"formatJSON", ExportFormatJSON()},
	// Minifies JSON strings by removing whitespace
	{"minifyJSON", ExportMinifyJSON()},
	// Validates JSON strings
	{"validateJSON", ExportValidateJSON()},
}

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

// ExportValidate returns a js.Func that validates Bloblang mappings without executing them.
// This function is intended to be exposed to JavaScript via WebAssembly.
//
// Note: Not currently used by the playground UI ('execute' already validates), but exposed
// for future integrations / consumers
//
// Arguments:
//   - args[0]: Bloblang mapping string to validate
//
// Returns a JS object with:
//   - "valid": true if mapping is valid, false otherwise
//   - "error": error message if validation failed, else nil
func ExportValidate() js.Func {
	return js.FuncOf(func(this js.Value, args []js.Value) any {
		if len(args) != 1 || args[0].Type() != js.TypeString {
			return ToJS(ValidationResponse{
				Valid: false,
				Error: "Invalid arguments: expected one string (mapping)",
			})
		}

		mapping := args[0].String()
		response := ValidateBloblangMapping(bloblang.GlobalEnvironment(), mapping)
		return ToJS(response)
	})
}

// ExportFormatJSON returns a js.Func that formats JSON strings with indentation.
// This function is intended to be exposed to JavaScript via WebAssembly.
//
// Arguments:
//   - args[0]: JSON string to format
//
// Returns a JS object with:
//   - "success": true if formatting succeeded, false otherwise
//   - "result": the formatted JSON string
//   - "error": error message if formatting failed, else nil
func ExportFormatJSON() js.Func {
	return js.FuncOf(func(this js.Value, args []js.Value) any {
		if len(args) != 1 || args[0].Type() != js.TypeString {
			return ToJS(JSONResponse{
				Success: false,
				Result:  "",
				Error:   "Invalid arguments: expected one string (JSON)",
			})
		}

		jsonString := args[0].String()
		response := FormatJSON(jsonString)
		return ToJS(response)
	})
}

// ExportMinifyJSON returns a js.Func that minifies JSON strings by removing whitespace.
// This function is intended to be exposed to JavaScript via WebAssembly.
//
// Arguments:
//   - args[0]: JSON string to minify
//
// Returns a JS object with:
//   - "success": true if minification succeeded, false otherwise
//   - "result": the minified JSON string
//   - "error": error message if minification failed, else nil
func ExportMinifyJSON() js.Func {
	return js.FuncOf(func(this js.Value, args []js.Value) any {
		if len(args) != 1 || args[0].Type() != js.TypeString {
			return ToJS(JSONResponse{
				Success: false,
				Result:  "",
				Error:   "Invalid arguments: expected one string (JSON)",
			})
		}

		jsonString := args[0].String()
		response := MinifyJSON(jsonString)
		return ToJS(response)
	})
}

// ExportValidateJSON returns a js.Func that validates JSON strings.
// This function is intended to be exposed to JavaScript via WebAssembly.
//
// Arguments:
//   - args[0]: JSON string to validate
//
// Returns a JS object with:
//   - "valid": true if JSON is valid, false otherwise
//   - "error": error message if validation failed, else nil
func ExportValidateJSON() js.Func {
	return js.FuncOf(func(this js.Value, args []js.Value) any {
		if len(args) != 1 || args[0].Type() != js.TypeString {
			return ToJS(ValidationResponse{
				Valid: false,
				Error: "Invalid arguments: expected one string (JSON)",
			})
		}

		jsonString := args[0].String()
		response := ValidateJSON(jsonString)
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
