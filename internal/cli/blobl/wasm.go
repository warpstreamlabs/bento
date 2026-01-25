//go:build wasm

package blobl

import (
	"encoding/json"
	"syscall/js"

	"github.com/warpstreamlabs/bento/internal/bloblang"
)

// executeHandler executes a Bloblang mapping against input JSON.
func executeHandler(env *bloblang.Environment) js.Func {
	return js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) != 2 || args[0].Type() != js.TypeString || args[1].Type() != js.TypeString {
			return toJS(map[string]any{
				"mapping_error": "Invalid arguments: expected two strings (input, mapping)",
				"parse_error":   nil,
				"result":        nil,
			})
		}

		input, mapping := args[0].String(), args[1].String()
		response := executeBloblangMapping(env, input, mapping)

		return toJS(map[string]any{
			"mapping_error": response.MappingError,
			"parse_error":   response.ParseError,
			"result":        response.Result,
		})
	})
}

// validateHandler validates a Bloblang mapping without execution.
func validateHandler(env *bloblang.Environment) js.Func {
	return js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) != 1 || args[0].Type() != js.TypeString {
			return toJS(ValidationResponse{
				Valid: false,
				Error: "Invalid arguments: expected one string (mapping)",
			})
		}

		mapping := args[0].String()
		response := validateBloblangMapping(env, mapping)

		return toJS(response)
	})
}

// syntaxHandler exposes Bloblang syntax metadata for editor tooling.
func syntaxHandler(env *bloblang.Environment) js.Func {
	return js.FuncOf(func(_ js.Value, args []js.Value) any {
		response, err := generateBloblangSyntax(env)
		if err != nil {
			return toJS(map[string]any{
				"error": err.Error(),
			})
		}

		return toJS(response)
	})
}

// formatHandler formats a Bloblang mapping.
func formatHandler(env *bloblang.Environment) js.Func {
	return js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) != 1 || args[0].Type() != js.TypeString {
			return toJS(FormatMappingResponse{
				Error:     "Invalid arguments: expected one string (mapping)",
				Formatted: "",
				Success:   false,
			})
		}

		mapping := args[0].String()
		response := formatBloblangMapping(env, mapping)

		return toJS(response)
	})
}

// autocompleteHandler returns completion suggestions for Bloblang code.
func autocompleteHandler(env *bloblang.Environment) js.Func {
	return js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) != 1 || args[0].Type() != js.TypeString {
			return toJS(AutocompletionResponse{
				Error:       "Invalid arguments: expected one string (request JSON)",
				Completions: nil,
				Success:     false,
			})
		}

		requestJSON := args[0].String()

		var req AutocompletionRequest
		if err := json.Unmarshal([]byte(requestJSON), &req); err != nil {
			return toJS(AutocompletionResponse{
				Error:       "Failed to parse request JSON: " + err.Error(),
				Completions: nil,
				Success:     false,
			})
		}

		response := generateAutocompletion(env, req)

		return toJS(response)
	})
}

// toJS marshals Go data into a JS value via JSON.
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

// InitWASM registers the Bloblang WASM API on the global object.
func InitWASM(env *bloblang.Environment) {
	api := js.Global().Get("Object").New()

	api.Set("execute", executeHandler(env))
	api.Set("validate", validateHandler(env))
	api.Set("syntax", syntaxHandler(env))
	api.Set("format", formatHandler(env))
	api.Set("autocomplete", autocompleteHandler(env))

	js.Global().Set("bloblangApi", api)
	js.Global().Set("wasmReady", js.ValueOf(true))
}
