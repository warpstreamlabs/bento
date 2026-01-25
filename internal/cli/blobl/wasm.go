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
			panic(js.Global().Get("Error").New(
				"invalid arguments: expected one string (mapping)",
			))
		}

		mapping := args[0].String()

		valid, err := validateBloblangMapping(env, mapping)
		if err != nil {
			panic(js.Global().Get("Error").New(err.Error()))
		}

		return toJS(valid)
	})
}

// syntaxHandler exposes Bloblang syntax metadata for editor tooling.
func syntaxHandler(env *bloblang.Environment) js.Func {
	return js.FuncOf(func(_ js.Value, _ []js.Value) any {
		syntax, err := generateBloblangSyntax(env)
		if err != nil {
			panic(js.Global().Get("Error").New(err.Error()))
		}

		return toJS(syntax)
	})
}

// formatHandler formats a Bloblang mapping.
func formatHandler(env *bloblang.Environment) js.Func {
	return js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) != 1 || args[0].Type() != js.TypeString {
			panic(js.Global().Get("Error").New(
				"invalid arguments: expected one string (mapping)",
			))
		}

		mapping := args[0].String()

		formatted, err := formatBloblangMapping(env, mapping)
		if err != nil {
			panic(js.Global().Get("Error").New(err.Error()))
		}

		return toJS(formatted)
	})
}

// autocompleteHandler returns completion suggestions for Bloblang code.
func autocompleteHandler(env *bloblang.Environment) js.Func {
	return js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) != 1 || args[0].Type() != js.TypeString {
			panic(js.Global().Get("Error").New(
				"invalid arguments: expected one string (request JSON)",
			))
		}

		var req AutocompletionRequest
		if err := json.Unmarshal([]byte(args[0].String()), &req); err != nil {
			panic(js.Global().Get("Error").New(
				"failed to parse request JSON: " + err.Error(),
			))
		}

		completions, err := generateAutocompletion(env, req)
		if err != nil {
			panic(js.Global().Get("Error").New(err.Error()))
		}

		return toJS(completions)
	})
}

// toJS marshals Go data into a JS value via JSON.
func toJS(data any) js.Value {
	if data == nil {
		return js.Null()
	}

	b, err := json.Marshal(data)
	if err != nil {
		return toJS("error: " + err.Error())
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
	js.Global().Set("wasmReady", toJS(true))
}
