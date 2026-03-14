//go:build wasm

// Package wasm provides WebAssembly bindings for the Bloblang playground.
package wasm

import (
	"encoding/json"
	"fmt"
	"syscall/js"

	"github.com/warpstreamlabs/bento/internal/bloblang"
	"github.com/warpstreamlabs/bento/internal/cli/blobl"
)

type wasmHandler func(args []js.Value) (any, error)

func toJSFunc(h wasmHandler) js.Func {
	return js.FuncOf(func(_ js.Value, args []js.Value) any {
		res, err := h(args)
		if err != nil {
			panic(js.Global().Get("Error").New(err.Error()))
		}
		return toJS(res)
	})
}

func executeHandler(env *bloblang.Environment) js.Func {
	return toJSFunc(func(args []js.Value) (any, error) {
		if len(args) != 2 || args[0].Type() != js.TypeString || args[1].Type() != js.TypeString {
			return map[string]any{
				"mapping_error": "Invalid arguments: expected two strings (input, mapping)",
				"parse_error":   nil,
				"result":        nil,
			}, nil
		}

		input, mapping := args[0].String(), args[1].String()
		response := blobl.ExecuteBloblangMapping(env, input, mapping)

		return map[string]any{
			"mapping_error": response.MappingError,
			"parse_error":   response.ParseError,
			"result":        response.Result,
		}, nil
	})
}

func syntaxHandler(syntax *blobl.BloblangSyntax) js.Func {
	return toJSFunc(func(args []js.Value) (any, error) {
		return syntax, nil
	})
}

func formatHandler(env *bloblang.Environment) js.Func {
	return toJSFunc(func(args []js.Value) (any, error) {
		if len(args) != 1 || args[0].Type() != js.TypeString {
			return nil, fmt.Errorf("invalid arguments: expected one string (mapping)")
		}

		return blobl.FormatBloblangMapping(env, args[0].String())
	})
}

func autocompleteHandler(syntax *blobl.BloblangSyntax) js.Func {
	return toJSFunc(func(args []js.Value) (any, error) {
		if len(args) != 1 || args[0].Type() != js.TypeString {
			return nil, fmt.Errorf("invalid arguments: expected request JSON")
		}

		var req blobl.AutocompletionRequest
		if err := json.Unmarshal([]byte(args[0].String()), &req); err != nil {
			return nil, fmt.Errorf("failed to parse request JSON: %w", err)
		}

		return blobl.GenerateAutocompletion(syntax, req)
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

// Init registers the Bloblang WASM API on the global object.
func Init(env *bloblang.Environment) {
	syntax, err := blobl.GenerateBloblangSyntax(env)
	if err != nil {
		panic(js.Global().Get("Error").New(err.Error()))
	}

	api := js.Global().Get("Object").New()

	api.Set("execute", executeHandler(env))
	api.Set("syntax", syntaxHandler(&syntax))
	api.Set("format", formatHandler(env))
	api.Set("autocomplete", autocompleteHandler(&syntax))

	js.Global().Set("bloblangApi", api)
	js.Global().Set("wasmReady", toJS(true))
}
