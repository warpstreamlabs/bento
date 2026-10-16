//go:build wasm

// Package wasm provides WebAssembly bindings for the Bloblang playground.
package wasm

import (
	"encoding/json"
	"fmt"
	"syscall/js"

	"github.com/warpstreamlabs/bento/internal/bloblang"
	"github.com/warpstreamlabs/bento/internal/cli/blobl/editor"
)

type wasmHandler func(args []js.Value) (any, error)

func toJSFunc(h wasmHandler) js.Func {
	return js.FuncOf(func(_ js.Value, args []js.Value) any {
		res, err := h(args)
		if err != nil {
			return toJS(map[string]any{"error": err.Error()})
		}
		return toJS(res)
	})
}

func executeHandler(ed *editor.Editor) js.Func {
	return toJSFunc(func(args []js.Value) (any, error) {
		if len(args) != 2 || args[0].Type() != js.TypeString || args[1].Type() != js.TypeString {
			msg := "Invalid arguments: expected two strings (input, mapping)"
			return &editor.ExecutionResult{MappingError: &msg}, nil
		}

		return ed.Execute(args[0].String(), args[1].String()), nil
	})
}

func syntaxHandler(syntax *editor.Syntax) js.Func {
	return toJSFunc(func(args []js.Value) (any, error) {
		return syntax, nil
	})
}

func formatHandler(ed *editor.Editor) js.Func {
	return toJSFunc(func(args []js.Value) (any, error) {
		if len(args) != 1 || args[0].Type() != js.TypeString {
			return nil, fmt.Errorf("invalid arguments: expected one string (mapping)")
		}

		return ed.Format(args[0].String())
	})
}

func autocompleteHandler(ed *editor.Editor) js.Func {
	return toJSFunc(func(args []js.Value) (any, error) {
		if len(args) != 1 || args[0].Type() != js.TypeString {
			return nil, fmt.Errorf("invalid arguments: expected request JSON")
		}

		var req editor.AutocompletionRequest
		if err := json.Unmarshal([]byte(args[0].String()), &req); err != nil {
			return nil, fmt.Errorf("failed to parse request JSON: %w", err)
		}

		return ed.Autocomplete(req), nil
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
	ed := editor.New(env)

	api := js.Global().Get("Object").New()

	api.Set("execute", executeHandler(ed))
	api.Set("syntax", syntaxHandler(ed.Syntax()))
	api.Set("format", formatHandler(ed))
	api.Set("autocomplete", autocompleteHandler(ed))

	js.Global().Set("bloblangApi", api)
	js.Global().Set("wasmReady", toJS(true))
}
