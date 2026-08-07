//go:build wasm

package main

import (
	"github.com/warpstreamlabs/bento/internal/bloblang"
	"github.com/warpstreamlabs/bento/internal/cli/blobl/wasm"
	_ "github.com/warpstreamlabs/bento/public/components/pure"
)

// main initializes and exposes Bloblang playground functions to JS via WASM.
func main() {
	env := bloblang.GlobalEnvironment().WithoutFunctions("env", "file")
	wasm.Init(env)
	select {} // Keep Go runtime alive to handle JS calls
}
