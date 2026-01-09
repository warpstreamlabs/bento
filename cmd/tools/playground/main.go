//go:build wasm

package main

import (
	"github.com/warpstreamlabs/bento/internal/cli/blobl"
	_ "github.com/warpstreamlabs/bento/public/components/pure"
)

// main initializes and exposes Bloblang playground functions to JS via WASM
func main() {
	blobl.InitializeWASM()
	select {} // Keeps Go runtime alive to handle JS calls
}
