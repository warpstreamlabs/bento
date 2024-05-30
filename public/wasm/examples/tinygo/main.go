//go:build tinygo

package main

import (
	"bytes"

	"github.com/warpstreamlabs/bento/v4/public/wasm/tinygo"
)

// main is required for TinyGo to compile to Wasm.
func main() {}

// _process is a WebAssembly export without arguments that triggers processing
// of a Bento message. The message data is accessed and mutated by functions
// imported from Bento and are accessible via the ./public/wasm packages (in
// this case tinygo).
//
//export process
func _process() {
	mBytes, err := tinygo.GetMsgAsBytes()
	if err != nil {
		panic(err)
	}
	if err := tinygo.SetMsgBytes(bytes.ToUpper(mBytes)); err != nil {
		panic(err)
	}
}
