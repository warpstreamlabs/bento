package huggingface

import (
	_ "embed"

	"github.com/warpstreamlabs/bento/public/service"

	// The Wasm plugin runtime must be initialised before the tokenizer plugin
	// is registered below.
	_ "github.com/warpstreamlabs/bento/internal/plugin/runtime/extism_v1"
)

// The tokenizer is a Wasm plugin wrapping https://github.com/huggingface/tokenizers,
// built from ./tokenizer/rust using ./tokenizer/rust/build.sh.

//go:embed tokenizer/plugin/plugin.yaml
var tokenizerManifest string

//go:embed tokenizer/plugin/plugin.wasm
var tokenizerWasm []byte

func init() {
	if err := service.RegisterWasmPlugin(tokenizerManifest, tokenizerWasm); err != nil {
		panic(err)
	}
}
