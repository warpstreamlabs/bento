package wasm

import (
	// Bring in the internal plugin definitions.
	_ "github.com/warpstreamlabs/bento/internal/impl/wasm"

	// Enable the WASM plugin runtime.
	_ "github.com/warpstreamlabs/bento/internal/plugin/runtime/extism_v1"
)
