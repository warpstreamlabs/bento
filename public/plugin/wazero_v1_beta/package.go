// Package wazero_v1_beta should be imported to enable Bento's WASM-based plugin
// system. Once stable, it will be replaced by a wazero_v1 package.
//
// For more information, see https://warpstreamlabs.github.io/bento/docs/guides/plugins/about.
//
// Go plugins are written with the Plugin Development Kit (PDK) at
// [github.com/warpstreamlabs/bento/public/wasm/service], and examples in any
// language are in https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples.
//
// Experimental: This package is experimental and therefore subject to
// change outside of major version releases.
package wazero_v1_beta

import (
	// Enable the WASM plugin runtime.
	_ "github.com/warpstreamlabs/bento/internal/plugin/runtime/wazero_v1"
)
