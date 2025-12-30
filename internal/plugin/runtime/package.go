//go:build wasm

package runtime

import (
	"github.com/warpstreamlabs/bento/internal/plugin"
)

func NewPluginRuntime() plugin.Runtime[any] {
	return nil
}
