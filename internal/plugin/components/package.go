//go:build !wasm

package components

import (
	"github.com/warpstreamlabs/bento/internal/plugin/model"
)

func malloc(size uint32) model.Buffer {
	panic("Cannot call 'malloc' outside of WASM builds")
}

func free(buffer model.Buffer) {
	panic("Cannot call 'free' outside of WASM builds")
}

func reset() {
	panic("Cannot call 'reset' outside of WASM builds")
}

func host_register_component(ct uint32, nameBuffer model.String, specBuffer model.Buffer) {
	panic("Cannot register component with host outside of WASM builds")
}
