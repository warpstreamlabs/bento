//go:build wasm && !tinygo

// Note: tinygo already exports free and malloc, so don't include them in exports.

package components

import "github.com/warpstreamlabs/bento/internal/plugin/model"

// stores allocated memory on a heap to prevent garbage collection
var heap = make(map[model.Buffer][]byte)

//go:wasmexport free
func free(buffer model.Buffer) {
	delete(heap, buffer)
}

//go:wasmexport malloc
func malloc(size uint32) model.Buffer {
	memory := make([]byte, size)
	buffer := model.FromSlice(memory)
	heap[buffer] = memory
	return buffer
}

//go:wasmexport reset
func reset() {
	clear(heap)
}
