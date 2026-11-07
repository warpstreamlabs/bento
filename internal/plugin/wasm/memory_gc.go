//go:build wasm && !tinygo

package wasm

// heap retains the buffers shared with the host until they are released, as an
// address alone does not keep an allocation alive.
var heap = make(map[uint32][]byte)

// Slice returns the data of a buffer allocated with malloc, which remains valid
// once the buffer is released.
func (buffer Buffer) Slice() []byte {
	if buffer == 0 {
		return nil
	}
	return heap[buffer.Address()][:buffer.Length()]
}

// Retain keeps value alive until the host releases it, and returns a buffer
// referencing it.
func Retain(value []byte) Buffer {
	buffer := FromSlice(value)
	if buffer != 0 {
		heap[buffer.Address()] = value
	}
	return buffer
}

// Retained returns the number of buffers that have not been released.
func Retained() int {
	return len(heap)
}

//go:wasmexport malloc
func malloc(size uint32) uint32 {
	if size == 0 {
		return 0
	}
	return Retain(make([]byte, size)).Address()
}

//go:wasmexport free
func free(address, _ uint32) {
	delete(heap, address)
}
