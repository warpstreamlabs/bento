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

// Alloc returns a buffer of size bytes that remain alive until the host
// releases them.
func Alloc(size uint32) Buffer {
	if size == 0 {
		return 0
	}

	area := make([]byte, size)
	buffer := FromSlice(area)
	heap[buffer.Address()] = area
	return buffer
}

//go:wasmexport malloc
func malloc(size uint32) uint32 {
	return Alloc(size).Address()
}

//go:wasmexport free
func free(address, _ uint32) {
	delete(heap, address)
}
