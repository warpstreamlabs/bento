//go:build wasm && tinygo

package wasm

// TinyGo's runtime already exports malloc and free, as free(address), so the
// host allocates and frees with those, and buffers returned to the host are
// copied into memory from the same malloc.

import "unsafe"

// libcMalloc is the malloc that TinyGo's runtime exports.
//
//go:linkname libcMalloc malloc
func libcMalloc(size uintptr) unsafe.Pointer

// Slice returns the data of a buffer allocated with malloc, which is valid
// until the host frees it.
func (buffer Buffer) Slice() []byte {
	if buffer == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(uintptr(buffer.Address()))), buffer.Length())
}

// Retain copies value into memory from malloc, which the host frees, and
// returns a buffer referencing it.
func Retain(value []byte) Buffer {
	if len(value) == 0 {
		return 0
	}
	output := unsafe.Slice((*byte)(libcMalloc(uintptr(len(value)))), len(value))
	copy(output, value)
	return FromSlice(output)
}
