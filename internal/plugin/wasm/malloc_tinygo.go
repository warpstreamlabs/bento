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

// Alloc returns a buffer of size bytes from malloc, which the host frees.
func Alloc(size uint32) Buffer {
	if size == 0 {
		return 0
	}
	return NewBuffer(uint32(uintptr(libcMalloc(uintptr(size)))), size)
}
