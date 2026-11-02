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
	ptr := libcMalloc(uintptr(len(value)))
	copy(unsafe.Slice((*byte)(ptr), len(value)), value)
	return NewBuffer(uint32(uintptr(ptr)), uint32(len(value)))
}

// FromError returns a buffer holding the message of err, or an empty buffer
// when err is nil.
func FromError(err error) Buffer {
	if err == nil {
		return 0
	}
	return Retain([]byte(err.Error()))
}
