//go:build wasm

package wasm

import "unsafe"

func FromSlice(value []byte) Buffer {
	if len(value) == 0 {
		return 0
	}
	ptr := uint32(uintptr(unsafe.Pointer(&value[0])))
	return NewBuffer(ptr, uint32(len(value)))
}

// FromError returns a buffer holding the message of err, or an empty buffer
// when err is nil.
func FromError(err error) Buffer {
	if err == nil {
		return 0
	}
	return Retain([]byte(err.Error()))
}
