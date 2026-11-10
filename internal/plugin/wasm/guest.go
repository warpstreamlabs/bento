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

func FromError(err error) Buffer {
	if err == nil {
		return 0
	}
	message := err.Error()
	if message == "" {
		message = "unknown error"
	}

	output := Alloc(uint32(len(message)))
	copy(output.Slice(), message)
	return output
}
