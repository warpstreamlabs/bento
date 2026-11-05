package wasm

import "unsafe"

type Buffer uint64

func NewBuffer(address, length uint32) Buffer {
	return Buffer(uint64(address)<<32 | uint64(length))
}

func FromSlice(value []byte) Buffer {
	if len(value) == 0 {
		return 0
	}
	ptr := uint32(uintptr(unsafe.Pointer(&value[0])))
	return NewBuffer(ptr, uint32(len(value)))
}

func (buffer Buffer) Address() uint32 {
	return uint32(buffer >> 32)
}

func (buffer Buffer) Length() uint32 {
	return uint32(buffer)
}
