package wasm

import (
	"structs"
	"unsafe"
)

type Buffer uint64

type Result struct {
	_      structs.HostLayout
	Output Buffer
	Error  Buffer
}

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

// heap retains the buffers shared with the host until they are released, as an
// address alone does not keep an allocation alive.
var heap = make(map[uint32][]byte)

// Slice returns the data of a buffer allocated with alloc, which remains valid
// once the buffer is released.
func (buffer Buffer) Slice() []byte {
	if buffer == 0 {
		return nil
	}
	return heap[buffer.Address()][:buffer.Length()]
}
