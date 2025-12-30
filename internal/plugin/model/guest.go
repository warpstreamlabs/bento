package model

import (
	"unsafe"
)

type Ptr uint32

func PtrTo[T any](value *T) Ptr {
	return Ptr(uintptr(unsafe.Pointer(value)))
}

type String uint64

func FromString(value string) String {
	position := uint32(uintptr(unsafe.Pointer(unsafe.StringData(value))))
	bytes := uint32(len(value))
	return String(uint64(position)<<32 | uint64(bytes))
}

type Buffer uint64

func FromSlice(value []byte) Buffer {
	if len(value) == 0 {
		return 0
	}
	ptr := uint64(uintptr(unsafe.Pointer(&value[0])))
	return Buffer(ptr<<32 | uint64(len(value)))
}

func (buffer Buffer) Address() uint32 {
	return uint32(buffer >> 32)
}

func (buffer Buffer) Length() uint32 {
	return uint32(buffer)
}

func (buffer Buffer) Slice() []byte {
	return append([]byte{}, unsafe.Slice((*byte)(unsafe.Pointer(uintptr(buffer.Address()))), buffer.Length())...)
}

func (buffer Buffer) String() string {
	return string(buffer.Slice())
}
