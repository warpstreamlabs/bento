package wasm

type Buffer uint64

func NewBuffer(address, length uint32) Buffer {
	return Buffer(uint64(address)<<32 | uint64(length))
}

func (buffer Buffer) Address() uint32 {
	return uint32(buffer >> 32)
}

func (buffer Buffer) Length() uint32 {
	return uint32(buffer)
}
