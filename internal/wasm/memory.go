//go:build wasm

package wasm

// Retain keeps value alive until the host releases it, and returns a buffer
// referencing it.
func Retain(value []byte) Buffer {
	buffer := FromSlice(value)
	if buffer != 0 {
		heap[buffer.Address()] = value
	}
	return buffer
}

// Retained returns the number of buffers that have not been released.
func Retained() int {
	return len(heap)
}

// FromError returns a buffer holding the message of err, or an empty buffer
// when err is nil.
func FromError(err error) Buffer {
	if err == nil {
		return 0
	}
	return Retain([]byte(err.Error()))
}

// Set sets the result of a call to its output, or to err if it failed. Both
// fields are set, as a result is reused between calls.
func (r *Result) Set(output []byte, err error) {
	if err != nil {
		*r = Result{Error: FromError(err)}
		return
	}
	*r = Result{Output: Retain(output)}
}

//go:wasmexport alloc
func alloc(size uint32) uint32 {
	if size == 0 {
		return 0
	}
	return Retain(make([]byte, size)).Address()
}

//go:wasmexport release
func release(address, _ uint32) {
	delete(heap, address)
}
