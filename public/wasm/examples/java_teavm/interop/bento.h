// Buffers exchanged with Bento. See internal/plugin/runtime/wazero_v1/host.go
// for the full ABI.
#pragma once
#include <stdint.h>

// A range of plugin memory, packed as its address in the upper 32 bits and its
// length in the lower 32 bits. Zero is empty.
typedef uint64_t bento_buffer;

// The result area that process_batch returns the address of, which holds the
// buffers of its output and error. The error is empty on success.
typedef struct {
    bento_buffer output;
    bento_buffer error;
} bento_result;

// Copies data into a buffer allocated with malloc, which Bento releases once it
// has read it.
bento_buffer bento_retain(bento_buffer data);
