package python

import (
	"bytes"
	_ "embed"
	"fmt"
	"io"
	"sync"

	"github.com/andybalholm/brotli"
)

// Calling 'go generate internal/impl/python/package.go' or 'make generate' will trigger this directive,
// downloading the python wasm runtime to the ./runtime directory.
//
// The artifacts required by this package (entrypoint.py and the compressed WASM runtime)
// must be present at build time for the //go:embed directives to succeed.

//go:generate go run runtime/install.go -out ./runtime

var (
	//go:embed runtime/entrypoint.py
	pythonEntrypoint []byte

	//go:embed runtime/python-3.12.0.wasm.br
	pythonBrotliWASM []byte
)

// getPythonWasm is an idempotent function that decompresses and returns the
// brotili encoded runtime/python-3.12.0.wasm.br into it's underlying .wasm form.
var getPythonWasm = sync.OnceValues(func() ([]byte, error) {
	br := brotli.NewReader(bytes.NewReader(pythonBrotliWASM))
	contents, err := io.ReadAll(br)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress embedded python wasm: %w", err)
	}
	return contents, nil
})
