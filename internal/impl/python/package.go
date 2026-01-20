// Calling 'go generate internal/impl/python/package.go' or 'make generate' will trigger this directive,
// downloading the python wasm runtime to the ./runtime directory.
//
// Note: the usage of the embed directive means that bento will not compile if the entrypoint.py and python-3.12.0.wasm files are not present.
package python

import (
	_ "embed"
)

//go:generate go run runtime/install.go -out ./runtime

var (
	//go:embed runtime/entrypoint.py
	pythonEntrypoint []byte

	//go:embed runtime/python-3.12.0.wasm
	pythonWASM []byte
)
