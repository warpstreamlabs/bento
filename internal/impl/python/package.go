//go:build x_wasm

package python

import (
	_ "embed"
)

var (
	//go:embed runtime/python-3.12.0.wasm
	_pythonWASM []byte

	//go:embed scripts/entrypoint.py
	_pythonEntrypoint []byte
)

func init() {
	pythonWASM = _pythonWASM
	pythonEntrypoint = _pythonEntrypoint
	isWASM = true
}
