package python

import (
	"embed"
)

// Calling 'go generate internal/impl/python/package.go' or 'make generate' will trigger this directive,
// downloading the python wasm runtime to the ./runtime directory.
//
// Note: the usage of the embed directive means that bento will not compile if the ./runtime directory is not present.
// We do not embed the entrypoint.py and python-3.12.0.wasm files directly since this would prevent compilation on a local
// dev environment if the .wasm does not exist or has not yet been downloaded.

//go:generate go run scripts/install.go -out ./artifacts

var (
	//go:embed artifacts
	runtimeFs embed.FS

	pythonEntrypoint []byte
	pythonWASM       []byte
)

func init() {
	pythonEntrypoint, _ = runtimeFs.ReadFile("artifacts/entrypoint.py")
	pythonWASM, _ = runtimeFs.ReadFile("artifacts/python-3.12.0.wasm")
}
