#!/bin/bash
set -e

WASM_RUNTIME_DIR="$(dirname "${BASH_SOURCE[0]}")/.."
WASM_BINARY_URL="https://github.com/vmware-labs/webassembly-language-runtimes/releases/latest/download/python-3.12.0.wasm"
WASM_PATH="$WASM_RUNTIME_DIR/runtime/python-3.12.0.wasm"

wget -q "$WASM_BINARY_URL" -O "$WASM_PATH"

if command -v sha256sum >/dev/null; then
    (cd "$WASM_RUNTIME_DIR/runtime" && sha256sum --check --status python-3.12.0.wasm.sha256sum)
elif command -v shasum >/dev/null; then
    (cd "$WASM_RUNTIME_DIR/runtime" && shasum -a 256 --check python-3.12.0.wasm.sha256sum)
else
    echo "No checksum tool found."
    exit 1
fi