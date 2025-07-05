#!/bin/bash

# Build script for Bloblang WASM playground

set -e

echo "Building WASM binary..."
GOOS=js GOARCH=wasm go build -o ../internal/cli/blobl/playground/playground.wasm ../cmd/playground/main.go

echo "Copying wasm_exec.js..."
cp "$(go env GOROOT)/misc/wasm/wasm_exec.js" ../internal/cli/blobl/playground/js/

echo "Moving playground files to website static folder..."
rm -rf static/playground
cp -r ../internal/cli/blobl/playground static/

echo "WASM build complete!"
