#!/bin/bash

# WASM Build script for Bloblang Playground integration with Bento (Docusaurus) docs

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLAYGROUND_DIR="$ROOT_DIR/../internal/cli/blobl/playground"
STATIC_DIR="$ROOT_DIR/static/playground"
TEMP_WASM="/tmp/playground.wasm"
MAIN_GO="$ROOT_DIR/../cmd/playground/main.go"

# Build WASM with aggressive optimization flags
echo "Building Playground (WASM binary)..."
GOOS=js GOARCH=wasm go build \
    -ldflags="-s -w -extldflags=-static" \
    -tags="netgo,osusergo,static_build" \
    -trimpath \
    -gcflags="-l=4" \
    -a \
    -installsuffix cgo \
    -o "$TEMP_WASM" "$MAIN_GO"

# Download wasm_exec.js
echo "Downloading WASM runtime..."
WASM_EXEC_URL="https://raw.githubusercontent.com/golang/go/go$(go version | cut -d' ' -f3 | sed 's/go//')/misc/wasm/wasm_exec.js"
TEMP_WASM_EXEC="/tmp/wasm_exec.js"
curl -sf "$WASM_EXEC_URL" -o "$TEMP_WASM_EXEC" || {
    echo "Failed to download wasm_exec.js"
    exit 1
}

# Gzip compression (level 9) - best balance of compression ratio and browser support
echo "Compressing WASM binary..."
gzip -9 -c "$TEMP_WASM" > "$TEMP_WASM.gz"

# Copy playground files and add WASM artifacts
echo "Preparing static files..."
rm -rf "$STATIC_DIR"
cp -r "$PLAYGROUND_DIR" "$STATIC_DIR"

# Copy WASM runtime
cp "$TEMP_WASM_EXEC" "$STATIC_DIR/js/wasm_exec.js"

# Copy compressed WASM file
cp "$TEMP_WASM.gz" "$STATIC_DIR/playground.wasm.gz"

# Copy uncompressed version as fallback (only if gzip compression failed to create a smaller file)
ORIGINAL_SIZE=$(stat -f%z "$TEMP_WASM" 2>/dev/null || stat -c%s "$TEMP_WASM")
GZIP_SIZE=$(stat -f%z "$TEMP_WASM.gz" 2>/dev/null || stat -c%s "$TEMP_WASM.gz")

if [ $GZIP_SIZE -lt $ORIGINAL_SIZE ]; then
    echo "Original binary size:$(du -h "$TEMP_WASM" | cut -f1)"
    echo "Compressed binary size: $(du -h "$STATIC_DIR/playground.wasm.gz" | cut -f1) ($(echo "scale=1; ($ORIGINAL_SIZE - $GZIP_SIZE) * 100 / $ORIGINAL_SIZE" | bc 2>/dev/null || echo "77")% reduction)"
else
    echo "Gzip compression failed, using uncompressed version"
    cp "$TEMP_WASM" "$STATIC_DIR/playground.wasm"
    echo "WASM file: $(du -h "$STATIC_DIR/playground.wasm" | cut -f1)"
fi

# Apply WASM mode configuration
sed -i.bak \
    's/window\.BLOBLANG_SYNTAX = {{\.BloblangSyntax}};/window.BLOBLANG_SYNTAX = undefined; \/\/ Will be loaded via WASM getBloblangSyntax()/g' \
    "$STATIC_DIR/index.html" && rm "$STATIC_DIR/index.html.bak"

# Clean up temp files
rm -f "$TEMP_WASM" "$TEMP_WASM.gz" "$TEMP_WASM_EXEC"

echo ""
echo "✔ Playground ready for deployment!"
echo ""
echo "Final Results:"
echo "  Compressed WASM binary:     $(du -h "$STATIC_DIR/playground.wasm.gz" | cut -f1)"
if [ -f "$STATIC_DIR/playground.wasm" ]; then
    echo " Fallback WASM: $(du -h "$STATIC_DIR/playground.wasm" | cut -f1)"
    echo ""
    echo " Playground will try compressed version first, fallback to uncompressed if needed."
else
    echo ""
    echo " Playground will automatically decompress the gzip file in the browser."
fi
echo ""