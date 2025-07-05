#!/bin/bash

# WASM Build script for Bloblang Playground integration with Bento (Docusaurus) docs

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLAYGROUND_DIR="$ROOT_DIR/../internal/cli/blobl/playground"
STATIC_DIR="$ROOT_DIR/static/playground"
WASM_OUT="$PLAYGROUND_DIR/playground.wasm"
MAIN_GO="$ROOT_DIR/../cmd/playground/main.go"
WASM_EXEC_SRC="$(go env GOROOT)/misc/wasm/wasm_exec.js"
WASM_EXEC_DST="$PLAYGROUND_DIR/js/"

BUILD_WASM=true
SYNC_STATIC=true

# Parse command line arguments
for arg in "$@"; do
    case $arg in
        --build-only)
            BUILD_WASM=true
            SYNC_STATIC=false
            shift
            ;;
        --sync-only)
            BUILD_WASM=false
            SYNC_STATIC=true
            shift
            ;;
        *)
            ;;
    esac
done

if [ "$BUILD_WASM" = true ]; then
    echo "Building Playground WASM binary..."
    GOOS=js GOARCH=wasm go build -ldflags="-s -w" -o "$WASM_OUT" "$MAIN_GO"
    cp "$WASM_EXEC_SRC" "$WASM_EXEC_DST"
fi

if [ "$SYNC_STATIC" = true ]; then
    echo "Syncing with Playground files in website folder..."
    rm -rf "$STATIC_DIR"
    mkdir -p "$(dirname "$STATIC_DIR")"
    cp -r "$PLAYGROUND_DIR" "$STATIC_DIR"

    INDEX_HTML="$STATIC_DIR/index.html"
    if [[ -f "$INDEX_HTML" ]]; then
        sed -i.bak \
            's/window\.BLOBLANG_SYNTAX = {{\.BloblangSyntax}};/window.BLOBLANG_SYNTAX = undefined; \/\/ Will be loaded via WASM getBloblangSyntax()/g' \
            "$INDEX_HTML"
        rm "$INDEX_HTML.bak"
    else
        echo "Warning: $INDEX_HTML not found, skipping patch."
    fi
fi

echo "Playground build complete!"