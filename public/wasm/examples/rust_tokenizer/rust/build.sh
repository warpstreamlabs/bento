#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

# WASI is required so that the plugin can read tokenizer files via mounts.
cargo build --target wasm32-wasip1 --release
cp target/wasm32-wasip1/release/bento_tokenizer.wasm ../plugin/plugin.wasm
echo "Built plugin/plugin.wasm successfully"
