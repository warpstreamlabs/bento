#!/bin/bash

cargo build --target wasm32-unknown-unknown --release
cp target/wasm32-unknown-unknown/release/rust_uppercase.wasm ./testdata/plugin.wasm
echo "Built rust_uppercase.wasm successfully"
