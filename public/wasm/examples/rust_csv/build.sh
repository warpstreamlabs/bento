#!/bin/bash

cargo build --target wasm32-unknown-unknown --release
cp target/wasm32-unknown-unknown/release/rust_csv.wasm ./testdata/plugin.wasm
echo "Built rust_csv.wasm successfully"
