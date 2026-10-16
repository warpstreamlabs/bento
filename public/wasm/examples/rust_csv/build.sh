#!/bin/bash

cargo build --target wasm32-unknown-unknown --release
cp target/wasm32-unknown-unknown/release/rust_parse_csv.wasm ./testdata/plugin.wasm
echo "Built rust_processor.wasm successfully"