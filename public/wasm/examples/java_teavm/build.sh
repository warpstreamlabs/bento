#!/bin/bash

# Compiles the Java plugin to C with TeaVM, then to a WASI reactor with wasi-sdk.
#
# Requires Maven, a JDK (11+) and wasi-sdk (https://github.com/WebAssembly/wasi-sdk).
# Set WASI_SDK_PATH if wasi-sdk isn't installed in /opt/wasi-sdk.

set -euo pipefail

cd "$(dirname "$0")"

WASI_SDK_PATH="${WASI_SDK_PATH:-/opt/wasi-sdk}"

# Java to C.
mvn -q -B package

# C to WASM.
#   -mexec-model=reactor   keep the instance alive between calls instead of exiting after main
#   -Dmain=teavm_main      TeaVM's main boots the runtime, and is called from interop/plugin.c
#   -D__EMSCRIPTEN__       select TeaVM's non-POSIX code paths (malloc'd heap, no signal timers)
#   -I interop             bento.h, which exchanges buffers with Bento
#   -I interop/compat      stubs for setjmp.h and pwd.h, which WASI lacks
"$WASI_SDK_PATH/bin/clang" --target=wasm32-wasip1 -mexec-model=reactor -O2 -w \
  -Dmain=teavm_main \
  -D__EMSCRIPTEN__ \
  -D_WASI_EMULATED_MMAN -D_WASI_EMULATED_SIGNAL -lwasi-emulated-mman \
  -I interop -I interop/compat \
  target/c/all.c interop/plugin.c \
  -o testdata/plugin.wasm

echo "Built testdata/plugin.wasm successfully"
