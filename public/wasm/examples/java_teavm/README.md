Java Bento WASM Plugin
======================

A `java_uppercase` processor written in Java. [TeaVM][teavm] compiles the Java to C, and [wasi-sdk][wasi-sdk] compiles the C to a WASI module.

TeaVM's own WASM targets emit WasmGC or depend on a JavaScript host, and neither runs in Bento's plugin runtime. Its C target compiles to plain WASM instead.

Bento passes each batch to the plugin as protobuf (`resources/protos/message.proto`), and the plugin returns the processed batch in the same format.

## Layout

- `src/main/java/uppercase/Main.java`: the processor, which uppercases the raw content of each message and copies its other fields.
- `src/main/java/uppercase/Proto.java`: reads and writes the length delimited protobuf fields of a `Batch` and a `Part`.
- `src/main/java/uppercase/Bento.java`: copies the input batch out of, and the output into, buffers exchanged with Bento.
- `interop/bento.h`: declares `bento_retain`, which `Bento.java` uses to return its output.
- `interop/plugin.c`: boots the TeaVM runtime and exports `alloc`, `release` and `process_batch`, which calls into Java. Buffers are allocated with `malloc`, outside of the Java heap.
- `interop/compat/`: stand-ins for `setjmp.h` and `pwd.h`, which WASI doesn't provide.

## Building

Requires Maven, a JDK (11+) and wasi-sdk. Set `WASI_SDK_PATH` if wasi-sdk isn't installed in `/opt/wasi-sdk`:

```sh
./build.sh
go test .
```

This writes `testdata/plugin.wasm`, which can be loaded with `bento -p ./testdata`.

## Limitations

- Java exceptions abort the call. TeaVM's C runtime throws with `longjmp`, which needs WASM exception handling that Bento's runtime doesn't support, so `interop/compat/setjmp.h` turns a throw into a trap. The plugin call fails, every message in the batch is marked with the error, and the instance is discarded. Code paths that throw, even if they catch, must be avoided.
- The Java heap is capped at 16MB (`maxHeapSize` in `pom.xml`), and is reserved when the plugin starts.

[teavm]: https://teavm.org/
[wasi-sdk]: https://github.com/WebAssembly/wasi-sdk
