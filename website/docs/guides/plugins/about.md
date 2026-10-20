---
title: Plugins
sidebar_label: About
---

Extending Bento typically requires recompiling the binary and maintaining custom forks. Bento's WebAssembly (WASM) plugin system solves this by allowing you to define custom processor components as standalone modules that load at runtime -- no recompilation required!

A Bento plugin consists of two files within a dedicated directory:

```
my-plugin/
├── plugin.yaml  # Plugin manifest (required)
└── plugin.wasm  # Compiled WASM binary (required)
```

## Quick Links

- [Writing and Loading Plugins][getting-started] - Learn how to create and load plugins
- [Example: Reverse Processor][examples] - Complete walkthrough of building a plugin
- [Manifest Reference][fields] - Full configuration schema documentation

## Language Support

Bento provides plugin development kits (PDKs) for:

- **Go** ([`public/wasm/service`][go-pdk]), compiled with [TinyGo][tinygo] or Go.
- **Rust** ([`public/wasm/pdk/rust`][rust-pdk]), compiled for `wasm32-unknown-unknown`, or `wasm32-wasip1` when the plugin reads files.

Plugins can be written in any other language that compiles to a 32-bit WASM module, such as C, Zig or AssemblyScript, by implementing the [ABI](#abi) below.

## How Plugins Run

Plugins run within the Bento process using [wazero][wazero], a WebAssembly runtime written in Go. A plugin is compiled the first time a pipeline uses it.

Each instance of a plugin processes one call at a time, so Bento creates as many instances as there are calls running at once, such as one per pipeline thread. `init_plugin` is called once for each instance, and any state a plugin keeps in globals belongs to that instance. Successive calls may go to different instances.

Plugins have access to [WASI preview 1][wasi], with clocks and a secure random source. They can only access the host's filesystem through the directories mounted with `runtime.wasm.mounts`, though symbolic links within those directories are followed even when they point outside of them. The linear memory of each instance is limited by `runtime.wasm.memory.max_pages`.

Metadata is passed to a plugin as strings, and replaced with the metadata the plugin returns. Values that aren't strings, such as numbers, are therefore strings once they have passed through a plugin.

## ABI

The PDKs implement this for you. It is only needed to write a plugin in another language.

Messages are passed to a plugin as [protobuf][message-proto], and the plugin returns its results in the same format.

Data is exchanged through buffers in the plugin's memory. A buffer is passed as an `i64` holding its address in the upper 32 bits and its length in the lower 32 bits, where zero is an empty buffer. A plugin exports:

| Export          | Signature                   | Description                                                                                                               |
|-----------------|-----------------------------|---------------------------------------------------------------------------------------------------------------------------|
| `alloc`         | `(size i32) -> i32`         | Allocates `size` bytes for Bento to write into, and returns their address.                                                |
| `release`       | `(address i32, length i32)` | Frees a buffer allocated by `alloc` or returned to Bento.                                                                 |
| `init_plugin`   | `(config i64) -> i64`       | Optional. Called once per instance with the plugin's config as JSON. Returns an error message, which is empty on success. |
| `process_batch` | `(batch i64) -> i32`        | Called with every message of a batch as a `Batch`, and returns `Batches`.                                                 |
| `process`       | `(part i64) -> i32`         | Called once per message as a `Part`, and returns a `Batch`. Only used when `process_batch` is not exported.               |

`process` and `process_batch` return the address of a 16 byte result area, which holds the buffer of their output followed by the buffer of an error message, each as a little-endian `i64`. The error is empty on success. The result area must remain valid until the next call, so it is typically a static.

Bento writes its input into buffers allocated with `alloc`, which the plugin may read until the call returns. The plugin returns its output and errors in buffers it allocates. Once a call returns, Bento copies them and frees every buffer with `release`.

### Errors

A plugin can fail individual messages or a whole call:

- To fail a message, set `error` on the `Part` it returns. The message continues through the pipeline marked with the error, and can be handled like any other processing error.
- To fail a call, return an error message. Every message of the call continues unchanged, marked with that error.

A trap, such as a panic, also fails the call, and the instance is discarded. Any message that is not returned is dropped.

See [Error Handling][error-handling] for handling failed messages.

[getting-started]: /docs/guides/plugins/getting_started
[examples]: /docs/guides/plugins/examples
[fields]: /docs/guides/plugins/fields
[error-handling]: /docs/configuration/error_handling
[go-pdk]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/service
[rust-pdk]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/pdk/rust
[message-proto]: https://github.com/warpstreamlabs/bento/blob/main/resources/protos/message.proto
[tinygo]: https://tinygo.org/
[wazero]: https://wazero.io/
[wasi]: https://github.com/WebAssembly/WASI/blob/main/legacy/preview1/docs.md
