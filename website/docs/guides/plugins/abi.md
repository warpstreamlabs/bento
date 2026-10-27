---
title: ABI
---

:::caution
Writing a plugin without a PDK is highly experimental. The ABI may change between Bento releases, and a plugin that implements it has to manage the memory it shares with Bento itself.
:::

The Go PDK implements the exports for you, which are only needed to write a plugin in another language that compiles to a 32-bit WASM module, such as Rust. See the [Rust examples][rust-examples] for plugins that implement them. How messages, errors and instances behave applies to every plugin.

Messages are passed to a plugin as [protobuf][message-proto], and the plugin returns its results in the same format.

Data is exchanged through buffers in the plugin's memory. A buffer is passed as an `i64` holding its address in the upper 32 bits and its length in the lower 32 bits, where zero is an empty buffer.

## Exports

A plugin exports its `memory` and the functions below. Bento fails to load a plugin that doesn't export `alloc`, `release`, and either `process_batch` or `process`:

| Export          | Signature                   | Description                                                                                                                                                                                                            |
|-----------------|-----------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `memory`        | memory                      | Required. The plugin's linear memory, which every buffer is in.                                                                                                                                                        |
| `alloc`         | `(size i32) -> i32`         | Required. Allocates `size` bytes for Bento to write into, and returns their address. Bento never asks for zero bytes, and fails the call if it returns zero.                                                           |
| `release`       | `(address i32, length i32)` | Required. Frees a buffer allocated by `alloc` or returned to Bento. Bento never releases an empty buffer.                                                                                                              |
| `process_batch` | `(batch i64) -> i32`        | Required unless `process` is exported. Called with every message of a batch as a `Batch`. Returns the address of its result, which holds its output as `Batches`, or an error.                                         |
| `process`       | `(part i64) -> i32`         | Required unless `process_batch` is exported. Called once per message as a `Part`. Returns the address of its result, which holds its output as a `Batch`, or an error. Only used when `process_batch` is not exported. |
| `init_plugin`   | `(config i64) -> i64`       | Optional. Called once per instance, before it processes any messages, with the processor's config as JSON, including the defaults of its fields. Returns an error message, which is empty on success.                  |
| `_initialize`   | `()`                        | Optional. Called when each instance is created, before `init_plugin`, as WASI reactors such as Go's `-buildmode=c-shared` modules require. `_start` is never called.                                                   |

The result of `process` and `process_batch` is a 16 byte area, which holds the buffer of their output followed by the buffer of an error message, each as a little-endian `i64`. The error is empty on success. Otherwise the call fails with that error, and its output is ignored. The result area must remain valid until the next call, so it is typically a static.

Bento writes its input into buffers allocated with `alloc`, which the plugin may read until the call returns. The plugin returns its output and errors in buffers it allocates. Once a call returns, Bento copies them and frees every buffer with `release`.

## Messages

A `Part` is a message, with:

- Its content, as `raw` bytes, or as a `structured` value when an earlier processor parsed it, such as with a mapping.
- Its `metadata`, where every value is a string. Values that aren't strings, such as numbers, are strings once they have passed through a plugin.
- Its `error`, which is empty unless it failed.

Messages that a plugin returns replace the messages it was called with. Return no messages to drop them, or more to split them.

## Errors

- To fail a message, set `error` on the `Part` it returns. The message continues through the pipeline marked with the error.
- To fail a call, return an error message in the result area. Every message of the call continues unchanged, marked with that error.
- An error from `init_plugin` discards the instance, and fails the call it was created for.

A trap, such as a panic, also fails the call, and the instance is discarded. See [Error Handling][errors] for handling failed messages.

## How Plugins Run

Plugins run within the Bento process using [wazero][wazero], a WebAssembly runtime written in Go. A plugin is compiled the first time a pipeline uses it.

Each instance of a plugin processes one call at a time, so Bento creates as many instances as there are calls running at once, such as one per pipeline thread. `init_plugin` is called once for each instance, and any state a plugin keeps in globals belongs to that instance. Successive calls may go to different instances.

Plugins have access to [WASI preview 1][wasi], with clocks and a secure random source. They can only access the host's filesystem through the directories mounted with `runtime.wasm.mounts`, though symbolic links within those directories are followed even when they point outside of them. The linear memory of each instance is limited by `runtime.wasm.memory.max_pages`.

[message-proto]: https://github.com/warpstreamlabs/bento/blob/main/resources/protos/message.proto
[errors]: /docs/configuration/error_handling
[rust-examples]: /docs/guides/plugins/developing_plugins#rust
[wazero]: https://wazero.io/
[wasi]: https://github.com/WebAssembly/WASI/blob/main/legacy/preview1/docs.md
