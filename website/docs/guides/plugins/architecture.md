---
title: Architecture
---

import CodeBlock from '@theme/CodeBlock';
import messageProto from '../../../../resources/protos/message.proto';

Plugins run within the Bento process using [wazero][wazero], a WebAssembly runtime written in Go. This page describes their ABI and how messages, errors and instances behave.

## ABI

:::caution
Writing a plugin without a PDK is highly experimental. The ABI may change between Bento releases, and a plugin that implements it has to manage the buffers it exchanges with Bento.
:::

The Go PDK implements the exports for you. To write a plugin in another language that compiles to a 32-bit WASM module, implement them directly. The [`rust_uppercase`][rust-uppercase-example] and [`rust_csv`][rust-csv-example] examples show how.

Messages are passed to a plugin as [protobuf][message-proto], and the plugin returns its results in the same format. Data is exchanged through buffers in the plugin's linear memory.

### Exports

A plugin exports its `memory` and the functions below. Bento requires `alloc`, `release`, and either `process_batch` or `process`.

The signatures use [WebAssembly text format (WAT)][wat]. They are a reference for checking exports, not a compilable module.

```wasm
;; Required: linear memory and buffer allocation.
(memory (export "memory") 0)
(func (export "alloc") (param $size i32) (result i32))
(func (export "release") (param $address i32) (param $length i32))

;; Required: at least one processing function.
;; Input: packed buffer. Return value: result area address.
(func (export "process_batch") (param $batch i64) (result i32))
(func (export "process") (param $part i64) (result i32))

;; Optional initialization functions.
(func (export "_initialize"))
(func (export "init_plugin") (param $config i64) (result i64))
```

- `alloc` allocates `size` bytes for Bento to write into, and returns their address. Bento never asks for zero bytes, and fails the call if it returns zero.
- `release` frees a buffer allocated by `alloc` or returned to Bento. Bento supplies the buffer's length and never releases an empty buffer.
- `process_batch` receives every message of a batch as a `Batch`, and returns the address of a [result area](#results) containing its output as `Batches`, or an error.
- `process` receives one message as a `Part`, and returns the address of a result area containing its output as a `Batch`, or an error. It is only used when `process_batch` is not exported.
- `init_plugin` is called once per instance, before it processes any messages, with the processor's config as JSON, including field defaults. It returns a packed buffer containing an error message, or zero on success.
- `_initialize` is called when each instance is created, before `init_plugin`, as WASI reactors such as Go's `-buildmode=c-shared` modules require. Bento never calls `_start`.

### Buffers

A buffer is passed as an `i64` holding its address in the upper 32 bits and its length in the lower 32 bits. Zero represents an empty buffer.

```text
 63                            32 31                             0
+--------------------------------+--------------------------------+
|         address (u32)          |          length (u32)          |
+--------------------------------+--------------------------------+

buffer = (address << 32) | length
```

### Results

The result of `process` and `process_batch` is the address of a 16-byte area containing two packed buffers, each stored as a little-endian `i64`:

```text
 byte 0                         7 8                             15
+--------------------------------+--------------------------------+
|      output buffer (i64)       |       error buffer (i64)       |
+--------------------------------+--------------------------------+
```

The error buffer is empty on success. Otherwise, the call fails with that error message, and its output is ignored. The result area must remain valid until the next call, so it is typically static. Bento releases the output and error buffers, but not the result area itself.

### Memory Ownership

Bento calls `alloc` and writes its encoded input into the returned buffer. The plugin may read that buffer until the call returns. The plugin returns its output and errors in buffers it allocates. Once the call returns, Bento copies the returned buffers and calls `release` to free them and the input buffer.

## Messages

The protobuf types are defined in [`message.proto`][message-proto]:

<CodeBlock language="protobuf" title="resources/protos/message.proto">{messageProto}</CodeBlock>

A `Part` is a message, with:

- Its content, as `raw` bytes, or as a `structured` value when an earlier processor parsed it, such as with a [`mapping`][mapping].
- Its `metadata`, where every value is a string. Values that aren't strings, such as numbers, are strings once they have passed through a plugin.
- Its `error`, which is empty unless it failed.

Messages that a plugin returns replace the messages it was called with. Return no messages to drop them, or more to split them.

## Errors

- To fail a message, set `error` on the `Part` it returns. The message continues through the pipeline marked with the error.
- To fail a call, return an error message in the result area. Every message of the call continues unchanged, marked with that error.
- An error from `init_plugin` discards the instance, and fails the call it was created for.

A trap, such as a panic, also fails the call, and the instance is discarded. See [Error Handling][errors] for handling failed messages.

## How Plugins Run

A plugin is compiled the first time a pipeline uses it.

Each instance of a plugin processes one call at a time, so Bento creates as many instances as there are calls running at once, such as one per [pipeline thread][pipeline-threads]. If exported, `init_plugin` is called once for each instance, and any state a plugin keeps in globals belongs to that instance. Successive calls may go to different instances.

### The Sandbox

Plugins have access to [WASI preview 1][wasi], with clocks and a secure random source. They can only access the host's filesystem through the directories mounted with `runtime.wasm.mounts`, though symbolic links within those directories are followed even when they point outside of them. The linear memory of each instance is limited by `runtime.wasm.memory.max_pages`.

[message-proto]: https://github.com/warpstreamlabs/bento/blob/main/resources/protos/message.proto
[errors]: /docs/configuration/error_handling
[mapping]: /docs/components/processors/mapping
[pipeline-threads]: /docs/configuration/processing_pipelines
[rust-uppercase-example]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples/rust_uppercase
[rust-csv-example]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples/rust_csv
[wazero]: https://wazero.io/
[wasi]: https://github.com/WebAssembly/WASI/blob/main/legacy/preview1/docs.md
[wat]: https://webassembly.github.io/spec/core/text/index.html
