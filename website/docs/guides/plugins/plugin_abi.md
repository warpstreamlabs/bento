---
title: Plugin ABI
---

import CodeBlock from '@theme/CodeBlock';
import abi from '../../../../resources/wasm/abi.wat';

export const WatExport = ({name}) => {
  const lines = abi.split('\n').map((line) => line.trim());
  const end = lines.findIndex((line) => line.includes(`(export "${name}")`));
  let start = end;
  while (start > 0 && lines[start - 1].startsWith(';;')) {
    start--;
  }
  return <CodeBlock language="wasm">{lines.slice(start, end + 1).join('\n')}</CodeBlock>;
};

This page is the application binary interface (ABI) reference for processor plugins that implement Bento's WebAssembly (WASM) interface directly. It defines the runtime, memory ownership, message formats, exports, and error behavior for a 32-bit WASM module.

The [Go plugin development kit (PDK)][developing] implements this interface for you. Complete implementations without a PDK are available in the [Rust examples][examples]. See [About plugins][about] for an overview and [Manifest fields][fields] for plugin configuration.

:::caution Experimental
The ABI may change between Bento releases. Plugins that implement it directly must manage the memory exchanged with Bento and match the ABI of the Bento version that loads them.
:::

## Runtime

Bento runs plugins in process using [wazero][wazero]. It compiles a plugin's module when a pipeline first uses it and creates instances as processing calls need them.

Each instance processes one call at a time. Concurrent calls can use separate instances, and successive calls may use different instances. Global variables belong to one instance; they are not shared state across the processor. Idle instances may be discarded, so their state is not persistent storage.

The plugin runs only while Bento calls it. Work for a call must finish before its export returns. Go goroutines do not run in the background between calls. The ABI has no teardown export; Bento closes instances directly.

### Sandbox

Plugins have access to WASI preview 1, with clocks and a secure random source. Filesystem access is limited to directories mounted with [`runtime.wasm.mounts`][mounts], although symbolic links within those directories can lead outside them. Mounts are read-only by default.

[`runtime.wasm.memory.max_pages`][memory-limit] limits each instance's linear memory in 64 KiB pages. Without a configured limit, each instance can allocate up to 4 GiB.

## Memory

Plugins exchange data with Bento through their exported linear [`memory`](#memory-export).

### Buffers

A buffer is an unsigned 32-bit address and an unsigned 32-bit byte length. Bento passes an input buffer to an export as two `i32` parameters, its address and then its length. The export returns its output buffer as an `i64` that packs the address in its upper bits and the length in its lower bits. `0` is an empty output.

```text
 63                            32 31                             0
+--------------------------------+--------------------------------+
|         address (u32)          |          length (u32)          |
+--------------------------------+--------------------------------+

output  = (u64(address) << 32) | u64(length)
address = u32(output >> 32)
length  = u32(output)
```

### Ownership

Bento allocates the input with the plugin's [`malloc`](#malloc), calls the export, reads the output from plugin memory, and frees both buffers with [`free`](#free).

- **Keep input and output allocations separate.** Bento skips `malloc` for an empty input and passes `0` as its address and length. Read the input only during the call; do not free it or retain a pointer to it afterward. Return output in a separate allocation that remains valid until Bento frees it. Returning the input buffer, a slice of it, or stack memory can cause a double free or an invalid free.
- **Return `0` for an empty output.** Bento never calls `free` with address `0`, but calls it on any other address, even if its length is `0`. Do not return a nonzero dangling pointer, such as an empty Rust `Vec`'s pointer.

## Wire Format

Processing inputs, outputs, and errors use protobuf, as defined in [`message.proto`][message-proto] and [`processor.proto`][processor-proto]. A message is called a `Part`. Initialization uses JSON config and UTF-8 error messages, as described under [`init_plugin`](#init_plugin).

```protobuf
message Part {
    oneof content {
        bytes raw = 1;
        Value structured = 2; // Bento's Value, not google.protobuf.Value
    }
    Error error = 3; // set when the message failed
    map<string, string> metadata = 4; // values cross the WASM boundary as strings
}

message Batch {
    repeated Part parts = 1;
}

message Batches {
    repeated Batch batches = 1;
}

message Error {
    string message = 1;
}
```

Each processing export takes a `Request` and returns a `Response`:

```protobuf
message Process {
    message Request {
        Part part = 1;
    }
    message Response {
        Batch batch = 1; // replaces the message
        Error error = 2; // fails the message
    }
}

message ProcessBatch {
    message Request {
        Batch batch = 1;
    }
    message Response {
        Batches batches = 1; // replaces the batch
        Error error = 2; // fails every message of the batch
    }
}
```

The returned messages replace the input. An empty result drops the input; returning `0` decodes as an empty response and has the same effect. When `error` is set, the returned messages are ignored. To pass a message through, include it in the response with its content, metadata, and any error to preserve. Returning an encoded `Part` or `Batch` alone does not satisfy the response format.

## Exports

The signatures below are rendered directly from [`abi.wat`][abi-wat], the ABI reference in WebAssembly text format (WAT). The file describes export signatures; it is not a compilable module.

A plugin must export `memory`, `malloc`, `free`, and at least one of `process_batch` or `process`. If both processing exports exist, Bento uses `process_batch`. Bento checks the signatures of all ABI functions present, including optional functions and an unused `process` export.

### `memory` {#memory-export}

<WatExport name="memory" />

All buffer addresses refer to this linear memory. Each buffer's address and length must describe a range within it.

### `malloc`

<WatExport name="malloc" />

If `malloc` returns `0` or an address whose requested byte range extends outside memory, the call fails. See [Errors](#errors).

### `free`

<WatExport name="free" />

### `_initialize`

<WatExport name="_initialize" />

Bento calls this export, if present, before `init_plugin`. It never calls `_start`.

### `init_plugin`

<WatExport name="init_plugin" />

Bento calls this export once per instance, before processing messages. The config contains the processor's configured fields, including defaults from the manifest. If there is no config, its address and length are `0`.

A nonempty result is a UTF-8 error message, not a protobuf `Error`.

### `process_batch`

<WatExport name="process_batch" />

Bento calls this export once per input batch.

### `process`

<WatExport name="process" />

Bento calls this export once per input message when `process_batch` is absent.

## Errors

A call failure preserves the original input messages and marks them with the error. For `process`, this affects one message; for `process_batch`, it affects the whole batch.

A returned `Part.error` marks that output message as failed. A processing response's `error` fails the whole call, and the instance remains available for reuse. Output that cannot be decoded as the expected protobuf response also fails the call, and Bento discards the instance.

If `init_plugin` returns an error, Bento discards the instance and fails the call that requested it.

A trap is a WebAssembly execution failure, such as an out-of-bounds memory access. If an export traps, `malloc` returns `0`, or a buffer is outside plugin memory, Bento fails the call and discards the instance. It creates a replacement when a later call needs one; it does not retry the failed call automatically.

If a required function is missing or an ABI function has an invalid signature, Bento rejects the module when the plugin is first used. No instance is created.

See [Error handling][errors] for handling failed messages in a pipeline.

[message-proto]: https://github.com/warpstreamlabs/bento/blob/main/resources/protos/message.proto
[processor-proto]: https://github.com/warpstreamlabs/bento/blob/main/resources/protos/processor.proto
[abi-wat]: https://github.com/warpstreamlabs/bento/blob/main/resources/wasm/abi.wat
[errors]: /docs/configuration/error_handling
[wazero]: https://wazero.io/
[examples]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples
[about]: /docs/guides/plugins/about
[developing]: /docs/guides/plugins/developing_plugins
[fields]: /docs/guides/plugins/fields
[mounts]: /docs/guides/plugins/fields#runtimewasmmounts
[memory-limit]: /docs/guides/plugins/fields#runtimewasmmemorymax_pages
