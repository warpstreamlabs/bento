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

The [Go PDK][go-pdk] is the most complete, and a minimal [Rust PDK][rust-pdk] is also available.

Plugins can be written in any language that compiles to a 32-bit WASM module, such as C, Zig or AssemblyScript. Messages are passed to a plugin as [protobuf][message-proto], and the plugin returns its results in the same format.

Data is exchanged through buffers in the plugin's memory, passed as an `i64` holding the address in its upper 32 bits and the length in its lower 32 bits. Bento writes input into buffers the plugin allocates with its `alloc` export, which the plugin may read until the call returns. The plugin returns its output in a buffer it allocates, which Bento copies. Bento releases every buffer once it is done with it through the `release` export. A plugin exports:

- `alloc(size)` and `release(address, length)`, which allocate and free buffers.
- Optionally `init_plugin(config)`, called once per instance with its JSON encoded config. It returns a buffer holding an error message, which is empty on success.
- Either `process_batch(batch)`, called with every message of a batch as a `Batch` and returning `Batches`, or `process(part)`, called once per message as a `Part` and returning a `Batch`.

Any message that is not returned is dropped, and errors are reported by setting them on the messages a plugin returns. The ABI is documented in [`host.go`][abi].

[getting-started]: /docs/guides/plugins/getting_started
[examples]: /docs/guides/plugins/examples
[fields]: /docs/guides/plugins/fields
[go-pdk]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/service
[rust-pdk]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/pdk/rust
[message-proto]: https://github.com/warpstreamlabs/bento/blob/main/resources/protos/message.proto
[abi]: https://github.com/warpstreamlabs/bento/blob/main/internal/plugin/runtime/wazero_v1/host.go
