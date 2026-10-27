---
title: Plugins
sidebar_label: Overview
---

Extending Bento typically requires recompiling the binary and maintaining custom forks. Bento's WebAssembly (WASM) plugin system solves this by allowing you to define custom processor components as standalone modules that load at runtime -- no recompilation required!

This section covers:

- [Loading Plugins][using] - Loading plugins, testing them and how they run
- [Developing Plugins][developing] - Writing, compiling and testing your own plugins
- [Manifest Reference][fields] - Full configuration schema documentation

## Language Support

Bento provides a plugin development kit (PDK) for **Go** ([`public/wasm/service`][go-pdk]), compiled with [TinyGo][tinygo] or Go.

Plugins can be written in any other language that compiles to a 32-bit WASM module, such as Rust, by implementing the [ABI][abi] directly. This is highly experimental.

## File Structure

A Bento plugin consists of two files within a dedicated directory:

```
my-plugin/
├── plugin.yaml  # Plugin manifest (required)
└── plugin.wasm  # Compiled WASM binary (required)
```

## Plugin.yaml

A `plugin.yaml` manifest is required for all plugins to register within the Bento component library.

You can see all available manifest fields in the [Manifest Reference][fields].

### Metadata

The values below are placeholders, and the comments describe each field:

```yml
name: my_plugin    # Required. The name of the processor.
type: processor    # Required. The type of component. Only "processor" is supported.
status: stable     # Optional. "stable" (default), "beta" or "experimental".
summary: ""        # Optional. A short summary of the plugin.
description: ""    # Optional. A detailed description of the plugin and how to use it.
```

Plugins can accept configuration fields just like native Bento components. Define fields in your `plugin.yaml` manifest:
```yml
fields:
  - name: my_field
    description: An example configuration field
    type: string
    default: ""
```

The config of each processor is passed to the plugin as JSON when it starts.

### Runtime

The `runtime.wasm` section configures the environment the plugin runs in. Every field in it is optional, and the values below are placeholders:

```yml
runtime:
  wasm:
    path: plugin.wasm       # The plugin's WASM binary, relative to the plugin directory (default "plugin.wasm").
    memory:
      max_pages: 128        # The most memory each instance may use, in 64KiB pages.
    mounts:                 # Directories on the host that the plugin may access.
      - host_path: ./data   # Required for each mount. The directory on the host.
        guest_path: /data   # Required for each mount. Where the plugin sees the directory.
        read_only: true     # Whether the plugin may only read the directory (default true).
```

`max_pages` limits the linear memory of each instance of the plugin, so a plugin running several instances can use that much memory for each one. It does not limit the memory Bento itself uses. When it is omitted or zero, an instance may use up to 4GiB, the most a 32-bit WASM module can address.

Plugins can't access the host's filesystem except through mounts. See [Plugin Security][security] before mounting a directory.

[using]: /docs/guides/plugins/loading_plugins
[developing]: /docs/guides/plugins/developing_plugins
[security]: /docs/guides/plugins/loading_plugins#plugin-security
[fields]: /docs/guides/plugins/fields
[abi]: /docs/guides/plugins/abi
[go-pdk]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/service
[tinygo]: https://tinygo.org/
