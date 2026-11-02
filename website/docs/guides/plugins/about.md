---
title: Plugins
sidebar_label: About
---

:::caution EXPERIMENTAL
Plugins are an experimental feature. The manifest, the Go PDK, and the ABI may change outside of major version releases.
:::

## Overview

Bento's WebAssembly (WASM) plugin system allows you to dynamically extend the component library with your own custom processors. Instead of recompiling the core binary, you can define processors as standalone modules that load directly at runtime.

A Bento plugin consists of two files within a dedicated directory:

```
my-plugin/
├── plugin.yaml  # Plugin manifest (required)
└── plugin.wasm  # Compiled WASM binary (required)
```

Load the plugin with `-p` (or `--plugin`):

```sh
bento -p ./my-plugin -c config.yaml
```

The `-p` flag can be used multiple times to load multiple plugins.

Configure it as a processor in `config.yaml`. The processor name must match `name` in the manifest:

```yml
pipeline:
  processors:
    - my-plugin: {}
```

Pass the same `-p` flag to commands that read the config, such as `test` and `lint`:

```sh
bento -p ./my-plugin test config.yaml
bento -p ./my-plugin lint config.yaml
```

See [Unit Testing][unit-testing] for writing tests.

## Why WebAssembly (WASM)?

While [custom builds][custom-build] in Go give you the full plugin API at the cost of compiling and maintaining your own binary, [templates][templates] require no build step at all -- but can only combine existing components.

Bento WASM plugins sit between the two: they introduce custom logic while running on any stock Bento binary. WebAssembly makes this possible through three guarantees:

- **Portability.** Once compiled, a WASM module runs on every platform Bento supports, so it can be distributed as a single package rather than built per target machine.
- **Isolation.** Modules run in a sandbox with no access to host resources unless explicitly granted at instantiation. Because each module is confined to its own dedicated linear memory, a fatal plugin crash or OOM won't take down the main Bento process.
- **Language flexibility.** You aren't limited to Go—any language that compiles to a 32-bit WASM target can implement the [ABI][abi].

This isolation requires copying messages into and out of the plugin's memory on each call. Plugins can only be processors, and they can't use Bento [resources][resources] such as caches. Use a custom build when you need another component type or access to those resources.

:::warning Inspect manifests before loading
Because host permissions are configured in `plugin.yaml`, always audit a third-party plugin's [`runtime.wasm` settings][wasm-fields] before loading it:
- **Filesystem access:** Check for mounted directories and write permissions. A plugin can follow symlinks inside a mounted directory to targets outside it.
- **Memory limits:** Without `memory.max_pages`, each instance can allocate up to 4 GiB, and concurrent calls can spawn multiple instances.

See [The Sandbox][sandbox] for full details on runtime capabilities.
:::

## Language Support

Bento provides a [plugin development kit (PDK) for Go][pdk], which handles the ABI for you. Plugins in other languages implement the [ABI][abi] directly, which is more work and more likely to break between releases. The [Rust examples][examples] show how.

## Examples

Complete plugins are available at [`public/wasm/examples`][examples]:

| Example                                    | Language | Description                                                        |
|--------------------------------------------|----------|--------------------------------------------------------------------|
| [`uppercase`][uppercase-example]           | Go       | Uppercases each message and prepends a configurable prefix.         |
| [`reverse`][reverse-example]               | Go       | Reverses a batch of messages, and records their positions.         |
| [`rust_uppercase`][rust-uppercase-example] | Rust     | Uppercases each message, implementing the ABI without a PDK.       |
| [`rust_csv`][rust-csv-example]             | Rust     | Parses CSV messages into JSON, implementing the ABI without a PDK. |

## Next Steps

- [Developing Plugins][developing]: build, run, and test your first plugin with the Go PDK.
- [Manifest Fields][fields]: every field of `plugin.yaml`.
- [Plugin ABI][plugin-abi]: how plugins run, how errors behave, and the ABI.

[plugin-abi]: /docs/guides/plugins/plugin_abi
[custom-build]: https://pkg.go.dev/github.com/warpstreamlabs/bento/public/service
[pdk]: https://pkg.go.dev/github.com/warpstreamlabs/bento/public/wasm/service
[templates]: /docs/configuration/templating
[resources]: /docs/configuration/resources
[abi]: /docs/guides/plugins/plugin_abi#exports
[developing]: /docs/guides/plugins/developing_plugins
[fields]: /docs/guides/plugins/fields
[unit-testing]: /docs/configuration/unit_testing
[wasm-fields]: /docs/guides/plugins/fields#runtimewasm
[sandbox]: /docs/guides/plugins/plugin_abi#sandbox
[examples]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples
[uppercase-example]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples/uppercase
[reverse-example]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples/reverse
[rust-uppercase-example]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples/rust_uppercase
[rust-csv-example]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples/rust_csv
