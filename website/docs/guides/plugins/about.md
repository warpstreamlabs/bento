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

### Check a Plugin Before Loading It

Only load plugins from sources you trust. Inspect the manifest's [`runtime.wasm` settings][wasm-fields] for mounted directories, write access, and the memory limit per instance. Without `memory.max_pages`, each instance can allocate up to 4 GiB, and concurrent calls can create multiple instances. A plugin can follow symlinks in a mounted directory to files outside it. See [The Sandbox][sandbox] for the runtime's capabilities.

## Why WebAssembly (WASM)?

Bento has two other ways to add behavior. A [custom build][custom-build] adds components written in Go against the full plugin API, but you have to compile and maintain your own Bento binary. [Templates][templates] need no build at all, but they can only combine components that already exist.

Plugins sit between the two. They can contain new logic, and they run on any stock Bento binary. WebAssembly is what makes this possible:

- **Portability.** One compiled module runs on every platform that Bento runs on, so a plugin is distributed as a file rather than built per machine.
- **Isolation.** A plugin runs in a sandbox, with no network access and with access to only the directories its manifest mounts. A crash in a plugin fails the messages it was processing rather than the Bento process.
- **Language choice.** Any language that compiles to a 32-bit WASM module can implement the [ABI][abi], not only Go.

This isolation requires copying messages into and out of the plugin's memory on each call. Plugins can only be processors, and they can't use Bento [resources][resources] such as caches. Use a custom build when you need another component type or access to those resources.

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
- [Architecture][architecture]: how plugins run, how errors behave, and the ABI.

[custom-build]: https://pkg.go.dev/github.com/warpstreamlabs/bento/public/service
[pdk]: https://pkg.go.dev/github.com/warpstreamlabs/bento/public/wasm/service
[templates]: /docs/configuration/templating
[resources]: /docs/configuration/resources
[abi]: /docs/guides/plugins/architecture#abi
[developing]: /docs/guides/plugins/developing_plugins
[fields]: /docs/guides/plugins/fields
[unit-testing]: /docs/configuration/unit_testing
[wasm-fields]: /docs/guides/plugins/fields#runtimewasm
[sandbox]: /docs/guides/plugins/architecture#the-sandbox
[examples]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples
[uppercase-example]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples/uppercase
[reverse-example]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples/reverse
[rust-uppercase-example]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples/rust_uppercase
[rust-csv-example]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples/rust_csv
