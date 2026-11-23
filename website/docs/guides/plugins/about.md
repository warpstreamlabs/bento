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

## Loading Plugins

List the plugins to load in the `plugins` section of your config, then use each one as a processor. The processor name must match `name` in the plugin's manifest:

```yml
plugins:
  - source: ./my-plugin

pipeline:
  processors:
    - my-plugin: {}
```

Bento loads the plugins in the `plugins` section before checking the rest of the config, so commands that read the config, such as `lint` and `test`, load them too:

```sh
bento -c config.yaml
bento lint config.yaml
bento test config.yaml
```

### Granting Mounts and Memory

A plugin's manifest lists the capabilities it [`requires`][fields], such as directories it expects to be mounted. It can't grant them to itself: you decide which host directories it gets and how much memory it may use, in the same `plugins` entry:

```yml
plugins:
  - source: ./my-plugin
    memory:
      max_pages: 128 # Limits each instance to 8MiB (64KiB per page)
    mounts:
      data: # The name of a mount the plugin requires
        host_path: ./data
        writable: false
```

Bento refuses to load a plugin when:

- It requires a mount that isn't granted, unless the mount is optional.
- A mount is granted that the plugin doesn't request.
- A mount is granted as writable but the plugin only requests read access.
- `max_pages` is below the `min_pages` the plugin requires.

Without `max_pages`, a plugin has no memory limit, the same as components compiled into Bento.

### The `-p` Flag

Plugins can also be loaded with `-p` (or `--plugin`), which can be used multiple times and supports glob patterns. Plugins loaded this way get no mounts or memory limits, so use the `plugins` section for plugins that need them. If a plugin is passed with `-p` and also listed in the `plugins` section with mounts or a memory limit, Bento refuses to start.

```sh
bento -p ./my-plugin -c config.yaml
```

See [Unit Testing][unit-testing] for writing tests.

## Why WebAssembly (WASM)?

While [custom builds][custom-build] in Go give you the full plugin API at the cost of compiling and maintaining your own binary, [templates][templates] require no build step at all -- but can only combine existing components.

Bento WASM plugins sit between the two: they introduce custom logic while running on any stock Bento binary. WebAssembly makes this possible through three guarantees:

- **Portability.** Once compiled, a WASM module runs on every platform Bento supports, so it can be distributed as a single package rather than built per target machine.
- **Isolation.** Modules run in a sandbox with no access to host resources unless explicitly granted at instantiation. Because each module is confined to its own dedicated linear memory, a fatal plugin crash or OOM won't take down the main Bento process.
- **Language flexibility.** You aren't limited to Go—any language that compiles to a 32-bit WASM target can implement the [ABI][abi].

This isolation requires copying messages into and out of the plugin's memory on each call. Plugins can only be processors, and they can't use Bento [resources][resources] such as caches. Use a custom build when you need another component type or access to those resources.

:::warning Check what a plugin asks for
A plugin only gets the host access you grant it in the `plugins` section, but check its [`requires`][fields] before granting it:
- **Filesystem access:** A plugin can follow symlinks inside a mounted directory to targets outside it. Only grant write access where it's needed.
- **Memory limits:** Without `memory.max_pages`, each instance can allocate up to 4 GiB, and concurrent calls can spawn multiple instances.

See the [Sandbox][sandbox] in the Plugin ABI guide for full details on runtime capabilities.
:::

## Enabling the Plugin Runtime

The plugin runtime is enabled in the released `bento` binary from `v1.24` onwards.

To enable it for [custom builds][custom-build], import the `public/plugin/wazero_v1_beta` package:

```go
import _ "github.com/warpstreamlabs/bento/public/plugin/wazero_v1_beta"
```

Once enabled, a custom build can also register a plugin via [`service.RegisterWasmPlugin`][register-wasm]:

```go
//go:embed plugin/plugin.yaml
var manifest string

//go:embed plugin/plugin.wasm
var wasm []byte

func init() {
	if err := service.RegisterWasmPlugin(manifest, wasm); err != nil {
		panic(err)
	}
}
```

Plugins registered from Go are trusted, so no mounts are granted to them through the config. They may still use the deprecated `runtime` block of the manifest.

## Migrating from the `runtime` Block

Earlier manifests set mounts and memory limits in a `runtime.wasm` block, which let a plugin choose its own access to the host. That block is deprecated and will be removed in a future release. Plugins that use it still load from a local directory, with a warning that shows the replacement.

To migrate a plugin, replace its `runtime` block with `requires`:

```yml title="Before: plugin.yaml"
runtime:
  wasm:
    path: plugin.wasm
    mounts:
      - host_path: ./data
        guest_path: /data
    memory:
      max_pages: 128
```

```yml title="After: plugin.yaml"
requires:
  mounts:
    - name: data
      guest_path: /data
```

And move the host path and memory limit into the config that runs it:

```yml title="config.yaml"
plugins:
  - source: ./my-plugin
    memory:
      max_pages: 128
    mounts:
      data:
        host_path: ./data
```

The WASM binary must be named `plugin.wasm`, as `runtime.wasm.path` has no replacement. Mounts were read-only unless `read_only: false` was set; request `writable: true` and grant it with `writable: true` for the same behavior.

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
[register-wasm]: https://pkg.go.dev/github.com/warpstreamlabs/bento/public/service#RegisterWasmPlugin
[pdk]: https://pkg.go.dev/github.com/warpstreamlabs/bento/public/wasm/service
[templates]: /docs/configuration/templating
[resources]: /docs/configuration/resources
[abi]: /docs/guides/plugins/plugin_abi#exports
[developing]: /docs/guides/plugins/developing_plugins
[fields]: /docs/guides/plugins/fields
[unit-testing]: /docs/configuration/unit_testing
[sandbox]: /docs/guides/plugins/plugin_abi#sandbox
[examples]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples
[uppercase-example]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples/uppercase
[reverse-example]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples/reverse
[rust-uppercase-example]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples/rust_uppercase
[rust-csv-example]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples/rust_csv
