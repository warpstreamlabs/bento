---
title: Getting Started
---

## Writing Plugins

Bento provides plugin development kits (PDKs) for [Go][go-pdk] and [Rust][rust-pdk], which should be used when writing plugins. They were designed to resemble how traditional Bento components are written, handling the registration and execution of plugins behind the scenes.

A plugin requires three key components:

1. **Plugin manifest** (`plugin.yaml`) - Defines metadata, config schema, and runtime settings
2. **Plugin implementation** - Code implementing the processor
3. **Registration** - Hooking your implementation into the PDK

Complete working examples are available at [wasm/examples][wasm-examples].

### Plugin.yaml

A `plugin.yaml` manifest is required for all plugins to register within the Bento component library.

You can see all available manifest fields in the [Manifest Reference][fields].

#### Metadata

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

#### Runtime

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

Plugins can't access the host's filesystem except through mounts. However, symbolic links within a mounted directory are followed, even when they point outside of it, and a plugin can create such links itself in a writable mount. Only mount directories that the plugin may access in full. Mounts are read-only unless they set `read_only: false`, which should be avoided where possible.

### Go

1. Define a config struct that matches your manifest.
```go
type config struct {
	MyField string `json:"my_field"`
}
```

The PDK parses the config into this struct before calling your constructor.

2. Define your component in accordance with the appropriate interface. A `service.BatchProcessor` is called with every message of a batch, and a `service.Processor` once per message.

```go
func newMyProcessor(cfg *config, mgr *service.Resources) (service.BatchProcessor, error) {
	return &myProc{field: cfg.MyField}, nil
}
```

3. Register the component with the PDK within an `init()` function, using the `batch_processor` package for a `service.BatchProcessor`, or the `processor` package for a `service.Processor`:

```go
import plugin "github.com/warpstreamlabs/bento/public/wasm/service/batch_processor"

func init() {
	if err := plugin.RegisterBatchProcessor(newMyProcessor); err != nil {
		panic(err)
	}
}
```

To fail a single message, set an error on it with `SetError`, and the message continues through the pipeline marked with that error. Returning an error from `ProcessBatch` or `Process` instead fails every message of the call.

#### Compiling

Compile your plugin to WebAssembly using either [TinyGo][tinygo]* or Go. Either needs a Go toolchain at least as recent as the version in Bento's `go.mod`. TinyGo must also support that Go version, and requires [binaryen][binaryen]'s `wasm-opt` to be installed.

```sh
# Using TinyGo (recommended)
GOOS="wasip1" GOARCH="wasm" tinygo build -scheduler=none -buildmode=c-shared -o ./plugin/plugin.wasm ./main.go

# Using Go
GOOS="wasip1" GOARCH="wasm" go build -buildmode=c-shared -o ./plugin/plugin.wasm ./main.go
```

*With TinyGo, `-scheduler=none` is strongly recommended. Otherwise every call into the plugin starts a goroutine, which can make each call more than ten times slower. Plugins built this way can't start goroutines.

### Rust

1. Create a library crate that depends on the `bento-pdk` crate, and builds a `cdylib`:

```toml
[package]
name = "my_plugin"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
bento-pdk = { git = "https://github.com/warpstreamlabs/bento" }
serde = { version = "1.0", features = ["derive"] }
```

2. Mark your functions with the `bento_plugin` attribute. An optional `init` function is called once per instance with the plugin's config, which is deserialized with [serde][serde]. A `process_batch` function is called with every message of a batch, or a `process` function once per message:

```rust
use std::sync::OnceLock;

use bento_pdk::{bento_plugin, Message, Result};
use serde::Deserialize;

#[derive(Deserialize)]
struct Config {
    my_field: String,
}

static MY_FIELD: OnceLock<String> = OnceLock::new();

#[bento_plugin(init)]
fn init(conf: Config) -> Result<()> {
    MY_FIELD.set(conf.my_field).map_err(|_| "plugin already initialised")?;
    Ok(())
}

#[bento_plugin(process_batch)]
fn process_batch(mut batch: Vec<Message>) -> Result<Vec<Vec<Message>>> {
    let field = MY_FIELD.get().ok_or("plugin not initialised")?;
    for msg in &mut batch {
        msg.meta_set("my_field", field);
    }
    Ok(vec![batch])
}
```

A `process` function has the signature `fn(msg: Message) -> Result<Vec<Message>>`. A `Message` provides its content with `bytes` and `set_bytes`, its metadata with `meta_get` and `meta_set`, and its error with `error` and `set_error`.

To fail a single message, set an error on it with `set_error`, and the message continues through the pipeline marked with that error. Returning an `Err` instead fails every message of the call.

#### Compiling

Compile your plugin for `wasm32-unknown-unknown`, or for `wasm32-wasip1` if it reads files mounted with `runtime.wasm.mounts`:
```sh
rustup target add wasm32-unknown-unknown
cargo build --target wasm32-unknown-unknown --release
cp target/wasm32-unknown-unknown/release/my_plugin.wasm ./plugin/plugin.wasm
```

See the [`rust_csv`][rust-csv] example for a simple Rust plugin, and the [`rust_tokenizer`][rust-tokenizer] example for one that loads files during `init`.

## Loading Plugins

Start up Bento with the `--plugins/-p` flag to specify one or more plugin directories:
```sh
bento -c config.yaml -p ./reverse/plugin -p ./uppercase/plugin
```

Each plugin directory should contain both `plugin.yaml` and `plugin.wasm` files. Plugins are loaded at startup and become available as processors in your pipeline configuration:
```yml
input:
  resource: ingest_data
  processors:
    - reverse: {}  # Loaded from ./reverse/plugin

pipeline:
  processors:
    - uppercase: {}  # Loaded from ./uppercase/plugin

output:
  resource: write_data
  processors:
    - reverse: {}  # Puts data back in order before writing
```

### Registering from Go

When building a custom Bento binary, plugins can instead be embedded and registered from Go, similar to [templates][templates]. Provide the manifest and the compiled WASM binary, typically within an `init()` function:

```go
import (
	_ "embed"

	"github.com/warpstreamlabs/bento/public/service"

	// Enables the WASM plugin runtime (already included by public/components/all).
	_ "github.com/warpstreamlabs/bento/public/components/wasm"
)

//go:embed plugin.yaml
var reverseManifest string

//go:embed plugin.wasm
var reverseWasm []byte

func init() {
	if err := service.RegisterWasmPlugin(reverseManifest, reverseWasm); err != nil {
		panic(err)
	}
}
```

Since the binary is provided directly, `runtime.wasm.path` is ignored. The binary is only compiled the first time a pipeline uses the plugin, and then stays loaded for the lifetime of the process.

For a complete example of building a plugin, see the [Reverse Processor Example][examples].

[go-pdk]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/service
[rust-pdk]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/pdk/rust
[wasm-examples]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples
[rust-csv]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples/rust_csv
[rust-tokenizer]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples/rust_tokenizer
[fields]: /docs/guides/plugins/fields
[tinygo]: https://tinygo.org/getting-started/install/
[binaryen]: https://github.com/WebAssembly/binaryen
[serde]: https://serde.rs/
[examples]: /docs/guides/plugins/examples
[templates]: /docs/configuration/templating
