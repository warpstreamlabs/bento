Bento Rust PDK
==============

A library for writing [Bento][bento] processor plugins in Rust, which Bento runs as WebAssembly modules. See the [plugin guides][guides] for how plugins are configured and loaded.

## Usage

Create a library crate that builds a `cdylib` and depends on `bento-pdk`:

```toml
[lib]
crate-type = ["cdylib"]

[dependencies]
bento-pdk = { git = "https://github.com/warpstreamlabs/bento" }
serde = { version = "1.0", features = ["derive"] }
```

Then mark the plugin's entry points with the `bento_plugin` attribute:

```rust
use std::sync::OnceLock;

use bento_pdk::{bento_plugin, Message, Result};
use serde::Deserialize;

#[derive(Deserialize)]
struct Config {
    prefix: String,
}

static PREFIX: OnceLock<String> = OnceLock::new();

#[bento_plugin(init)]
fn init(conf: Config) -> Result<()> {
    PREFIX.set(conf.prefix).map_err(|_| "plugin already initialised")?;
    Ok(())
}

#[bento_plugin(process)]
fn process(mut msg: Message) -> Result<Vec<Message>> {
    let prefix = PREFIX.get().ok_or("plugin not initialised")?;
    let content = [prefix.as_bytes(), &msg.bytes()].concat();
    msg.set_bytes(content);
    Ok(vec![msg])
}
```

The attribute takes one of:

| Entry point     | Signature                                              | Description                                                                                        |
|-----------------|--------------------------------------------------------|----------------------------------------------------------------------------------------------------|
| `init`          | `fn(config: C) -> Result<()>`                          | Optional. Called once per instance with the plugin's config, deserialized with serde into any `C`. |
| `process_batch` | `fn(batch: Vec<Message>) -> Result<Vec<Vec<Message>>>` | Called with every message of a batch.                                                              |
| `process`       | `fn(msg: Message) -> Result<Vec<Message>>`             | Called once per message. Only used when there is no `process_batch`.                               |

Messages that aren't returned are dropped. To fail a single message, set an error on it with `set_error`, and it continues through the pipeline marked with that error. Returning an `Err` instead fails every message of the call.

Bento may run several instances of a plugin at once, each processing one call at a time. State kept in statics, such as the config above, belongs to a single instance.

## Messages

| Method                                  | Description                                                                    |
|-----------------------------------------|--------------------------------------------------------------------------------|
| `Message::new(content)`                 | Creates a message, such as to return more messages than were passed in.        |
| `bytes()`, `set_bytes(content)`         | Get and replace the content. Structured content is returned encoded as JSON.   |
| `meta_get(key)`, `meta_set(key, value)` | Get and set metadata. Values that aren't strings are returned encoded as JSON. |
| `error()`, `set_error(err)`             | Get and set the error of the message. An empty error clears it.                |

Run `cargo doc --open -p bento-pdk` from your plugin's crate for the full API reference.

## Building

Build plugins for `wasm32-unknown-unknown`, or for `wasm32-wasip1` if they read files mounted with `runtime.wasm.mounts`:

```sh
rustup target add wasm32-unknown-unknown
cargo build --target wasm32-unknown-unknown --release
cp target/wasm32-unknown-unknown/release/my_plugin.wasm ./plugin/plugin.wasm
```

The `plugin` directory also needs a [`plugin.yaml` manifest][manifest]. See [`public/wasm/examples`][examples] for complete plugins, such as `rust_csv` and `rust_tokenizer`.

[bento]: https://warpstreamlabs.github.io/bento/
[guides]: https://warpstreamlabs.github.io/bento/docs/guides/plugins/about
[manifest]: https://warpstreamlabs.github.io/bento/docs/guides/plugins/fields
[examples]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples
