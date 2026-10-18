---
title: Examples
---

All examples are available at [wasm/examples](https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples):

| Example                                    | Language | Description                                                                       |
|--------------------------------------------|----------|-----------------------------------------------------------------------------------|
| [`reverse`][reverse-example]               | Go       | Reverses a batch of messages, walked through below in Go and Rust.                |
| [`uppercase`][uppercase-example]           | Go       | Uppercases each message, with an optional prefix from its config.                 |
| [`rust_csv`][rust-csv-example]             | Rust     | Parses CSV messages into JSON.                                                    |
| [`rust_tokenizer`][rust-tokenizer-example] | Rust     | Tokenizes text with a Hugging Face tokenizer, which it loads from a mounted file. |

## Example: Reverse Processor

Now, let's create a simple processor using the Go PDK that reverses a batch of messages and adds some metadata about the original position.

The full example is available at [wasm/examples/reverse][reverse-example], but the steps for creation are outlined below. They require Go and TinyGo, as described in [Compiling][compiling].

1. Create a `reverse` directory containing a Go module and a `plugin` subdirectory, and run every following command from within it:

```sh
mkdir -p reverse/plugin
cd reverse
go mod init example.com/reverse
```

2. Add a `plugin/plugin.yaml` with the following manifest:

```yml
name: reverse
type: processor
status: stable
summary: Reverses a batch.
description: "A WASM plugin used to reverse a batch of messages."

fields: [] # We don't expect any fields

runtime:
  wasm:
    memory:
      max_pages: 128 # Limits each instance to 8MiB (64KiB per page)
```

3. Add a `main.go` with the processor. It implements the `service.BatchProcessor` interface, as it processes a batch of messages at a time, and so is registered with the `batch_processor` package. The plugin doesn't need a config, so its constructor accepts a `service.ParsedConfig` that it ignores. A plugin also needs an empty `main` function to compile.

```go
package main

import (
	"context"
	"fmt"
	"slices"

	"github.com/warpstreamlabs/bento/public/wasm/service"
	plugin "github.com/warpstreamlabs/bento/public/wasm/service/batch_processor"
)

func init() {
	if err := plugin.RegisterBatchProcessor(newReverseProcessor); err != nil {
		panic(err)
	}
}

func newReverseProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	return &reverseProc{}, nil
}

type reverseProc struct{}

func (p *reverseProc) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	for i, part := range batch {
		part.MetaSet("reversed_position", fmt.Sprintf("%d", i))
		part.MetaSet("processed_by", "reverse_wasm")
	}

	slices.Reverse(batch)
	return []service.MessageBatch{batch}, nil
}

func (p *reverseProc) Close(ctx context.Context) error {
	return nil
}

func main() {}
```

4. Add Bento to the module's dependencies, and compile the plugin with TinyGo:

```sh
go mod tidy
GOOS="wasip1" GOARCH="wasm" tinygo build -scheduler=none -buildmode=c-shared -o ./plugin/plugin.wasm .
```

Your final directory should look like this:

```
reverse/
├── go.mod
├── go.sum
├── main.go
└── plugin/
    ├── plugin.yaml
    └── plugin.wasm
```

5. Add a `config.yaml` that generates a batch of three messages, reverses it, and prints each message with its original position:

```yml
input:
  generate:
    count: 3
    batch_size: 3
    interval: ""
    mapping: 'root = "message " + counter().string()'

pipeline:
  processors:
    - reverse: {}
    - mapping: 'root = content().string() + " was at " + @reversed_position'

output:
  stdout: {}
```

6. Run Bento with the `--plugins/-p` flag to load the plugin directory:

```sh
bento -c config.yaml -p ./plugin
```

Which prints the batch in reverse:

```
message 3 was at 2
message 2 was at 1
message 1 was at 0
```

For more information on manifest configuration options, see the [Manifest Reference][fields].

## Example: Reverse Processor in Rust

The same processor can be written in Rust using the [Rust PDK][rust-pdk]. This requires Rust and the `wasm32-unknown-unknown` target:

```sh
rustup target add wasm32-unknown-unknown
```

1. Create a library crate, and run every following command from within it:

```sh
cargo new --lib reverse
cd reverse
mkdir plugin
```

2. Replace its `Cargo.toml` so that it depends on the PDK and builds a WASM module:

```toml
[package]
name = "reverse"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
bento-pdk = { git = "https://github.com/warpstreamlabs/bento" }
```

3. Replace `src/lib.rs` with the processor. The `bento_plugin` attribute exports `process_batch`, which is called with every message of a batch:

```rust
use bento_pdk::{bento_plugin, Message, Result};

#[bento_plugin(process_batch)]
fn process_batch(mut batch: Vec<Message>) -> Result<Vec<Vec<Message>>> {
    for (i, msg) in batch.iter_mut().enumerate() {
        msg.meta_set("reversed_position", &i.to_string());
        msg.meta_set("processed_by", "reverse_wasm");
    }

    batch.reverse();
    Ok(vec![batch])
}
```

4. Compile the plugin, and copy it into the plugin directory:

```sh
cargo build --target wasm32-unknown-unknown --release
cp target/wasm32-unknown-unknown/release/reverse.wasm ./plugin/plugin.wasm
```

5. Add the `plugin/plugin.yaml` from step 2 and the `config.yaml` from step 5 of the [Go example](#example-reverse-processor), and run Bento:

```sh
bento -c config.yaml -p ./plugin
```

Which prints the same output:

```
message 3 was at 2
message 2 was at 1
message 1 was at 0
```

[reverse-example]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples/reverse
[uppercase-example]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples/uppercase
[rust-csv-example]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples/rust_csv
[rust-tokenizer-example]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples/rust_tokenizer
[rust-pdk]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/pdk/rust
[fields]: /docs/guides/plugins/fields
[compiling]: /docs/guides/plugins/getting_started#compiling
