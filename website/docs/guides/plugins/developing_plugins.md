---
title: Developing Plugins
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Bento provides a plugin development kit (PDK) for [Go][go-pdk], which should be used when writing plugins. It was designed to resemble how traditional Bento components are written, handling the registration and execution of plugins behind the scenes. Plugins in [other languages](#other-languages) can implement the [ABI][abi] directly.

A plugin requires three key components:

1. **Plugin manifest** (`plugin.yaml`) - Defines metadata, config schema, and runtime settings
2. **Plugin implementation** - Code implementing the processor
3. **Registration** - Hooking your implementation into the PDK

See the [Overview][overview] for the plugin's file structure and manifest, and [Loading Plugins][using] for loading plugins and how they run.

## Building a Processor

This walkthrough builds a processor with the [Go PDK][go-pdk] that uppercases each message after a prefix from its config, and records in its metadata that it processed it. It takes you from an empty directory to a plugin that runs and passes a test.

You need a Bento binary, and Go or TinyGo, as described in [Compiling](#compiling).

Create a directory for the project, containing a Go module and a `plugin` directory for the compiled plugin, and run every following command from within it:

```sh
mkdir -p uppercase/plugin
cd uppercase
go mod init example.com/uppercase
```

### Plugin Manifest

Add a `plugin/plugin.yaml` manifest. Its `name` is the processor's name in a pipeline, and its `fields` are the processor's config:

```yml title="plugin/plugin.yaml"
name: uppercase
type: processor
status: stable
summary: Uppercases each message, after a prefix.

fields:
  - name: prefix
    description: Added before the content of each message.
    type: string
    default: ""

runtime:
  wasm:
    memory:
      max_pages: 128 # Limits each instance to 8MiB (64KiB per page)
```

For more information on manifest configuration options, see the [Manifest Reference][fields].

### Configuring the Pipeline

Add a `config.yaml` that reads messages from standard input, uppercases them after a prefix, and prints each with its metadata:

```yml title="config.yaml"
input:
  stdin: {}

pipeline:
  processors:
    - uppercase:
        prefix: "shout: "
    - mapping: 'root = content().string() + " processed by " + @processed_by'

output:
  stdout: {}
```

### Writing the Plugin

The plugin is written with the [Go PDK][go-pdk], which handles the [ABI][abi] between the plugin and Bento, so that a plugin is written much like any other Bento processor. Add it to the module's dependencies:

```sh
go get github.com/warpstreamlabs/bento/public/wasm/service/processor
```

Then add a `main.go`. The processor implements the `service.Processor` interface, as it processes one message at a time, and so is registered with the `processor` package. Its constructor is called with the processor's config, decoded from JSON into `config`. A plugin also needs an empty `main` function to compile.

```go title="main.go"
package main

import (
	"bytes"
	"context"

	"github.com/warpstreamlabs/bento/public/wasm/service"
	plugin "github.com/warpstreamlabs/bento/public/wasm/service/processor"
)

type config struct {
	Prefix string `json:"prefix"`
}

func init() {
	if err := plugin.RegisterProcessor(newUppercaseProcessor); err != nil {
		panic(err)
	}
}

func newUppercaseProcessor(conf *config, mgr *service.Resources) (service.Processor, error) {
	return &uppercaseProc{prefix: conf.Prefix}, nil
}

type uppercaseProc struct {
	prefix string
}

func (p *uppercaseProc) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	content, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}

	msg.SetBytes(append([]byte(p.prefix), bytes.ToUpper(content)...))
	msg.MetaSet("processed_by", "uppercase_wasm")
	return service.MessageBatch{msg}, nil
}

func (p *uppercaseProc) Close(ctx context.Context) error {
	return nil
}

func main() {}
```

### Building & Running

Compile the plugin into the plugin directory with TinyGo or Go. See [Compiling](#compiling) for what each needs:

<Tabs groupId="go-compiler">
<TabItem value="tinygo" label="TinyGo (recommended)">

```sh
GOOS="wasip1" GOARCH="wasm" tinygo build -scheduler=none -buildmode=c-shared -o ./plugin/plugin.wasm .
```

</TabItem>
<TabItem value="go" label="Go">

```sh
GOOS="wasip1" GOARCH="wasm" go build -buildmode=c-shared -o ./plugin/plugin.wasm .
```

</TabItem>
</Tabs>

Run Bento with the `--plugins/-p` flag to load the plugin directory:

```sh
printf 'hello\n' | bento -c config.yaml -p ./plugin
```

Which prints:

```
shout: HELLO processed by uppercase_wasm
```

### Testing

Add a `config_bento_test.yaml` beside the config, with a [unit test][unit-testing] of the plugin. `target_processors` runs only the first processor, the plugin, and the test checks the content and metadata of the message it returns:

```yml title="config_bento_test.yaml"
tests:
  - name: uppercases each message after the prefix
    target_processors: /pipeline/processors/0
    input_batch:
      - content: hello
    output_batches:
      - - content_equals: "shout: HELLO"
          metadata_equals:
            processed_by: uppercase_wasm
```

Run the tests, loading the plugin directory as before:

```sh
bento -p ./plugin test config.yaml
```

Which prints:

```
Test 'config.yaml' succeeded
```

When a test fails, Bento prints what it expected and received, and exits with an error.

## Go PDK

The `main.go` above has the parts of every Go plugin:

- A config struct that matches the manifest's `fields`. The PDK parses the config into it before calling the constructor.
- A constructor that returns the component. A `service.Processor` is called once per message, and a `service.BatchProcessor` with every message of a batch.
- Registration within an `init()` function, using the `processor` package for a `service.Processor`, or the `batch_processor` package for a `service.BatchProcessor`, as the [`reverse`][reverse-example] example does.
- An empty `main` function.

To fail a single message, set an error on it with `SetError`, and the message continues through the pipeline marked with that error. Returning an error from `ProcessBatch` or `Process` instead fails every message of the call.

### Compiling

Compile your plugin to WebAssembly using either [TinyGo][tinygo]* or Go. Either needs Go 1.27.2 or later, the version in Bento's `go.mod`. TinyGo must also support that Go version, and requires [binaryen][binaryen]'s `wasm-opt` to be installed.

```sh
# Using TinyGo (recommended)
GOOS="wasip1" GOARCH="wasm" tinygo build -scheduler=none -buildmode=c-shared -o ./plugin/plugin.wasm .

# Using Go
GOOS="wasip1" GOARCH="wasm" go build -buildmode=c-shared -o ./plugin/plugin.wasm .
```

*With TinyGo, `-scheduler=none` is strongly recommended. Otherwise every call into the plugin starts a goroutine, which can make each call more than ten times slower. Plugins built this way can't start goroutines.

## Other Languages

:::caution
Writing a plugin without a PDK is highly experimental, and the ABI may change between Bento releases.
:::

Plugins can be written in any other language that compiles to a 32-bit WASM module, such as Rust, by implementing the [ABI][abi] directly. The plugin uses the same `plugin.yaml` manifest, and exports the functions that Bento calls to process messages, which it receives and returns as [protobuf][message-proto].

### Rust

The [`rust_uppercase`][rust-uppercase-example] and [`rust_csv`][rust-csv-example] examples implement the [ABI][abi] directly, without a PDK. Each is a `cdylib` crate that:

- Generates the messages of [`message.proto`][message-proto] in `build.rs`, with [prost][prost] and [protox][protox].
- Exports each function of the ABI under its name, with `#[cfg_attr(target_arch = "wasm32", export_name = "...")]`.

Their `build.rs` reads `message.proto` from the Bento repository, so build them within a clone of it. For example, to build `rust_uppercase` and run it with the `config.yaml` above:

```sh
rustup target add wasm32-unknown-unknown
cd public/wasm/examples/rust_uppercase
./build.sh
printf 'hello\n' | bento -c /path/to/config.yaml -p ./testdata
```

`build.sh` compiles the crate for `wasm32-unknown-unknown`, and copies it to `testdata/plugin.wasm`, beside the example's manifest. A plugin that reads files mounted with `runtime.wasm.mounts` is compiled for `wasm32-wasip1` instead.

## Examples

All examples are available at [wasm/examples](https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples):

| Example                                    | Language | Description                                                              |
|--------------------------------------------|----------|--------------------------------------------------------------------------|
| [`uppercase`][uppercase-example]           | Go       | Uppercases each message, with an optional prefix from its config.        |
| [`reverse`][reverse-example]               | Go       | Reverses a batch of messages, and records their positions.               |
| [`rust_uppercase`][rust-uppercase-example] | Rust     | Uppercases each message, like `uppercase`, implementing the ABI by hand. |
| [`rust_csv`][rust-csv-example]             | Rust     | Parses CSV messages into JSON, implementing the ABI by hand.             |

[go-pdk]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/service
[abi]: /docs/guides/plugins/abi
[overview]: /docs/guides/plugins/about
[using]: /docs/guides/plugins/loading_plugins
[message-proto]: https://github.com/warpstreamlabs/bento/blob/main/resources/protos/message.proto
[tinygo]: https://tinygo.org/getting-started/install/
[binaryen]: https://github.com/WebAssembly/binaryen
[prost]: https://github.com/tokio-rs/prost
[protox]: https://github.com/andrewhickman/protox
[uppercase-example]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples/uppercase
[reverse-example]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples/reverse
[rust-uppercase-example]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples/rust_uppercase
[rust-csv-example]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples/rust_csv
[fields]: /docs/guides/plugins/fields
[unit-testing]: /docs/configuration/unit_testing
