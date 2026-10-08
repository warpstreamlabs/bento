---
title: Getting Started
---

## Writing Plugins

Bento provides a [plugin development kit (PDK)][pdk] that should be used when writing plugins. This was designed to resemble how traditional Bento components are written, handling much of the drama behind-the-scenes of plugin registration and execution.

For now, only Go is supported by a PDK, with future versions providing better multi-language support.

A plugin requires three key components:

1. **Plugin manifest** (`plugin.yaml`) - Defines metadata, config schema, and runtime settings
2. **Plugin implementation** - Go code implementing the processor interface
3. **Registration** - Hooking your implementation into the PDK via `init()`

Complete working examples are available at [wasm/examples][wasm-examples].

### Plugin.yaml

A `plugins.yaml` manifest is required for all plugins to register within the Bento component library.

You can see all available manifest fields in the [Manifest Reference][fields].

#### Metadata

```yml
name: REQUIRED - The name of the plugin.
type: REQUIRED - The underlying component type. Can only be "processor".
status: OPTIONAL - The stability status of the plugin. Can be "stable" (default), "beta", or "experimental".
summary: OPTIONAL - A short summary of the plugin.
description: OPTIONAL - A detailed description of the plugin and how to use it.
```

Plugins can accept configuration fields just like native Bento components. Define fields in your `plugin.yaml` manifest:
```yml
fields:
  - name: my_field
    description: An example configuration field
    type: string
    default: ""
```

#### Runtime

Next, we need to specify paameters for our WASM environment at `runtime.wasm`:
```yml
runtime:
  wasm: REQUIRED - WASM runtime configuration object for the plugin.
```

Since our WASM plugin will run within a fully-sandboxed environment, memory constrains, file-mounts, and network access all
need to be specified upfront:
```yml
runtime:
  wasm:
    memory:
      max_pages: REQUIRED - The max amount of pages the plugin can allocate. One page is 64KiB.
    allowed_hosts: OPTIONAL - List of hosts that the plugin is allowed to connect to.
    mounts:
      - host_path: REQUIRED - Path on host to mount.
        guest_path: REQUIRED - Path inside the WASM container.
    path: OPTIONAL - Path to the plugin's WASM binary, relative to the plugin directory (default "plugin.wasm").
```


### Implementation

1. Define a config struct that matches your manifest.
```go
type config struct {
	MyField string `json:"my_field"`
}
```

The PDK automatically parses and validates configuration based on your manifest schema and constructor signature.

2. Define your component in accordance with the appropriate interface

```go
func newMyProcessor(cfg *config, mgr *service.Resources) (service.BatchProcessor, error) {
	return &myProc{field: cfg.MyField}, nil
}
```

A component should then be defined and registered to the plugin environment 

```go
func init() {
	if err := plugin.RegisterBatchProcessor(newMyProcessor); err != nil {
		panic(err)
	}
}
```

Next, we need to register


### Compiling

Compile your plugin to WebAssembly using either [TinyGo][tinygo] or Go:
```sh
# Using TinyGo (recommended)
GOOS="wasip1" GOARCH="wasm" tinygo build -tags=wasm -buildmode=c-shared -o ./plugin/plugin.wasm ./main.go

# Using Go
GOOS="wasip1" GOARCH="wasm" go build -tags=wasm -buildmode=c-shared -o ./plugin/plugin.wasm ./main.go
```

## Loading Plugins

Start up Bento with the `--plugin/-p` flag to specify one or more plugin directories:
```sh
bento -c config.yaml -p ./reverse/plugin -p ./flip/plugin
```

Each plugin directory should contain both `plugin.yaml` and `plugin.wasm` files. Plugins are loaded at startup and become available as processors in your pipeline configuration:
```yml
input:
  resource: ingest_data
  processors:
    - reverse: {}  # Loaded from ./reverse/plugin

pipeline:
  processors:
    - flip: {}  # Loaded from ./flip/plugin

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

The `nlp_tokenize` processor in [`internal/impl/huggingface`][hf-tokenizer] is registered this way, wrapping the Hugging Face [tokenizers](https://github.com/huggingface/tokenizers) library as a plugin.

For a complete example of building a plugin, see the [Reverse Processor Example][examples].

[pdk]: https://github.com/warpstreamlabs/bento/public/wasm/service
[wasm-examples]: https://github.com/warpstreamlabs/bento/public/wasm/examples
[fields]: /docs/guides/plugins/fields
[tinygo]: https://tinygo.org/getting-started/install/
[examples]: /docs/guides/plugins/examples
[templates]: /docs/configuration/templating
[hf-tokenizer]: https://github.com/warpstreamlabs/bento/tree/main/internal/impl/huggingface
