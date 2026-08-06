---
title: Getting Started
---

## Writing Plugins

Bento provides a [plugin development kit (PDK)][pdk] that should be used when writing plugins. This was designed to resemble how traditional bento components are written, handling much of the drama behind-the-scenes of plugin registration and execution.

For now, only Go is supported by a PDK, with future versions providing better multi-language support.

### Structure

A plugin requires three key components:

1. **Plugin manifest** (`plugin.yaml`) - Defines metadata, config schema, and runtime settings
2. **Plugin implementation** - Go code implementing the processor interface
3. **Registration** - Hooking your implementation into the PDK via `init()`

Complete working examples are available at [wasm/examples][wasm-examples].

### Configuration

Plugins can accept configuration fields just like native Bento components. Define fields in your `plugin.yaml` manifest:
```yml
fields:
  - name: my_field
    description: An example configuration field
    type: string
    default: ""
```

Then define a config struct that matches your manifest:
```go
type config struct {
	MyField string `json:"my_field"`
}

func newMyProcessor(cfg *config, mgr *service.Resources) (service.BatchProcessor, error) {
	return &myProc{field: cfg.MyField}, nil
}
```

The PDK automatically parses and validates configuration based on your manifest schema and constructor signature.

You can see all available manifest fields in the [Manifest Reference][fields].

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

For a complete example of building a plugin, see the [Reverse Processor Example][examples].

[pdk]: https://github.com/warpstreamlabs/bento/public/wasm/service
[wasm-examples]: https://github.com/warpstreamlabs/bento/public/wasm/examples
[fields]: /docs/guides/plugins/fields
[tinygo]: https://tinygo.org/getting-started/install/
[examples]: /docs/guides/plugins/examples
