---
title: Examples
---

All examples are are available at [wasm/examples](github.com/warpstreamlabs/public/wasm/examples).

## Example: Reverse Processor

Now, let's create a simple processor using the PDK that reverses a batch of messages and adds some metadata about the original position.

The full example is available at [wasm/examples/reverse][reverse-example], but the steps for creation are outlined below.

1. Create a `reverse` directory with a `main.go` and a `plugin` subdirectory.

```
reverse/
├── main.go  			# Plugin implementation in Go
└── plugin/plugin.yaml  # Plugin manifest
```

Then add a `plugin/plugin.yaml` with the following manifest:

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
      max_pages: 128 # Assigns ~8MiB for the plugin (64KiB per page)
```

2. Import the relevant packages from `github.com/warpstreamlabs/bento/public/wasm`. As we want to process a batch of messages, we'll need to use the `batch_processor` package for registration.


```go
import (
	"github.com/warpstreamlabs/bento/public/wasm/service"
	plugin "github.com/warpstreamlabs/bento/public/wasm/service/batch_processor"
)
```

3. Write your plugin implementation, ensuring it adheres to the `BatchProcessor` interface.

```go
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
```

For convenience sake, let's also add a simple constructor function for the `reverseProc`. Note, that the plugin does not require any config or resources, yet we'll still have to add these to satisfy the registration function pattern.

```go
func newReverseProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	return &reverseProc{}, nil
}
```

4. Ensure the component is registered at runtime using the `init` function.

```go
func init() {
	plugin.RegisterBatchProcessor(newReverseProcessor)
}
```

Lastly, we'll need to define a no-op `main` function to satisfy the WASM compiler.

```go
func main() {}
```

Now, you can compile your plugin to a WASM executable using `TinyGo`.

```sh
GOOS="wasip1" GOARCH="wasm" tinygo build -tags=wasm -buildmode=c-shared -o ./reverse/plugin/plugin.wasm ./reverse/main.go
```

Your final directory should look like this:

```
reverse/
├── main.go
└── plugin/
	├── plugin.yaml
	└── plugin.wasm
```

Configure a simple `config.yaml` to use the new `reverse` processor

```yml
pipeline:
  processors:
    - reverse: {}
```

And use the `--plugin/-p` flag to specify a plugin directory:

```sh
bento -c config.yaml -p ./reverse/plugin
```

For more information on manifest configuration options, see the [Manifest Reference][fields].

[reverse-example]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples/reverse
[fields]: /docs/guides/plugins/fields
