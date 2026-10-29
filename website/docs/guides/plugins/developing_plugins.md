---
title: Developing Plugins
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Build, load, and test an `uppercase` processor with the [Go plugin development kit (PDK)][pdk]. It converts each message to uppercase and adds a configurable prefix.

## 1. Create the Project

Create a directory with a Go module, and a `plugin` directory that will hold the finished plugin:

```sh
mkdir -p uppercase/plugin
cd uppercase
go mod init example.com/uppercase
go get github.com/warpstreamlabs/bento/public/wasm/service/processor
```

Run every following command from the `uppercase` directory.

## 2. Write the Manifest

Create `plugin/plugin.yaml`. The `name` is what pipelines call the processor, and each entry in `fields` is a field of its config:

```yml title="plugin/plugin.yaml"
name: uppercase
type: processor
summary: Uppercases each message and prepends a configurable prefix.

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

## 3. Write the Processor

Create `main.go`:

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
	msg.MetaSet("processed_by", "uppercase")
	return service.MessageBatch{msg}, nil
}

func main() {}
```

In this example:

- The JSON tag maps `config.Prefix` to the manifest's `prefix` field. The PDK decodes the config before calling the constructor.
- `init()` registers the processor. Go requires `main()`, but Bento never calls it.
- `mgr` is an empty `*service.Resources` value; the WASM PDK does not expose Bento resources yet.
- `-buildmode=c-shared` exports `_initialize` instead of `_start`. Bento calls `_initialize` for each instance, which initializes the Go runtime and runs `init()`.

## 4. Compile the Plugin

Compile the plugin into the `plugin` directory, next to its manifest:

<Tabs groupId="go-compiler">
<TabItem value="go" label="Go">

```sh
GOOS="wasip1" GOARCH="wasm" go build -buildmode=c-shared -o ./plugin/plugin.wasm .
```

</TabItem>
<TabItem value="tinygo" label="TinyGo">

```sh
GOOS="wasip1" GOARCH="wasm" tinygo build -scheduler=none -buildmode=c-shared -o ./plugin/plugin.wasm .
```

TinyGo can produce smaller WASM modules. Use a [TinyGo version compatible with Bento's Go version][tinygo-compat]. The `-scheduler=none` flag disables goroutines and channels; use it only if your plugin needs neither. Follow TinyGo's [installation instructions][tinygo-install] for your platform.

</TabItem>
</Tabs>

The `plugin` directory now holds `plugin.yaml` and `plugin.wasm`, which together are the plugin.

## 5. Run the Plugin

Create `config.yaml`, which reads lines from standard input, runs them through the plugin, and prints the result:

```yml title="config.yaml"
input:
  stdin: {}

pipeline:
  processors:
    - uppercase:
        prefix: "shout: "

output:
  stdout: {}
```

Run Bento, loading the plugin directory with `-p`:

```sh
printf 'hello\n' | bento -c config.yaml -p ./plugin
```

Among Bento's log lines, you should see:

```
shout: HELLO
```

## 6. Test the Plugin

Create `config_bento_test.yaml` next to the config. This [unit test][unit-testing] runs only the first processor, the plugin, and checks both the content and the metadata of the message it returns:

```yml title="config_bento_test.yaml"
tests:
  - name: uppercases each message after the prefix
    target_processors: /pipeline/processors/0
    input_batch:
      - content: hello
    output_batches:
      - - content_equals: "shout: HELLO"
          metadata_equals:
            processed_by: uppercase
```

Run the test, loading the plugin in the same way:

```sh
bento -p ./plugin test config.yaml
```

You should see:

```
Test 'config.yaml' succeeded
```

## Next Steps

- [Go PDK reference][pdk]: every type and function of the PDK. To process whole batches at once, implement a `BatchProcessor` and register it with [`batch_processor`][batch-pdk], as the [`reverse`][reverse-example] example does.
- [Plugins][about]: load the plugin into Bento elsewhere.
- [Architecture][architecture]: how instances of the plugin run, and how its errors affect messages.

[tinygo-compat]: https://tinygo.org/docs/reference/go-compat-matrix/
[tinygo-install]: https://tinygo.org/getting-started/install/
[unit-testing]: /docs/configuration/unit_testing
[about]: /docs/guides/plugins/about
[architecture]: /docs/guides/plugins/architecture
[pdk]: https://pkg.go.dev/github.com/warpstreamlabs/bento/public/wasm/service
[batch-pdk]: https://pkg.go.dev/github.com/warpstreamlabs/bento/public/wasm/service/batch_processor
[reverse-example]: https://github.com/warpstreamlabs/bento/tree/main/public/wasm/examples/reverse
