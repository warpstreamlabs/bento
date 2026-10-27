---
title: Loading Plugins
---

This guide covers loading plugins into Bento, testing them, and how they behave in a pipeline. See the [Overview][overview] for what a plugin is.

Start up Bento with the `--plugins/-p` flag to specify one or more plugin directories:
```sh
bento -c config.yaml -p ./reverse/plugin -p ./uppercase/plugin
```

Each plugin directory should contain both `plugin.yaml` and `plugin.wasm` files. Plugins are loaded at startup and become available as processors in your pipeline configuration:
```yml
input:
  stdin: {}

pipeline:
  processors:
    - reverse: {}    # Loaded from ./reverse/plugin
    - uppercase: {}  # Loaded from ./uppercase/plugin

output:
  stdout: {}
```

## Testing Plugins

Plugins are tested with Bento's [unit tests][unit-testing], loading them with the same flag:

```sh
bento -p ./uppercase/plugin test config.yaml
```

## Runtime Behavior

- Bento creates an instance of the plugin for each call running at once, such as one per pipeline thread. Each instance is created with the processor's config and keeps its own globals, so successive calls may see different state.
- An error from the constructor, such as for an invalid config, fails the call the instance was created for, and its messages are marked with that error.
- A failed message continues through the pipeline marked with its error, and Bento doesn't exit with an error. Handle failed messages with [error handling][error-handling], and check them with [unit tests][unit-testing].

See [How Plugins Run][how-plugins-run] for details.

## Plugin Security

Plugins run in a WebAssembly sandbox, without network access, and with access to the host's filesystem only through the directories mounted with `runtime.wasm.mounts`. Symbolic links within a mounted directory are followed, even when they point outside of it, and a plugin can create such links itself in a writable mount. Only mount directories that the plugin may access in full. Mounts are read-only unless they set `read_only: false`, which should be avoided where possible.

Only load plugins from sources you trust, and check the mounts of their manifest before you do.

## Registering from Go

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

[overview]: /docs/guides/plugins/about
[unit-testing]: /docs/configuration/unit_testing
[error-handling]: /docs/configuration/error_handling
[how-plugins-run]: /docs/guides/plugins/abi#how-plugins-run
[templates]: /docs/configuration/templating
