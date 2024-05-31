TinyGo Bento WASM Module
==========================

This example builds a Bento plugin as a WASM module written in Go and can be compiled using [TinyGo][tinygo] with the following command:

```sh
tinygo build -scheduler=none -target=wasi -o uppercase.wasm .
```

You can then run the compiled module using the [`wasm` processor][processor.wasm], configured like so:

```yaml
pipeline:
  processors:
    - wasm:
        module_path: ./uppercase.wasm
```

[TinyGo]: https://tinygo.org/
[processor.wasm]: https://warpstreamlabs.github.io/bento/docs/components/processors/wasm
