---
title: Plugins
sidebar_label: About
---

Extending Bento typically requires recompiling the binary and maintaining custom forks. Bento's WebAssembly (WASM) plugin system solves this by allowing you to define custom processor components as standalone modules that load at runtime -- no recompilation required!

A Bento plugin consists of two files within a dedicated directory:

```
my-plugin/
├── plugin.yaml  # Plugin manifest (required)
└── plugin.wasm  # Compiled WASM binary (required)
```

## Quick Links

- [Writing and Loading Plugins][getting-started] - Learn how to create and load plugins
- [Example: Reverse Processor][examples] - Complete walkthrough of building a plugin
- [Manifest Reference][fields] - Full configuration schema documentation

## Language Support

For now, only Go is supported by a PDK, with future versions providing better multi-language support.

[getting-started]: /docs/guides/plugins/getting_started
[examples]: /docs/guides/plugins/examples
[fields]: /docs/guides/plugins/fields
