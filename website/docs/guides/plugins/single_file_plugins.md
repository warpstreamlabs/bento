---
title: Single File Plugins
sidebar_label: Single File Plugins
---

:::caution EXPERIMENTAL
Plugins are an experimental feature. The manifest, the Go PDK, and the ABI may change outside of major version releases.
:::

## Overview

Bento supports the loading of plugins from a **Bento Plugin Zip** (`.bpz`) file as an alternative to directory based loading.

Each BPZ file is a zip archive containing a manifest as well as a plugin WASM file.

```
> unzip -l uppercase.bpz
Archive:  uppercase.bpz
  Length      Date    Time    Name
---------  ---------- -----   ----
      512  2026-11-10 14:26   plugin.yaml
  4903179  2026-11-10 20:13   plugin.wasm
---------                     -------
  4903691                     2 files
```

To create one, simply zip a folder containing the manifest and your wasm

## Why use a custom format?

- The main purpose behind using the `.bpz` extension is to make it identifiable at a glance that a file is a bento plugin and not just a random zip file
- The file is Zip based as it gives compression (which is good), while also allowing reading specific files, whereas a tar.gz can require decompressing the whole archive to access any specific file
- This is specifically relevant, because in the manifest `runtime.wasm.path` can be used to change the binary's path, meaning that we can't assume it'll be `plugin.wasm`
