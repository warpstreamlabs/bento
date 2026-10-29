---
title: Manifest Fields
sidebar_label: Fields
---

The `plugin.yaml` manifest defines a plugin's name, its config fields, and its sandbox limits. Only `name` and `type` are required at the root level. If you define fields, each needs a `name` and `type`; if you define mounts, each needs a `host_path` and `guest_path`.

## Example

```yml
name: uppercase
type: processor
status: experimental
summary: Uppercases each message and prepends a configurable prefix.

fields:
  - name: prefix
    description: Added before the content of each message.
    type: string
    default: ""

runtime:
  wasm:
    path: plugin.wasm
    mounts:
      - host_path: ./data
        guest_path: /data
    memory:
      max_pages: 128
```

## Fields

{{template "field_docs" . -}}
