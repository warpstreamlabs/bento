---
title: Manifest Fields
sidebar_label: Fields
---

The `plugin.yaml` manifest defines a plugin's name, its config fields, and the limits of the sandbox it runs in. Only `name` and `type` are required, along with the `name` and `type` of each field, and the `host_path` and `guest_path` of each mount.

## Example

```yml
name: uppercase
type: processor
status: experimental
summary: Uppercases each message, after a prefix.

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
