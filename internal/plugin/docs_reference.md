---
title: Manifest Fields
sidebar_label: Fields
---

The `plugin.yaml` manifest describes a plugin: its name, its config fields, and the capabilities it `requires`. Only `name` and `type` are required at the root level. If you define fields, each needs a `name` and `type`; if you request mounts, each needs a `name` and `guest_path`.

The manifest only contains what is fixed when the plugin is built. Whoever runs the plugin decides which host directories it gets and how much memory it may use, in the [`plugins` section][plugins-config] of their Bento config.

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

requires:
  memory:
    min_pages: 64
  mounts:
    - name: data
      guest_path: /data
      description: Lookup tables read at startup.
```

And the Bento config that runs it, granting the `data` mount:

```yml
plugins:
  - source: ./uppercase
    memory:
      max_pages: 128
    mounts:
      data:
        host_path: ./data
```

The `runtime` block is deprecated. See [Migrating from the `runtime` block][migrating].

[plugins-config]: /docs/guides/plugins/about#loading-plugins
[migrating]: /docs/guides/plugins/about#migrating-from-the-runtime-block

## Fields

{{template "field_docs" . -}}
