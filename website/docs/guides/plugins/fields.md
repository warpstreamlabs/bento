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

### `name`

The name of the plugin.


Type: `string`  

### `type`

The type of component this plugin creates.


Type: `string`  
Options: `processor`.

### `status`

The stability status of the plugin.


Type: `string`  
Default: `"stable"`  

| Option | Summary |
|---|---|
| `stable` | This plugin is stable and will not change in breaking ways outside of major version releases. |
| `beta` | This plugin is beta and will not change in breaking ways unless a major problem is found. |
| `experimental` | This plugin is experimental and subject to breaking changes outside of major version releases. |


### `summary`

A short summary of the plugin.


Type: `string`  
Default: `""`  

### `description`

A detailed description of the plugin and how to use it.


Type: `string`  
Default: `""`  

### `fields`

The configuration fields of the plugin.


Type: list of `object`  

### `fields[].name`

The name of the field.


Type: `string`  

### `fields[].description`

A description of the field.


Type: `string`  
Default: `""`  

### `fields[].type`

The scalar type of the field.


Type: `string`  

| Option | Summary |
|---|---|
| `string` | Standard string type. |
| `int` | Standard integer type. |
| `float` | Standard float type. |
| `bool` | Boolean value. |
| `unknown` | Allows nested arbitrary configuration inside a field. |


### `fields[].kind`

The kind of the field.


Type: `string`  
Default: `"scalar"`  
Options: `scalar`, `map`, `list`.

### `fields[].default`

An optional default value for the field. If a default value is not specified then a configuration without the field is considered incorrect.


Type: `unknown`  

### `fields[].advanced`

Whether this field is considered advanced.


Type: `bool`  
Default: `false`  

### `requires`

The capabilities the plugin needs. Whoever runs the plugin grants them in the `plugins` section of their Bento config.


Type: `object`  

### `requires.memory`

The memory the plugin needs.


Type: `object`  

### `requires.memory.min_pages`

The fewest pages of memory each instance of the plugin needs. One page is 64KiB. Bento refuses to run the plugin with a lower `max_pages` limit.


Type: `int`  

### `requires.mounts`

Directories the plugin expects to be mounted.


Type: list of `object`  

### `requires.mounts[].name`

The name the mount is granted by.


Type: `string`  

### `requires.mounts[].guest_path`

The path the plugin expects the directory at.


Type: `string`  

### `requires.mounts[].writable`

Whether the plugin needs to write to the directory.


Type: `bool`  
Default: `false`  

### `requires.mounts[].optional`

Whether the plugin can run without the mount.


Type: `bool`  
Default: `false`  

### `requires.mounts[].description`

What the plugin uses the directory for.


Type: `string`  
Default: `""`  

