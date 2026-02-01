---
title: Manifest Fields
sidebar_label: Fields
---

The `plugin.yaml` manifest defines your plugin's configuration schema and runtime behavior.

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
| `string` | standard string type |
| `int` | standard integer type |
| `float` | standard float type |
| `bool` | a boolean true/false |
| `unknown` | allows for nesting arbitrary configuration inside of a field |


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

### `runtime`

The runtime configuration for the plugin.


Type: `object`  

### `runtime.wasm`

The WASM runtime configuration.


Type: `object`  

### `runtime.wasm.path`

The path to the WASM binary relative to the plugin directory.


Type: `string`  
Default: `"plugin.wasm"`  

### `runtime.wasm.env`

Environment variables to pass to the plugin.


Type: map of `string`  

### `runtime.wasm.mounts`

A list of directory mounts.


Type: list of `object`  

### `runtime.wasm.mounts[].host_path`

The path on the host machine to mount.


Type: `string`  

### `runtime.wasm.mounts[].guest_path`

The path inside the WASM container.


Type: `string`  

### `runtime.wasm.allowed_hosts`

A list of hosts that the plugin is allowed to connect to.


Type: list of `string`  

### `runtime.wasm.config`

Arbitrary key-value configuration for the plugin.


Type: map of `string`  

### `runtime.wasm.memory`

Describes the limits on the memory the plugin may be allocated.


Type: `object`  

### `runtime.wasm.memory.max_pages`

The max amount of pages the plugin can allocate. One page is 64KiB.


Type: `int`  

### `runtime.wasm.memory.max_http_response_bytes`

The max size of an Extism HTTP response (e.g., '10MB').


Type: `string`  

### `runtime.wasm.memory.max_var_bytes`

The max size of all Extism vars (e.g., '1MB').


Type: `string`  

