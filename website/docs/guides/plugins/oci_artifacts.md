---
title: OCI Artifact Format
sidebar_label: OCI Artifact Format
---

:::caution EXPERIMENTAL
Plugins are an experimental feature. The manifest, the Go PDK, the ABI, and the OCI artifact format may change outside of major version releases.
:::

This page describes how a plugin is stored as an [OCI artifact][oci-artifacts], for tools other than Bento that produce or read plugins. To build, push and pull plugins with Bento, see [Distributing Plugins][distributing].

## Manifest

A plugin is a single OCI image manifest (`application/vnd.oci.image.manifest.v1+json`, image-spec v1.1):

```json
{
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.manifest.v1+json",
  "artifactType": "application/vnd.bento.plugin.v1",
  "config": {
    "mediaType": "application/vnd.bento.plugin.config.v1+json",
    "digest": "sha256:...",
    "size": 512
  },
  "layers": [
    {
      "mediaType": "application/wasm",
      "digest": "sha256:...",
      "size": 4903179,
      "annotations": {
        "org.opencontainers.image.title": "plugin.wasm"
      }
    }
  ],
  "annotations": {
    "vnd.bento.plugin.name": "uppercase",
    "vnd.bento.plugin.type": "processor",
    "vnd.bento.plugin.abi": "1",
    "org.opencontainers.image.created": "2026-11-20T14:33:04Z"
  }
}
```

| Part | Value |
|------|-------|
| `artifactType` | `application/vnd.bento.plugin.v1` |
| `config` | The plugin's manifest as JSON, with the media type `application/vnd.bento.plugin.config.v1+json`. |
| `layers` | Exactly one layer, the WASM binary, with the media type `application/wasm`. |

The media types use the `vnd.bento` prefix so that any implementation of Bento's plugin system can use them, and are versioned so the format can change in future.

## Config

The config holds the same fields as a [`plugin.yaml`][fields], as JSON: the plugin's name, type, status, summary, description, `fields` and `requires`. It must not have the deprecated `runtime` block, and has no file paths, as the WASM binary is the only layer.

```json
{
  "name": "uppercase",
  "type": "processor",
  "summary": "Uppercases each message and prepends a configurable prefix.",
  "fields": [
    { "name": "prefix", "type": "string", "default": "" }
  ],
  "requires": {
    "mounts": [{ "name": "data", "guest_path": "/data" }]
  }
}
```

Bento reads the config before downloading the WASM binary, so it can check a plugin first.

## Annotations

| Annotation | Description |
|------------|-------------|
| `vnd.bento.plugin.name` | The plugin's name, from its config. |
| `vnd.bento.plugin.type` | The plugin's component type, from its config. |
| `vnd.bento.plugin.abi` | The [ABI version][abi-version] the WASM binary declares. |
| `org.opencontainers.image.*` | Any of the [pre-defined annotations][oci-annotations], such as `created`, `version` and `source`. |

The annotations are informational, for listing plugins without downloading them. Bento reads the name, type and ABI version from the config and the WASM binary when it loads a plugin.

## Validation

Bento refuses to load an artifact that:

- Isn't an image manifest, such as a multi-platform index. Plugins don't need one, as WASM runs on every platform.
- Has an `artifactType` or config media type other than those above.
- Doesn't have exactly one layer, of type `application/wasm`.
- Has a manifest or config larger than 4 MiB, or a WASM binary larger than 256 MiB.
- Has a config that isn't a valid plugin manifest, or uses the `runtime` block.
- Has a WASM binary built for an ABI version Bento doesn't support.

[oci-artifacts]: https://github.com/opencontainers/image-spec/blob/main/artifacts-guidance.md
[oci-annotations]: https://github.com/opencontainers/image-spec/blob/main/annotations.md#pre-defined-annotation-keys
[distributing]: /docs/guides/plugins/distributing_plugins
[fields]: /docs/guides/plugins/fields
[abi-version]: /docs/guides/plugins/plugin_abi#bento_abi_v1
