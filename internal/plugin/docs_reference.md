---
title: Manifest Fields
sidebar_label: Fields
---

The `plugin.yaml` manifest defines your plugin's configuration schema and runtime behavior. Only `name` and `type` are required, along with the `name` and `type` of each field, and the `host_path` and `guest_path` of each mount.

### `plugin.yaml`

{{template "field_docs" . -}}
