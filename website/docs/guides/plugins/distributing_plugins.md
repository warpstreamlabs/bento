---
title: Distributing Plugins
sidebar_label: Distributing Plugins
---

:::caution EXPERIMENTAL
Plugins are an experimental feature. The manifest, the Go PDK, the ABI, and the OCI artifact format may change outside of major version releases.
:::

Plugins can be shared through any container registry that supports OCI artifacts, such as GitHub Container Registry (GHCR), Docker Hub, Amazon ECR or Harbor. They're stored as [OCI artifacts][format], so they get the same versioning, access control, mirroring and signing as container images.

Bento does everything itself with the `bento plugin` commands, which work like their `docker` equivalents. No other tools are needed.

## Build, Push and Run

Start with a plugin directory, with a `plugin.yaml` and a compiled `plugin.wasm` (see [Developing Plugins][developing]).

Build it into the local plugin store, tagged with where it will be pushed:

```sh
bento plugin build -t ghcr.io/me/uppercase:1.0.0 ./uppercase
```

`build` checks the manifest and the plugin's [ABI version][abi-version] and packages them. It doesn't compile the WASM, so build that first with your usual tools. Add `--annotation org.opencontainers.image.version=1.0.0` (or any other annotation) to describe the plugin, and set `SOURCE_DATE_EPOCH` for reproducible builds.

Log in to the registry and push:

```sh
echo $GITHUB_TOKEN | bento plugin login ghcr.io -u me --password-stdin
bento plugin push ghcr.io/me/uppercase:1.0.0
```

`bento plugin login` saves credentials in the same place as `docker login`, so either can be used.

To run the plugin, list it in the `plugins` section of the config with an `oci://` source, along with anything it's [granted][granting]:

```yml
plugins:
  - source: oci://ghcr.io/me/uppercase:1.0.0
    memory:
      max_pages: 128

pipeline:
  processors:
    - uppercase:
        prefix: ">> "
```

Bento pulls the plugin into its local store the first time it's used. It can also be passed with `-p oci://ghcr.io/me/uppercase:1.0.0`.

## Pinning Versions

Tags can be moved to a different plugin. Reference a plugin by its digest to always get exactly the same plugin:

```yml
plugins:
  - source: oci://ghcr.io/me/uppercase@sha256:b165ec49dcb2a03e55b5e1790915135a36e7647ca976acd5d76b3f2286419208
```

A plugin referenced by digest is never downloaded again once it's in the local store, so it loads even when the registry can't be reached. `bento plugin ls` and `bento plugin inspect` show digests.

## The Local Store

Bento keeps plugins in a local store, like Docker's image store. It's a standard [OCI image layout][oci-layout] directory, in `bento/plugins` within the user cache directory (such as `~/.cache/bento/plugins`) by default. Set `--plugin-store` or `BENTO_PLUGIN_STORE` to use another directory. The Bento container image sets `BENTO_PLUGIN_STORE` to `/plugins`.

When Bento pulls `oci://` plugins is set with `--plugin-pull`, as with `docker run --pull`:

| Value | Behavior |
|-------|----------|
| `missing` (default) | Pull only plugins that aren't in the local store. |
| `always` | Pull every time Bento starts. Plugins referenced by digest aren't pulled again, as their content can't change. |
| `never` | Only use plugins already in the local store. |

Manage the store with:

| Command | Docker equivalent | Description |
|---------|-------------------|-------------|
| `bento plugin build -t REF DIR` | `docker build` | Build a plugin directory into the store. |
| `bento plugin push REF` | `docker push` | Push a plugin to its registry. |
| `bento plugin pull REF` | `docker pull` | Pull a plugin from its registry. |
| `bento plugin ls` | `docker image ls` | List plugins in the store. |
| `bento plugin inspect REF...` | `docker inspect` | Show a plugin's manifest, annotations and digest, as JSON. |
| `bento plugin tag SRC DST` | `docker tag` | Give a plugin another reference. |
| `bento plugin rm REF...` | `docker image rm` | Remove references, and the plugins no reference uses. |
| `bento plugin prune` | `docker image prune` | Remove untagged plugins and unused content. |
| `bento plugin save -o FILE REF...` | `docker save` | Save plugins to a tar archive. |
| `bento plugin load -i FILE` | `docker load` | Load plugins from a tar archive. |
| `bento plugin login REGISTRY` | `docker login` | Save registry credentials. |
| `bento plugin logout REGISTRY` | `docker logout` | Remove registry credentials. |

References need a registry, such as `ghcr.io/me/uppercase:1.0.0`, as there's no default registry for plugins. A reference without a tag gets the tag `latest`. Registries on `localhost` are reached over plain HTTP.

## Sharing Without a Registry

`bento plugin save` writes plugins to a single file, which can be copied to another machine, such as one without network access, and loaded there:

```sh
bento plugin save -o uppercase.tar ghcr.io/me/uppercase:1.0.0
# On another machine:
bento plugin load -i uppercase.tar
bento --plugin-pull=never -c config.yaml
```

The file is a tar archive of an OCI image layout, so other OCI tools can read it too.

## Docker Compose

The Bento image keeps its plugin store in `/plugins`. Mount a volume there, so plugins are only pulled once rather than every time the container is created:

```yml
services:
  bento:
    image: ghcr.io/warpstreamlabs/bento
    command: ["-c", "/bento.yaml"]
    volumes:
      - ./bento.yaml:/bento.yaml:ro
      - plugins:/plugins
      # For private registries, the credentials saved by docker login:
      - ~/.docker/config.json:/docker/config.json:ro
    environment:
      DOCKER_CONFIG: /docker

volumes:
  plugins:
```

## Kubernetes

Pull plugins in an init container, so the pipeline container never needs registry access or credentials, and starts with the plugins already in its store. The Bento image keeps its plugin store in `/plugins`, so share a volume there:

```yml
apiVersion: v1
kind: Pod
metadata:
  name: bento
spec:
  initContainers:
    - name: pull-plugins
      image: ghcr.io/warpstreamlabs/bento
      args: ["plugin", "pull", "ghcr.io/me/uppercase:1.0.0"]
      env:
        # For private registries, a docker config.json secret:
        - name: DOCKER_CONFIG
          value: /docker
      volumeMounts:
        - { name: plugins, mountPath: /plugins }
        - { name: registry-auth, mountPath: /docker, readOnly: true }
  containers:
    - name: bento
      image: ghcr.io/warpstreamlabs/bento
      args: ["--plugin-pull=never", "-c", "/config/bento.yaml"]
      volumeMounts:
        - { name: plugins, mountPath: /plugins, readOnly: true }
        - { name: config, mountPath: /config }
  volumes:
    - name: plugins
      emptyDir: {}
    - name: registry-auth
      secret:
        secretName: registry-auth
        items: [{ key: .dockerconfigjson, path: config.json }]
    - name: config
      configMap:
        name: bento-config
```

Reference plugins by digest in production, so every pod runs exactly the same plugin.

## Signing

Plugins are standard OCI artifacts, so they can be signed and verified with [cosign][cosign], the same as container images:

```sh
cosign sign ghcr.io/me/uppercase@sha256:...
cosign verify ghcr.io/me/uppercase@sha256:... --certificate-identity ... --certificate-oidc-issuer ...
```

Bento doesn't verify signatures itself yet, so verify them before pulling, such as in CI or with a registry or admission policy.

[format]: /docs/guides/plugins/oci_artifacts
[developing]: /docs/guides/plugins/developing_plugins
[abi-version]: /docs/guides/plugins/plugin_abi#bento_abi_v1
[granting]: /docs/guides/plugins/about#granting-mounts-and-memory
[oci-layout]: https://github.com/opencontainers/image-spec/blob/main/image-layout.md
[cosign]: https://github.com/sigstore/cosign
