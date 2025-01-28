Generate an authentication token by logging onto the web console at
[s2.dev](https://s2.dev/dashboard).

### Metadata

The metadata attributes are set as S2 record headers. Currently, only string
attribute values are supported.

### Batching

The plugin expects batched inputs. Messages are batched automatically by Bento.

By default, Bento disables batching based on `count`, `byte_size`, and `period`
parameters, but the plugin enables batching setting both `count` and
`byte_size` to the maximum values supported by S2 (`1000` and `1MiB`). It also
sets a flush period of `5ms` as a reasonable default.
