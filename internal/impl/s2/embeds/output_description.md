Generate an authentication token by logging onto the web console at
[s2.dev](https://s2.dev/dashboard).

### Metadata

The metadata attributes are set as S2 record headers. Currently, only string
attribute values are supported.

### Batching

The plugin expects batched inputs. Messages are batched automatically by Bento.

By default, Bento disables batching based on `count`, `byte_size`, and `period`
parameters, but the plugin enables batching setting both `count` and
`byte_size` to the maximum values supported by S2. It also
sets a flush period of `5ms` as a reasonable default.

**Note:** An S2 record batch can be a maximum of 1MiB but the plugin limits the
size of a message to 256KiB since the Bento size limit doesn't take metadata into
account. Moreover, the metered size of the same Bento message will be greater
than the byte size of a Bento message.
