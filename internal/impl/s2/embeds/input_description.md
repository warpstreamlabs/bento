Generate an authentication token by logging onto the web console at
[s2.dev](https://s2.dev/dashboard).

### Cache

The plugin requires setting up a caching mechanism to resume the input after
the last acknowledged record.

To know more about setting up a cache resource, see
[Cache docs for Bento](https://warpstreamlabs.github.io/bento/docs/components/caches/about).

### Metadata

This input adds the following metadata fields to each message in addition to the
record headers:

- `s2_stream`: The origin S2 stream.
- `s2_seq_num`: Sequence number of the record in the origin stream formatted as a string.

All the header values are loosely converted to strings as metadata attributes.

**Note:** An [S2 command record](https://s2.dev/docs/stream#command-records) has no header
name. This is set as the `s2_command` meta key.
