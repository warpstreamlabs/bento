---
slug: bluesky-jetstream
title: Consume Bluesky Jetstream
description: Stream real-time Bluesky events with filters, cursor resumption, and high volume tuning.
---

Bluesky Jetstream is a JSON firehose for the AT Protocol, which makes it a practical source for real-time posts, likes, and follows. This cookbook shows how to consume Jetstream, keep a resume cursor, and tune for high volume traffic. For background, see the [Jetstream overview][jetstream-docs].

## Connect to Jetstream

Start with a basic config that consumes only posts and prints them to stdout. You can send the stream to any [output][outputs].

```yaml
input:
  bluesky_jetstream:
    collections:
      - app.bsky.feed.post

output:
  stdout:
    codec: lines
```

Jetstream emits multiple event types. Posts are `commit` events with the record payload under `commit.record`, while identity and account events have different shapes. A simple [Bloblang mapping][bloblang] is often enough to normalize this into a friendly schema.

## Resume with a Cursor

To resume from the last processed event on restart, store the cursor in a [cache resource][caches]. A local file cache is good for development:

```yaml
input:
  bluesky_jetstream:
    cache: jetstream_cursor
    cursor_commit_interval: 1s

cache_resources:
  - label: jetstream_cursor
    file:
      directory: /tmp/bento-bluesky
```

When a cache is configured, the input stores the most recent `time_us` value that has been acknowledged. On restart, it resumes from that timestamp with a small safety buffer.

## Shape Events for Downstream Use

This example keeps only commit events and normalizes the fields we care about:

```yaml
pipeline:
  processors:
    - switch:
        - check: this.kind == "commit"
          processors:
            - bloblang: |
                root.did = this.did
                root.time_us = this.time_us
                root.collection = this.commit.collection
                root.operation = this.commit.operation
                root.rkey = this.commit.rkey
                root.text = this.commit.record.text | null
        - processors:
            - bloblang: 'root = deleted()'
```

The input also adds metadata keys like `bluesky_did`, `bluesky_kind`, and `bluesky_collection`. You can use these in routing or output fields with [metadata interpolation][metadata].

## Filter by Collections and DIDs

If you only care about certain activity types or authors, filter directly at the input. This reduces bandwidth and downstream load.

```yaml
input:
  bluesky_jetstream:
    collections:
      - app.bsky.feed.post
      - app.bsky.feed.like
    dids:
      - did:plc:example1
      - did:plc:example2
```

Leave `collections` and `dids` empty to receive all events.

## Route by Collection to Kafka

Use metadata to route different collections to separate topics. Here we write posts and likes to their own topics and send everything else to a catch-all.

```yaml
pipeline:
  processors:
    - switch:
        - check: metadata("bluesky_collection") == "app.bsky.feed.post"
          processors:
            - bloblang: 'meta output_topic = "bsky_posts"'
        - check: metadata("bluesky_collection") == "app.bsky.feed.like"
          processors:
            - bloblang: 'meta output_topic = "bsky_likes"'
        - processors:
            - bloblang: 'meta output_topic = "bsky_other"'

output:
  kafka_franz:
    seed_brokers: [ TODO ]
    topic: '${! metadata("output_topic") }'
```

See the [`kafka_franz` output][kafka_franz_output] docs for batching and delivery tuning.

## Key by DID for Ordering

If you need per-author ordering downstream, set the Kafka message key to the Bluesky DID. Kafka will preserve order per key within a partition.

```yaml
output:
  kafka_franz:
    seed_brokers: [ TODO ]
    topic: bsky_events
    key: '${! metadata("bluesky_did") }'
    partitioner: murmur2_hash
```

## Emit Per-Collection Metrics

Use the [`metric` processor][metric_processor] to emit counters for each collection. The default metrics exporter exposes a Prometheus endpoint, or configure a different sink via the `metrics` block in the [metrics docs][metrics_about].

```yaml
pipeline:
  processors:
    - metric:
        name: bluesky_events
        type: counter
        labels:
          kind: ${! metadata("bluesky_kind") }
          collection: ${! metadata("bluesky_collection").or("unknown") }

metrics:
  statsd:
    address: localhost:8125
    flush_period: 100ms
```

## Custom Endpoints

You can point the input at a Jetstream-compatible Personal Data Server (PDS):

```yaml
input:
  bluesky_jetstream:
    endpoint: wss://my-pds.example.com/subscribe
```

## High Volume Tuning

For large workloads, enable compression and increase internal buffering. You can also batch cursor commits to reduce cache writes:

```yaml
input:
  bluesky_jetstream:
    compress: true
    buffer_size: 4096
    max_in_flight: 2048
    cursor_commit_interval: 2s
    ping_interval: 30s
    read_timeout: 60s
    read_limit_bytes: 1048576
    endpoints:
      - wss://jetstream1.us-east.bsky.network/subscribe
      - wss://jetstream2.us-east.bsky.network/subscribe
```

If you leave `endpoints` empty, Bento uses the public Jetstream pool with automatic failover. Set `read_limit_bytes` to protect against unexpectedly large frames, or keep it at `0` to disable the limit.

## Full Config

The full example config for this cookbook lives at [config/examples/bluesky_jetstream.yaml][full-config].

[jetstream-docs]: https://docs.bsky.app/blog/jetstream
[bloblang]: /docs/guides/bloblang/about
[metadata]: /docs/configuration/metadata
[caches]: /docs/components/caches/about
[outputs]: /docs/components/outputs/about
[kafka_franz_output]: /docs/components/outputs/kafka_franz
[metric_processor]: /docs/components/processors/metric
[metrics_about]: /docs/components/metrics/about
[full-config]: https://github.com/warpstreamlabs/bento/blob/main/config/examples/bluesky_jetstream.yaml
