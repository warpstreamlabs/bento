---
slug: kafka-mirroring
title: Kafka Topic Mirroring
description: Learn how to mirror Kafka topics while preserving partition mapping.
---

Kafka-flavoured Bento (カフカ風弁当; Kafuka-fū Bentō), a favourite here at WarpStream Labs, is a quick-and-easy recipe you can whip up in minutes. This cookbook will illustrate how to use Bento for consuming and publishing events to Kafka, with the goal of **mirroring Kafka topics while preserving partition mappings**.

For example, the diagram below shows *partition preservation* of some process where `bento` consumes an event from `Partition 2` of `Topic A` and maps it to `Partition 2` of `Topic B`:
```
  Topic A                                                                  Topic B                       
+-----------------------------+                                          +-----------------------------+
|                             |                                          |                             |
|    +--------------------+   |                                          |   +--------------------+    |
| P1 |                    |   |                                          |   |                    | P1 |
|    +--------------------+   |                                          |   +--------------------+    |
|                             |       +--------------------------+       |                             |
|    +--------------------+   |       |                          |       |   +--------------------+    |
| P2 |                    |---------->|          bento           |---------->|                    | P2 |
|    +--------------------+   |       |                          |       |   +--------------------+    |
|                             |       +--------------------------+       |                             |
|    +--------------------+   |                                          |   +--------------------+    |
| P3 |                    |   |                                          |   |                    | P3 |
|    +--------------------+   |                                          |   +--------------------+    |
|                             |                                          |                             |
+-----------------------------+                                          +-----------------------------+
```

## Consuming Events

To start consuming data, we can use the [`kafka_franz input`][kafka.input] component. Here, we will read in all new events from the `foo` and `bar` topics.

```yaml
input:
  kafka_franz:
    consumer_group: bento_bridge_consumer
    seed_brokers: [ TODO ]
    topics: [ foo, bar ]
```

## Publishing Events

We can use the [`kafka_franz output`][kafka.output] component for publishing messages to a topic. As you'll see, this component is incredibly flexible, with several fields supporting [string interpolation][bloblang.interpolation] for dynamic value setting.

Let's route all events received from `foo` and `bar` to some existing topics named `output-foo` and `output-bar`, respectively. 

Fortunately, Bento makes this straightforward as the [`kafka_franz input`][kafka.input] component attaches useful [metadata][kafka.input.metadata] to each message, including the source event's `kafka_key`, `kafka_topic`, and `kafka_partition`.

Using [string interpolation][bloblang.interpolation], we can then extract the original topic name from the `kafka_topic` metadata field, prepend the `output-` prefix, and pass this as output to the `topic` field -- dynamically setting the topic destinations.

```yaml
output:
  kafka_franz:
    seed_brokers: [ TODO ]
    topic: 'output-${! metadata("kafka_topic") }'
```

Recall from earlier that we also wanted to preserve our partition mapping when writing to new topics. Again, we can use metadata to retrieve the original partition of each message in the source topic. We'll use the `kafka_partition` metadata field in conjunction with setting `partitioner` to `manual` -- overriding any other fancy partitioning algorithm in favour of preserving our initial mapping. Combining again with [string interpolation][bloblang.interpolation], we get the following:

```yaml
output:
  kafka_franz:
    seed_brokers: [ TODO ]
    topic: 'output-${! metadata("kafka_topic") }'
    partition: ${! metadata("kafka_partition") }
    partitioner: manual
```

Voilà! The above config:
- Consumes events from the `foo` and `bar` topics
- Routes the output destination of events from `foo` to `output-foo` and from `bar` to `output-bar` using the `kafka_topic` metadata field
- Explicitly sets the message partition to that of the source message using the metadata field `kafka_partition`

For completeness, we can also route all consumed events back to their original source topic and partition.

```yaml
output:
  kafka_franz:
    seed_brokers: [ TODO ]
    topic: ${! metadata("kafka_topic") }
    partition: ${! metadata("kafka_partition") }
    partitioner: manual
```

## Regular Expression Matching

We begin by consuming from 2 topics: `foobar` and `foobaz`.

```yaml
input:
  kafka_franz:
    consumer_group: bento_bridge_consumer
    seed_brokers: [ TODO ]
    topics: [ foobar, foobaz ]
```

Notice that both topics share a common prefix of `foo`. It's easy to imagine a large or variable amount of topics needing to be consumed by the input. Luckily, we have tools for that as the [`kafka_franz input`][kafka.input] also has [regular expression][kafka.input.regex] matching capabilities.

Include your topics pattern as regex and include `regexp_topics: true` so that listed topics are interpreted as regex. 

```yaml
input:
  kafka_franz:
    consumer_group: bento_bridge_consumer
    seed_brokers: [ TODO ]
    topics: [ foo.* ]
    regexp_topics: true
```

Now Bento will consume events from all topics with the prefix `foo`.

## Final Words

Wow, you're a natural, aren't you? 

In this cookbook, we've explored how to use Bento to mirror Kafka topics while preserving partition mappings. We've covered:

- Consuming events from Kafka topics
- Publishing events to dynamically determined topics
- Preserving partition information when writing to new topics
- Regular expressions for matching and consuming from many topics

If you have any more questions, come [join our Discord!][discord-link]

Otherwise, happy streaming!


[bloblang.interpolation]: /docs/configuration/interpolation/#bloblang-queries
[discord-link]: https://console.warpstream.com/socials/discord
[kafka.input]: /docs/components/inputs/kafka_franz
[kafka.input.metadata]: /docs/components/inputs/kafka_franz/#metadata
[kafka.input.regex]: /docs/components/inputs/kafka_franz#regexp_topics
[kafka.output]: /docs/components/outputs/kafka_franz