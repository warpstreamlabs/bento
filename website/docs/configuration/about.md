---
title: Configuration
sidebar_label: About
description: Learn about Bento configuration
---

Bento pipelines are configured in a YAML file that consists of a number of root sections, arranged like so:

import Tabs from '@theme/Tabs';

<Tabs defaultValue="common" values={[
  { label: 'Common', value: 'common', },
  { label: 'Full', value: 'full', },
]}>

import TabItem from '@theme/TabItem';

<TabItem value="common">

```yaml
input:
  kafka:
    addresses: [ TODO ]
    topics: [ foo, bar ]
    consumer_group: foogroup

pipeline:
  processors:
  - mapping: |
      root.message = this
      root.meta.link_count = this.links.length()

output:
  aws_s3:
    bucket: TODO
    path: '${! metadata("kafka_topic") }/${! json("message.id") }.json'
```

</TabItem>
<TabItem value="full">

```yaml
http:
  address: 0.0.0.0:4195
  debug_endpoints: false

input:
  kafka:
    addresses: [ TODO ]
    topics: [ foo, bar ]
    consumer_group: foogroup

buffer:
  none: {}

pipeline:
  processors:
  - mapping: |
      root.message = this
      root.meta.link_count = this.links.length()

output:
  aws_s3:
    bucket: TODO
    path: '${! metadata("kafka_topic") }/${! json("message.id") }.json'

input_resources: []
cache_resources: []
processor_resources: []
rate_limit_resources: []
output_resources: []

logger:
  level: INFO
  static_fields:
    '@service': bento

metrics:
  prometheus: {}

tracer:
  none: {}

shutdown_timeout: 20s
shutdown_delay: ""
```

</TabItem>

</Tabs>

Most sections represent a component type, which you can read about in more detail in [this document][components].

These types are hierarchical. For example, an `input` can have a list of child `processor` types attached to it, which in turn can have their own `processor` children.

This is powerful but can potentially lead to large and cumbersome configuration files. This document outlines tooling provided by Bento to help with writing and managing these more complex configuration files.

### Testing

For guidance on how to write and run unit tests for your configuration files read [this guide][config.testing].

## Customising Your Configuration

Sometimes it's useful to write a configuration where certain fields can be defined during deployment. For this purpose Bento supports [environment variable interpolation][config-interp], allowing you to set fields in your config with environment variables like so:

```yaml
input:
  kafka:
    addresses:
    - ${KAFKA_BROKER:localhost:9092}
    topics:
    - ${KAFKA_TOPIC:default-topic}
```

This is very useful for sharing configuration files across different deployment environments.

## Reusing Configuration Snippets

Sometimes it's necessary to use a rather large component multiple times. Instead of copy/pasting the configuration or using YAML anchors you can define your component [as a resource][config.resources].

In the following example we want to make an HTTP request with our payloads. Occasionally the payload might get rejected due to garbage within its contents, and so we catch these rejected requests, attempt to "cleanse" the contents and try to make the same HTTP request again. Since the HTTP request component is quite large (and likely to change over time) we make sure to avoid duplicating it by defining it as a resource `get_foo`:

```yaml
pipeline:
  processors:
    - resource: get_foo
    - catch:
      - mapping: |
          root = this
          root.content = this.content.strip_html()
      - resource: get_foo

processor_resources:
  - label: get_foo
    http:
      url: http://example.com/foo
      verb: POST
      headers:
        SomeThing: "set-to-this"
        SomeThingElse: "set-to-something-else"
```

### Feature Toggles

Resources can be imported separately to your config file with the cli flag `-r` or `-resources`, which is a useful way to switch out resources with common names based on your chosen environment. For example, with a main configuration file `config.yaml`:

```yaml
pipeline:
  processors:
    - resource: get_foo
```

And then two resource files, one stored at the path `./staging/request.yaml`:

```yaml
processor_resources:
  - label: get_foo
    http:
      url: http://example.com/foo
      verb: POST
      headers:
        SomeThing: "set-to-this"
        SomeThingElse: "set-to-something-else"
```

And another stored at the path `./production/request.yaml`:

```yaml
processor_resources:
  - label: get_foo
    http:
      url: http://example.com/bar
      verb: PUT
      headers:
        Desires: "are-empty"
```

We can select our chosen resource by changing which file we import, either running:

```sh
bento -r ./staging/request.yaml -c ./config.yaml
```

Or:

```sh
bento -r ./production/request.yaml -c ./config.yaml
```

These flags also support wildcards, which allows you to import an entire directory of resource files like `bento -r "./staging/*.yaml" -c ./config.yaml`. You can find out more about configuration resources in the [resources document][config.resources].

### Templating

Resources can only be instantiated with a single configuration, which means they aren't suitable for cases where the configuration is required in multiple places but with slightly different parameters, ugh!

But hey, why don't you chill out? Bento has a (currently experimental) alternative feature called templates, with which it's possible to define a custom configuration schema and a template for building a configuration from that schema. You can read more about templates [in this guide][config.templating].

## Reloading

It's possible to have a running instance of Bento reload configurations, including resource files imported with `-r`/`--resources`, automatically when the files are updated without needing to manually restart the service. This is done by specifying the `-w`/`--watcher` flag when running Bento in normal mode or in streams mode:

```sh
# Normal mode
bento -w -r ./production/request.yaml -c ./config.yaml
```

```sh
# Streams mode
bento -w -r ./production/request.yaml streams ./stream_configs/*.yaml
```

If a file update results in configuration parsing or linting errors then the change is ignored (with logs informing you of the problem) and the previous configuration will continue to be run (until the issues are fixed).

## Enabling Discovery

The discoverability of configuration fields is a common headache with any configuration driven application. The classic solution is to provide curated documentation that is often hosted on a dedicated site.

However, a user often only needs to get their hands on a short, runnable example config file for their use case. They just need to see the format and field names as the fields themselves are usually self explanatory. Forcing such a user to navigate a website, scrolling through paragraphs of text, seems inefficient when all they actually needed to see was something like:

```yaml
input:
  amqp_0_9:
    urls: [ amqp://guest:guest@localhost:5672/ ]
    consumer_tag: bento-consumer
    queue: bento-queue
    prefetch_count: 10
    prefetch_size: 0
output:
  stdout: {}
```

In order to make this process easier Bento is able to generate usable configuration examples for any types, and you can do this from the binary using the `create` subcommand.

If, for example, we wanted to generate a config with a websocket input, a Kafka output and a [`mapping` processor][processors.mapping] in the middle, we could do it with the following command:

```text
bento create websocket/mapping/kafka
```

> If you need a gentle reminder as to which components Bento offers you can see those as well with `bento list`.

All of these generated configuration examples also include other useful config sections such as `metrics`, `logging`, etc with sensible defaults.

For more information read the output from `bento create --help`.

## Help With Debugging

Once you have a config written you now move onto the next headache of proving that it works, and understanding why it doesn't. Bento, like most good config driven services, performs validation on configs and tries to provide sensible error messages.

However, with validation it can be hard to capture all problems, and the user usually understands their intentions better than the service. In order to help expose and diagnose config errors Bento provides two mechanisms, linting and echoing.

### Linting

If you attempt to run a config that has linting errors Bento will print the errors and halt execution. If, however, you want to test your configs before deployment you can do so with the `lint` subcommand:

For example, imagine we have a config `foo.yaml`, where we intend to read from AMQP, but there is a typo in our config struct:

```text
input:
  amqp_0_9:
    yourl: amqp://guest:guest@rabbitmqserver:5672/
```

We can catch this error before attempting to run the config:

```sh
$ bento lint ./foo.yaml
./foo.yaml: line 3: field yourl not recognised
```

For more information read the output from `bento lint --help`.

### Echoing

Echoing is where Bento can print back your configuration _after_ it has been parsed. It is done with the `echo` subcommand, which is able to show you a normalised version of your config, allowing you to see how it was interpreted:

```sh
bento -c ./your-config.yaml echo
```

You can check the output of the above command to see if certain sections are missing or fields are incorrect, which allows you to pinpoint typos in the config.

## Shutting down

Under normal operating conditions, the Bento process will shut down when there are no more messages produced by inputs and the final message has been processed. The shutdown procedure can also be initiated by sending the process a interrupt (`SIGINT`) or termination (`SIGTERM`) signal. There are two top-level configuration options that control the shutdown behaviour: `shutdown_timeout` and `shutdown_delay`.

### Shutdown delay

The `shutdown_delay` option can be used to delay the start of the shutdown procedure. This is useful for pipelines that need a short grace period to have their metrics and traces scraped. While the shutdown delay is in effect, the HTTP metrics endpoint continues to be available for scraping and any active tracers are free to flush remaining traces.

The shutdown delay can be interrupted by sending the Bento process a second OS interrupt or termination signal.

### Shutdown timeout

The `shutdown_timeout` option sets a hard deadline for Bento process to gracefully terminate. If this duration is exceeded then the process is forcefully terminated and any messages that were in-flight will be dropped.

This option takes effect after the `shutdown_delay` duration has passed if that is enabled.

[processors]: /docs/components/processors/about
[processors.mapping]: /docs/components/processors/mapping
[config-interp]: /docs/configuration/interpolation
[config.testing]: /docs/configuration/unit_testing
[config.templating]: /docs/configuration/templating
[config.resources]: /docs/configuration/resources
[json-references]: https://tools.ietf.org/html/draft-pbryan-zyp-json-ref-03
[components]: /docs/components/about
