
<p align="center">
    <img src="icon.png" width=50% height=50% alt="Bento">
</p>

[![godoc for warpstreamlabs/bento][godoc-badge]][godoc-url]
[![Build Status][actions-badge]][actions-url]
[![Docs site][website-badge]][website-url]

[Discord](https://console.warpstream.com/socials/discord)
[Slack](https://console.warpstream.com/socials/slack)

Bento is a high performance and resilient stream processor, able to connect various [sources][inputs] and [sinks][outputs] in a range of brokering patterns and perform [hydration, enrichments, transformations and filters][processors] on payloads.

It comes with a [powerful mapping language][bloblang-about], is easy to deploy and monitor, and ready to drop into your pipeline either as a static binary, docker image, or [serverless function][serverless], making it cloud native as heck.

Bento is declarative, with stream pipelines defined in as few as a single config file, allowing you to specify connectors and a list of processing stages:

```yaml
input:
  gcp_pubsub:
    project: foo
    subscription: bar

pipeline:
  processors:
    - mapping: |
        root.message = this
        root.meta.link_count = this.links.length()
        root.user.age = this.user.age.number()

output:
  redis_streams:
    url: tcp://TODO:6379
    stream: baz
    max_in_flight: 20
```

### Delivery Guarantees

Delivery guarantees [can be a dodgy subject](https://youtu.be/QmpBOCvY8mY). Bento processes and acknowledges messages using an in-process transaction model with no need for any disk persisted state, so when connecting to at-least-once sources and sinks it's able to guarantee at-least-once delivery even in the event of crashes, disk corruption, or other unexpected server faults.

This behaviour is the default and free of caveats, which also makes deploying and scaling Bento much simpler.

## Supported Sources & Sinks

AWS (DynamoDB, Kinesis, S3, SQS, SNS), Azure (Blob storage, Queue storage, Table storage), GCP (Pub/Sub, Cloud storage, Big query), Kafka, NATS (JetStream, Streaming), NSQ, MQTT, AMQP 0.91 (RabbitMQ), AMQP 1, Redis (streams, list, pubsub, hashes), Cassandra, Elasticsearch, HDFS, HTTP (server and client, including websockets), MongoDB, SQL (MySQL, PostgreSQL, Clickhouse, MSSQL), and [you know what just click here to see them all, they don't fit in a README][about-categories].

Connectors are being added constantly, if something you want is missing then [open an issue](https://github.com/warpstreamlabs/bento/issues/new).

## Documentation

If you want to dive fully into Bento then don't waste your time in this dump, check out the [documentation site][general-docs].

For guidance on how to configure more advanced stream processing concepts such as stream joins, enrichment workflows, etc, check out the [cookbooks section.][cookbooks]

For guidance on building your own custom plugins in Go check out the section [Plugins](./README.md#plugins).

## Install

We're working on the release process, but you can either compile from source or pull the docker image:

```
docker pull ghcr.io/warpstreamlabs/bento
```

For more information check out the [getting started guide][getting-started].

## Run

```shell
bento -c ./config.yaml
```

Or, with docker:

```shell
# Using a config file
docker run --rm -v /path/to/your/config.yaml:/bento.yaml ghcr.io/warpstreamlabs/bento

# Using a series of -s flags
docker run --rm -p 4195:4195 ghcr.io/warpstreamlabs/bento \
  -s "input.type=http_server" \
  -s "output.type=kafka" \
  -s "output.kafka.addresses=kafka-server:9092" \
  -s "output.kafka.topic=bento_topic"
```

## Monitoring

### Health Checks

Bento serves two HTTP endpoints for health checks:
- `/ping` can be used as a liveness probe as it always returns a 200.
- `/ready` can be used as a readiness probe as it serves a 200 only when both the input and output are connected, otherwise a 503 is returned.

### Metrics

Bento [exposes lots of metrics][metrics] either to Statsd, Prometheus, a JSON HTTP endpoint, [and more][metrics].

### Tracing

Bento also [emits open telemetry tracing events][tracers], which can be used to visualise the processors within a pipeline.

## Configuration

Bento provides lots of tools for making configuration discovery, debugging and organisation easy. You can [read about them here][config-doc].

## Build

Build with Go (any [currently supported version](https://go.dev/dl/)):

```shell
git clone git@github.com:warpstreamlabs/bento
cd bento
make
go build -o bento ./cmd/bento/main.go
```

## Lint

Bento uses [golangci-lint][golangci-lint] for linting, which you can install with:

```shell
curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.64.5
```

Check the [GitHub Action](.github/workflows/test.yml ) for what version is currently used in CI.

And then run it with `make lint`.

## Plugins

It's pretty easy to write your own custom plugins for Bento in Go, for information check out [the API docs][godoc-url] and for inspiration there's a set of [plugin examples][plugin-examples] demonstrating a variety of plugin implementations. For help on how to set up your own distribution of Bento then checkout the [plugin example module][plugin-example-module].

## Extra Plugins

By default Bento does not build with components that require linking to external libraries, such as the `zmq4` input and outputs. If you wish to build Bento locally with these dependencies then set the build tag `x_bento_extra`:

```shell
# With go
go install -tags "x_bento_extra" github.com/warpstreamlabs/bento/cmd/bento@latest

# Using make
make TAGS=x_bento_extra
```

Note that this tag may change or be broken out into granular tags for individual components outside of major version releases. If you attempt a build and these dependencies are not present you'll see error messages such as `ld: library not found for -lzmq`.

## Docker Builds

There's a multi-stage `Dockerfile` for creating a Bento docker image which results in a minimal image from scratch. You can build it with:

```shell
make docker
```

Then use the image:

```shell
docker run --rm \
	-v /path/to/your/bento.yaml:/config.yaml \
	-v /tmp/data:/data \
	-p 4195:4195 \
	bento -c /config.yaml
```

## Contributing

Contributions are welcome, please [read the guidelines](CONTRIBUTING.md), come and chat (links are on the [community page][community]), and watch your back.

[inputs]: https://warpstreamlabs.github.io/bento/docs/components/inputs/about
[about-categories]: https://warpstreamlabs.github.io/bento/docs/about#components
[processors]: https://warpstreamlabs.github.io/bento/docs/components/processors/about
[outputs]: https://warpstreamlabs.github.io/bento/docs/components/outputs/about
[metrics]: https://warpstreamlabs.github.io/bento/docs/components/metrics/about
[tracers]: https://warpstreamlabs.github.io/bento/docs/components/tracers/about
[config-interp]: https://warpstreamlabs.github.io/bento/docs/configuration/interpolation
[streams-api]: https://warpstreamlabs.github.io/bento/docs/guides/streams_mode/streams_api
[streams-mode]: https://warpstreamlabs.github.io/bento/docs/guides/streams_mode/about
[general-docs]: https://warpstreamlabs.github.io/bento/docs/about
[bloblang-about]: https://warpstreamlabs.github.io/bento/docs/guides/bloblang/about
[config-doc]: https://warpstreamlabs.github.io/bento/docs/configuration/about
[serverless]: https://warpstreamlabs.github.io/bento/docs/guides/serverless/about
[cookbooks]: https://warpstreamlabs.github.io/bento/cookbooks
[releases]: https://github.com/warpstreamlabs/bento/releases
[plugin-examples]: https://pkg.go.dev/github.com/warpstreamlabs/bento/public/service#pkg-examples
[plugin-example-module]: https://github.com/warpstreamlabs/bento/tree/main/resources/plugin_example
[getting-started]: https://warpstreamlabs.github.io/bento/docs/guides/getting_started

[godoc-badge]: https://pkg.go.dev/badge/github.com/warpstreamlabs/bento/public
[godoc-url]: https://pkg.go.dev/github.com/warpstreamlabs/bento/public
[actions-badge]: https://github.com/warpstreamlabs/bento/actions/workflows/test.yml/badge.svg
[actions-url]: https://github.com/warpstreamlabs/bento/actions/workflows/test.yml
[website-badge]: https://img.shields.io/badge/Docs-Learn%20more-ffc7c7
[website-url]: https://warpstreamlabs.github.io/bento/

[community]: https://warpstreamlabs.github.io/bento/community

[golangci-lint]: https://golangci-lint.run/
[jaeger]: https://www.jaegertracing.io/
