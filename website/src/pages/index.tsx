import React from 'react';
import classnames from 'classnames';
import ReactPlayer from 'react-player/youtube'
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import useBaseUrl from '@docusaurus/useBaseUrl';
import styles from './index.module.css';
import CodeBlock from "@theme/CodeBlock";
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

const installs = [
  {
    label: 'Curl',
    language: 'bash',
    children: `# Install
curl -Lsf https://warpstreamlabs.github.io/bento/sh/install | bash

# Make a config
bento create nats/protobuf/aws_sqs > ./config.yaml

# Run
bento -c ./config.yaml`
  },
  {
    label: 'Docker',
    language: 'bash',
    children: `# Pull
docker pull ghcr.io/warpstreamlabs/bento

# Make a config
docker run --rm ghcr.io/warpstreamlabs/bento create nats/protobuf/aws_sqs > ./config.yaml

# Run
docker run --rm -v $(pwd)/config.yaml:/bento.yaml ghcr.io/warpstreamlabs/bento`
  },
]

const snippets = [
  {
    label: 'Mapping',
    further: '/docs/guides/bloblang/about',
    language: 'yaml',
    children: `input:
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
    max_in_flight: 20`,
  },
  {
    label: 'Multiplexing',
    further: '/docs/components/outputs/about#multiplexing-outputs',
    language: 'yaml',
    children: `input:
  kafka:
    addresses: [ TODO ]
    topics: [ foo, bar ]
    consumer_group: foogroup

output:
  switch:
    cases:
      - check: doc.tags.contains("AWS")
        output:
          aws_sqs:
            url: https://sqs.us-west-2.amazonaws.com/TODO/TODO
            max_in_flight: 20

      - output:
          redis_pubsub:
            url: tcp://TODO:6379
            channel: baz
            max_in_flight: 20`,
  },
  {
    label: 'Windowing',
    further: '/docs/configuration/windowed_processing',
    language: 'yaml',
    children: `input:
  nats_jetstream:
    urls: [ nats://TODO:4222 ]
    queue: myqueue
    subject: traffic.light.events
    deliver: all

buffer:
  system_window:
    timestamp_mapping: root = this.created_at
    size: 1h

pipeline:
  processors:
    - group_by_value:
        value: '\${! json("traffic_light_id") }'
    - mapping: |
        root = if batch_index() == 0 {
          {
            "traffic_light_id": this.traffic_light_id,
            "created_at": @window_end_timestamp,
            "total_cars": json("registration_plate").from_all().unique().length(),
            "passengers": json("passengers").from_all().sum(),
          }
        } else { deleted() }

output:
  http_client:
    url: https://example.com/traffic_data
    verb: POST
    max_in_flight: 64`,
  },
  {
    label: 'Enrichments',
    further: '/cookbooks/enrichments',
    language: 'yaml',
    children: `input:
  mqtt:
    urls: [ tcp://TODO:1883 ]
    topics: [ foo ]

pipeline:
  processors:
    - branch:
        request_map: |
          root.id = this.doc.id
          root.content = this.doc.body
        processors:
          - aws_lambda:
              function: sentiment_analysis
        result_map: root.results.sentiment = this

output:
  aws_s3:
    bucket: TODO
    path: '\${! metadata("partition") }/\${! timestamp_unix_nano() }.tar.gz'
    batching:
      count: 100
      period: 10s
      processors:
        - archive:
            format: tar
        - compress:
            algorithm: gzip`,
  },
];

const features = [
  {
    title: 'Takes Care of the Dull Stuff',
    imageUrl: 'img/Blobboring.svg',
    description: (
      <>
        <p>
          Bento solves common data engineering tasks such as transformations, integrations, and multiplexing with declarative and <a href="/bento/docs/configuration/unit_testing">unit testable</a> configuration. This allows you to easily and incrementally adapt your data pipelines as requirements change, letting you focus on the more exciting stuff.
        </p>
        <p>
          It comes armed with a wide range of <a href="/bento/docs/components/processors/about">processors</a>, a <a href="/bento/docs/guides/bloblang/about">lit mapping language</a>, stateless <a href="/bento/docs/configuration/windowed_processing">windowed processing capabilities</a> and an industry leading mascot.
        </p>
      </>
    ),
  },
  {
    title: 'Well Connected',
    imageUrl: 'img/Blobborg.svg',
    description: (
      <>
        <p>
          Bento is able to glue a wide range of <a href="/bento/docs/components/inputs/about">sources</a> and <a href="/bento/docs/components/outputs/about">sinks</a> together and hook into a variety of <a href="/bento/docs/components/processors/sql_raw">databases</a>, <a href="/bento/docs/components/processors/cache">caches</a>, <a href="/bento/docs/components/processors/http">HTTP APIs</a>, <a href="/bento/docs/components/processors/aws_lambda">lambdas</a> and <a href="/bento/docs/components/processors/about">more</a>, enabling you to seamlessly drop it into your existing infrastructure.
        </p>
        <p>
          Working with disparate APIs and services can be a daunting task, doubly so in a streaming data context. With Bento it's possible to break these tasks down and automatically parallelize them as <a href="/bento/cookbooks/enrichments">a streaming workflow</a>.
        </p>
      </>
    ),
  },
  {
    title: 'Reliable and Operationally Simple',
    imageUrl: 'img/Blobscales.svg',
    description: (
      <>
        <p>
          Delivery guarantees <a href="https://youtu.be/QmpBOCvY8mY">can be a dodgy subject</a>. Bento processes and acknowledges messages using an in-process transaction model with no need for any disk persisted state, so when connecting to at-least-once sources and sinks it's able to guarantee at-least-once delivery even in the event of crashes, disk corruption, or other unexpected server faults.
        </p>
        <p>
          This behaviour is the default and free of caveats, which also makes deploying and scaling Bento much simpler. However, simplicity doesn't negate the need for observability, so it also exposes <a href="/bento/docs/components/metrics/about">metrics</a> and <a href="/bento/docs/components/tracers/about">tracing</a> events to targets of your choice.
        </p>
      </>
    ),
  },
  {
    title: 'Extendable',
    imageUrl: 'img/Blobextended.svg',
    description: (
      <>
        <p>
          Sometimes the components that come with Bento aren't enough. Luckily, Bento has been designed to be easily plugged with whatever components you need.
        </p>
        <p>
          You can either write plugins <a href="https://pkg.go.dev/github.com/warpstreamlabs/bento/public">directly in Go (recommended)</a> or you can have Bento run your plugin as a <a href="/bento/docs/components/processors/subprocess">subprocess</a>.
        </p>
      </>
    ),
  },
];

interface FeatureArgs {
  imageUrl?: string;
  title?: string;
  description: JSX.Element;
};

function Feature({ imageUrl, title, description }: FeatureArgs) {
  const imgUrl = useBaseUrl(imageUrl);
  return (
    <div className={classnames('col col--6')}>
      {imgUrl && (
        <div className="text--center">
          <img className={classnames('padding-vert--md', styles.featureImage)} src={imgUrl} alt={title} />
        </div>
      )}
      <h3>{title}</h3>
      {description}
    </div>
  );
}

function Home() {
  const context = useDocusaurusContext();
  const siteConfig = context.siteConfig;
  return (
    <Layout
      title={`${siteConfig.title}`}
      description="Fancy stream processing made operationally mundane">
      <header className={classnames('hero', styles.heroBanner)}>
        <div className="container">
          <div className="row">
            <div className={classnames('col col--5 col--offset-1')}>
              <h1 className="hero__title">{siteConfig.title}</h1>
              <p className="hero__subtitle">{siteConfig.tagline}</p>
              <div className={styles.buttons}>
                <Link
                  className={classnames(
                    'button button--outline button--primary button--lg',
                    styles.getStarted,
                  )}
                  to={useBaseUrl('docs/guides/getting_started')}>
                  Get Started
                </Link>
              </div>
            </div>
            <div className={classnames('col col--5')}>
              <img className={styles.heroImg} src="img/logo_hero.svg" />
            </div>
          </div>
        </div>
      </header>
      <main>
        <div className="container">
          <div className="row">
            <div className={classnames(`${styles.pitch} col col--6`)}>
              <h2>It's boringly easy to use</h2>
              <p>
                Written in Go, deployed as a static binary, declarative configuration. <a href="https://github.com/warpstreamlabs/bento">Open source</a> and cloud native as utter heck.
              </p>
              {/* TODO(install): fix me */}
              {/* {installs && installs.length && (
                <Tabs defaultValue={installs[0].label} values={installs.map((props, idx) => {
                  return { label: props.label, value: props.label };
                })}>
                  {installs.map((props, idx) => (
                    <TabItem key={idx} value={props.label}>
                      <CodeBlock {...props} />
                    </TabItem>
                  ))}
                </Tabs>
              )} */}
            </div>
            <div className={classnames('col col--6')}>
              {snippets && snippets.length && (
                <section className={styles.configSnippets}>
                  <Tabs defaultValue={snippets[0].label} values={snippets.map((props, idx) => {
                    return { label: props.label, value: props.label };
                  })}>
                    {snippets.map((props, idx) => (
                      <TabItem key={idx} value={props.label}>
                        <div style={{ position: 'relative' }}>
                          <CodeBlock {...props} />
                          {props.further && <Link
                            className={classnames(styles.furtherButton, 'button button--outline button--primary')}
                            to={props.further}>
                            Read about
                          </Link>}
                        </div>
                      </TabItem>
                    ))}
                  </Tabs>
                </section>
              )}
            </div>
          </div>
        </div>
        {features && features.length && (
          <section className={styles.features}>
            <div className="container margin-vert--md">
              <div className="row">
                {features.map((props, idx) => (
                  <Feature key={idx} {...props} />
                ))}
              </div>
            </div>
          </section>
        )}
        <section className={styles.loveSection}>
          <div className="container">
            <div className="row">
              <div className={classnames('col col--6')}>
                <h3 id="sponsored-by">Sponsored by the following heroes</h3>
                <div className="container">
                  <div className={classnames(styles.sponsorsBox, styles.goldSponsors)}>
                    <a href="https://www.warpstream.com/"><img src="/bento/img/sponsors/warpstream_logo.svg" /></a>
                  </div>
                  {/* <div>
                    <a href="https://synadia.com"><img className={styles.synadiaImg} src="/bento/img/sponsors/synadia.svg" /></a>
                  </div> */}
                </div>
              </div>
            </div>
          </div>
        </section>
      </main>
    </Layout>
  );
}

export default Home;
