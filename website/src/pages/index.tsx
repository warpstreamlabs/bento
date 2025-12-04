import React from 'react';
import classnames from 'classnames';
import ReactPlayer from 'react-player/youtube'
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import useBaseUrl from '@docusaurus/useBaseUrl';
import ThemedImage from '@theme/ThemedImage';
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
    description: (
      <>
        <p>
          Bento is able to glue a wide range of <a href="/bento/docs/components/inputs/about">sources</a> and <a href="/bento/docs/components/outputs/about">sinks</a> together and hook into a variety of <a href="/bento/docs/components/processors/sql_raw">databases</a>, <a href="/bento/docs/components/processors/cache">caches</a>, <a href="/bento/docs/components/processors/http">HTTP APIs</a>, <a href="/bento/docs/components/processors/aws_lambda">lambdas</a> and <a href="/bento/docs/components/processors/about">more processors</a>, enabling you to seamlessly drop it into your existing infrastructure.
        </p>
        <p>
          Working with disparate APIs and services can be a daunting task, doubly so in a streaming data context. With Bento it's possible to break these tasks down and automatically parallelize them as <a href="/bento/cookbooks/enrichments">a streaming workflow</a>.
        </p>
      </>
    ),
  },
  {
    title: 'Reliable and Operationally Simple',
    description: (
      <>
        <p>
          Delivery guarantees <a href="https://youtu.be/QmpBOCvY8mY" aria-label="Watch video about delivery guarantees">can be a dodgy subject</a>. Bento processes and acknowledges messages using an in-process transaction model with no need for any disk persisted state, so when connecting to at-least-once sources and sinks it's able to guarantee at-least-once delivery even in the event of crashes, disk corruption, or other unexpected server faults.
        </p>
        <p>
          This behaviour is the default and free of caveats, which also makes deploying and scaling Bento much simpler. However, simplicity doesn't negate the need for observability, so it also exposes <a href="/bento/docs/components/metrics/about" aria-label="Learn about Bento metrics">metrics</a> and <a href="/bento/docs/components/tracers/about" aria-label="Learn about Bento tracing">tracing</a> events to targets of your choice.
        </p>
      </>
    ),
  },
  {
    title: 'Extendable',
    description: (
      <>
        <p>
          Sometimes the components that come with Bento aren't enough. Luckily, Bento has been designed to be easily plugged with whatever components you need.
        </p>
        <p>
          You can either write plugins <a href="https://pkg.go.dev/github.com/warpstreamlabs/bento/public" aria-label="Learn how to write Bento plugins in Go">directly in Go (recommended)</a> or you can have Bento run your plugin as a <a href="/bento/docs/components/processors/subprocess" aria-label="Learn about Bento subprocess plugins">subprocess</a>.
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
      description="Fancy stream processing made operationally mundane"
      wrapperClassName="homepage">
      <header className={classnames('hero', styles.heroBanner)}>
        <div className="container">
          <div className="row">
            <div className={classnames('col col--5 col--offset-1')} style={{display: 'flex', flexDirection: 'column', justifyContent: 'center'}}>
              <h1 className="hero__title">Bento</h1>
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
            <div className={classnames('col col--5')} style={{display: 'flex', alignItems: 'center', justifyContent: 'center'}}>
              <ThemedImage
                className={styles.heroImg}
                sources={{
                  light: useBaseUrl('img/lightmode.svg'),
                  dark: useBaseUrl('img/darkmode.svg'),
                }}
                alt="Bento Logo"
              />
            </div>
          </div>
        </div>
      </header>
      <main>
        {/* Tabbed Section with Image */}
        <section className={classnames(styles.tabbedSection)}>
          <div className="container marfo">
            <div className="row">
              <div className={classnames('col col--5')} style={{display: 'flex', flexDirection: 'column', alignItems: 'flex-start', justifyContent: 'flex-start'}}>
                <ThemedImage
                  className={styles.tabbedSectionImg}
                  sources={{
                    light: useBaseUrl('img/boredom.svg'),
                    dark: useBaseUrl('img/boredom_dark.svg'),
                  }}
                  alt="Boredom"
                  style={{ marginTop: 0, marginBottom: '1rem' }}
                />
                <p className={styles.writtenInGo}>
                  Written in Go, deployed as a static binary, declarative configuration. <a href="https://github.com/warpstreamlabs/bento" target="_blank" rel="noopener noreferrer" aria-label="View Bento source code on GitHub">Open source</a> and cloud native as utter heck.
                </p>
                <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap' }}>
                  <Link
                    className={classnames('button button--outline button--primary', styles.learnMoreButton)}
                    to={useBaseUrl('docs/guides/getting_started')}>
                    Get Started
                  </Link>
                  <Link
                    className={classnames('button button--outline button--primary', styles.learnMoreButton)}
                    style={{ backgroundColor: 'rgba(255, 255, 255, 0.1)' }}
                    to={useBaseUrl('cookbooks')}>
                    Explore Cookbooks
                  </Link>
                  <Link
                    className={classnames('button button--outline button--primary', styles.learnMoreButton)}
                    style={{ backgroundColor: 'rgba(255, 255, 255, 0.1)' }}
                    to={useBaseUrl('docs/guides/bloblang/playground')}>
                    Try Playground
                  </Link>
                </div>
              </div>
              <div className={classnames('col col--7')}>
                {snippets && snippets.length && (
                  <section className={styles.configSnippets} style={{ marginTop: '50px' }}>
                    <Tabs defaultValue={snippets[0].label} values={snippets.map((props, idx) => {
                      return { label: props.label, value: props.label };
                    })}>
                      {snippets.map((props, idx) => (
                        <TabItem key={idx} value={props.label}>
                          <div style={{ position: 'relative' }}>
                            <CodeBlock {...props} />
                          </div>
                        </TabItem>
                      ))}
                    </Tabs>
                  </section>
                )}
              </div>
            </div>
          </div>
        </section>
        
        {features && features.length && (
          <section className={classnames(styles.features, 'snarf')}>
            <div className="container margin-vert--md">
              <div className="row" style={{ rowGap: '2rem' }}>
                {features.map((props, idx) => (
                  <Feature key={idx} {...props} />
                ))}
              </div>
              
              <a href="https://www.warpstream.com/" target="_blank" rel="external follow" style={{ textDecoration: 'none', color: 'inherit', display: 'block', marginTop: '3rem' }} aria-label="Visit WarpStream - Sponsor of Bento">
                <section className={styles.loveSection} style={{
                  maxWidth: '80rem',
                  margin: '0 auto',
                  borderRadius: '140px',
                  marginBottom: '60px',
                  boxShadow: '20px 20px #eb878829, 0px -10px 0px #ffdcdd29 inset',
                  transition: 'background-color 0.3s ease'
                }}>
                  <div className="container">
                    <div className="row">
                      <div className={classnames('col col--12')}>
                        <h3 id="sponsored-by">Sponsored by the boring folks at</h3>
                        <div className="container">
                          <div className={classnames(styles.sponsorsBox, styles.goldSponsors)}>
                            <img src="/bento/img/sponsors/warpstream_logo.svg" alt="WarpStream Logo" />
                          </div>
                          {/* <div>
                            <a href="https://synadia.com"><img className={styles.synadiaImg} src="/bento/img/sponsors/synadia.svg" /></a>
                          </div> */}
                        </div>
                      </div>
                    </div>
                  </div>
                </section>
              </a>
            </div>
          </section>
        )}

        {/* Full width food party image */}
        <section className={styles.fullWidthImageSection}>
          <div className="container-fluid" style={{ padding: 0 }}>
            <ThemedImage 
              sources={{
                light: useBaseUrl('img/foodparty.svg'),
                dark: useBaseUrl('img/foodparty_dark.svg'),
              }}
              alt="Food Party" 
              style={{ 
                width: '470px', 
                display: 'block', 
                margin: '0 auto', 
                marginTop: '6rem',
                marginBottom: '-60px',
                position: 'relative',
                zIndex: 10
              }}
            />
          </div>
        </section>
      </main>
    </Layout>
  );
}

export default Home;
