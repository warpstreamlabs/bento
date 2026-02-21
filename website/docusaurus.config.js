const path = require('path');
const {components} = require('./src/plugins/components');

module.exports = {
  title: 'Bento | Fancy stream processing made operationally mundane',
  tagline: 'Fancy stream processing made operationally mundane',
  url: 'https://warpstreamlabs.github.io/',
  baseUrl: '/bento/',
  favicon: '/img/favicon.ico',
  organizationName: 'warpstreamlabs',
  projectName: 'bento',
  customFields: {
    components: {
      inputs: components("inputs"),
      processors: components("processors"),
      outputs: components("outputs"),
      caches: components("caches"),
      rate_limits: components("rate_limits"),
      buffers: components("buffers"),
      metrics: components("metrics"),
      tracers: components("tracers"),
      scanners: components("scanners"),
    },
  },
  themeConfig: {
    prism: {
      theme: require('./src/plugins/prism_themes/github'),
      darkTheme: require('./src/plugins/prism_themes/monokai'),
    },
    colorMode: {
      defaultMode: 'light',
    },
    image: 'img/opengraph.png',
    metadata: [
      {name: 'description', content: 'Bento is a stream processor that makes data engineering simple with declarative and unit testable configuration. Connect various sources and sinks with YAML.'},
      {name: 'keywords', content: 'bento, stream processor, data engineering, ETL, ELT, event processor, go, golang, stream processing, data pipeline, Apache Kafka alternative'},
      {name: 'twitter:card', content: 'summary_large_image'},
      {name: 'twitter:image', content: 'img/opengraph.png'},
      {name: 'og:image', content: 'img/opengraph.png'},
      {name: 'og:image:width', content: '1200'},
      {name: 'og:image:height', content: '630'},
      {name: 'og:description', content: 'Bento is a stream processor that makes data engineering simple with declarative and unit testable configuration. Connect various sources and sinks with YAML.'},
      {name: 'og:title', content: 'Bento | Fancy stream processing made operationally mundane'},
      {name: 'og:type', content: 'website'},
      {name: 'og:site_name', content: 'Bento'},
    ],
    navbar: {
      title: '',
      logo: {
        alt: 'Bento',
        src: 'img/logo.svg',
        srcDark: 'img/logo_dark.svg',
      },
      items: [
        {to: 'docs/about', label: 'Docs', position: 'left'},
        {to: 'cookbooks', label: 'Cookbooks', position: 'left'},
        {to: 'docs/guides/bloblang/playground', label: 'Playground', position: 'left'},
        {
          type: 'html',
          position: 'right',
          value: '<div class="github-buttons-container" style="display: flex; align-items: center; height: 100%;"><iframe src="https://ghbtns.com/github-btn.html?user=warpstreamlabs&repo=bento&type=star&count=true" frameborder="0" scrolling="0" width="100" height="20" title="GitHub"></iframe></div>',
        },
        {to: 'community', label: 'Community / Support', position: 'right'},
        {
          href: 'https://github.com/warpstreamlabs/bento/releases/latest',
          position: 'right',
          className: 'header-download-link header-icon-link',
          'aria-label': 'Download',
        },
        {
          href: 'https://github.com/warpstreamlabs/bento',
          position: 'right',
          className: 'header-github-link header-icon-link',
          'aria-label': 'GitHub repository',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Help',
          items: [
            {
              label: 'Documentation',
              to: 'docs/guides/getting_started',
            },
            {
              label: 'See the Code',
              href: 'https://github.com/warpstreamlabs/bento',
            },
          ],
        },
        {
          title: null,
          items: [
            {
              html: `
                <div style="display: flex; justify-content: center; align-items: center; height: 100%;">
                  <a href="/bento/" style="display: flex; align-items: center; justify-content: center;">
                    <picture style="display: flex; align-items: center; justify-content: center; margin: 0;">
                      <source srcset="/bento/img/logo_dark.svg" media="(prefers-color-scheme: dark)">
                      <img src="/bento/img/logo.svg" alt="Bento Logo" width="180" height="auto" style="margin: 0;">
                    </picture>
                  </a>
                </div>
              `,
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'X (Twitter)',
              href: 'https://twitter.com/warpstream_labs',
            },
            {
              label: 'Slack',
              href: 'https://console.warpstream.com/socials/slack',
            },
            {
              label: 'Discord',
              href: 'https://console.warpstream.com/socials/discord',
            }
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} <a href="https://warpstream.com" target="_blank" rel="external follow" style="font-weight: bold; color: var(--ifm-color-white); color: #d32f2f;">WarpStream - A diskless, Apache Kafka compatible data streaming platform</a>. Portions used under MIT License from Ashley Jeffs.`,
    },
    announcementBar: {
      id: 'star_the_dang_repo',
      content: `<strong>Hey, 🫵 you, make sure you've <a target="_blank" rel="noopener noreferrer" href="https://github.com/warpstreamlabs/bento">⭐ starred the repo ⭐</a> otherwise you won't be entered into our daily prize draw for silent admiration.</strong>`,
      backgroundColor: 'var(--ifm-color-primary)',
      textColor: 'var(--ifm-background-color)',
      isCloseable: true,
    },
    algolia: {
      appId: 'LBT8FSOYRM',
      apiKey: 'f04107cd5a957d0508560517357bb54e',
      indexName: 'warpstreamlabsio',
      contextualSearch: true
    }
  },
  presets: [
    [
      '@docusaurus/preset-classic',
      {
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl:
            'https://github.com/warpstreamlabs/bento/edit/main/website/',
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
        blog: {
          feedOptions: {
            type: 'all',
          },
        },
      },
    ],
  ],
  headTags: [
    {
      tagName: 'link',
      attributes: {
        rel: 'icon',
        type: 'image/png',
        href: '/bento/img/favicon.png',
      },
    },
    {
      tagName: 'link',
      attributes: {
        rel: 'apple-touch-icon',
        href: '/bento/img/favicon.png',
      },
    },
    {
      tagName: 'link',
      attributes: {
        rel: 'shortcut icon',
        href: '/bento/img/favicon.ico',
      },
    },
    {
      tagName: 'meta',
      attributes: {
        name: 'msapplication-TileImage',
        content: '/bento/img/favicon.png',
      },
    },
    {
      tagName: 'meta',
      attributes: {
        name: 'msapplication-TileColor',
        content: '#FFBCBA',
      },
    },
    {
      tagName: 'script',
      attributes: {
        type: 'application/ld+json',
      },
      innerHTML: JSON.stringify({
        '@context': 'https://schema.org',
        '@type': 'WebSite',
        'name': 'Bento',
        'url': 'https://warpstreamlabs.github.io/bento/',
        'description': 'Fancy stream processing made operationally mundane',
        'potentialAction': {
          '@type': 'SearchAction',
          'target': 'https://warpstreamlabs.github.io/bento/search?q={search_term_string}',
          'query-input': 'required name=search_term_string'
        }
      }),
    },
    {
      tagName: 'script',
      attributes: {
        type: 'application/ld+json',
      },
      innerHTML: JSON.stringify({
        '@context': 'https://schema.org',
        '@type': 'SoftwareApplication',
        'name': 'Bento',
        'operatingSystem': 'Cross-platform',
        'applicationCategory': 'DeveloperApplication',
        'offers': {
          '@type': 'Offer',
          'price': '0',
          'priceCurrency': 'USD'
        },
        'description': 'A stream processor that makes fancy stream processing operationally mundane. Connect various data sources and sinks with simple YAML configuration.'
      }),
    },
    {
      tagName: 'script',
      attributes: {
        type: 'application/ld+json',
      },
      innerHTML: JSON.stringify({
        '@context': 'https://schema.org',
        '@type': 'Organization',
        'name': 'WarpStream Labs',
        'url': 'https://warpstream.com',
        'logo': 'https://warpstreamlabs.github.io/bento/img/logo.svg',
        'sameAs': [
          'https://twitter.com/warpstream_labs',
          'https://github.com/warpstreamlabs/bento'
        ]
      }),
    },
  ],
  plugins: [
    path.resolve(__dirname, './src/plugins/analytics'),
    [
      require.resolve("./src/plugins/cookbooks/compiled/index"),
      {
        path: 'cookbooks',
        routeBasePath: 'cookbooks',
        include: ['*.md', '*.mdx'],
        exclude: [],
        guideListComponent: '@theme/CookbookListPage',
        guidePostComponent: '@theme/CookbookPage',
      },
    ],
  ],
};

