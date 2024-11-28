const path = require('path');
const {components} = require('./src/plugins/components');

module.exports = {
  title: 'Bento',
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
    image: 'img/og_img.png',
    metadata: [
      {name: 'keywords', content: 'bento, stream processor, data engineering, ETL, ELT, event processor, go, golang'},
      {name: 'twitter:card', content: 'summary'},
    ],
    navbar: {
      title: 'Bento',
      logo: {
        alt: 'Bento',
        src: 'img/logo.svg',
      },
      items: [
        {to: 'docs/about', label: 'Docs', position: 'left'},
        {to: 'cookbooks', label: 'Cookbooks', position: 'left'},
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
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'See the Code',
              href: 'https://github.com/warpstreamlabs/bento',
            }
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} WarpStream Labs. Portions used under MIT License from Ashley Jeffs.`,
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

