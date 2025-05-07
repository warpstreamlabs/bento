import React from 'react';
import styles from './brand.module.css';
import Layout from '@theme/Layout';
import ThemedImage from '@theme/ThemedImage';
import Link from '@docusaurus/Link';

import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

function Brand() {
  const context = useDocusaurusContext();

  // Brand assets ordered as requested
  const brandAssets = [
    {
      name: 'Bento Brown',
      file: 'Bento Brown.svg',
    },
    {
      name: 'Bento Pink',
      file: 'Bento Pink.svg',
    },
    {
      name: 'Bento Full',
      file: 'Bento Full.svg',
    },
    {
      name: 'Benty',
      file: 'Benty.svg',
    },
    {
      name: 'Food Party',
      file: 'Food Party.svg',
    },
    {
      name: 'Slumber Party',
      file: 'Slumber Party.svg',
    },
    {
      name: 'Slumber Party Transparent',
      file: 'Slumber Party Transparent.svg',
    },
    {
      name: 'Rupert',
      file: 'Rupert.svg',
    },
    {
      name: 'Gohan',
      file: 'Gohan.svg',
    },
    {
      name: 'Geoff',
      file: 'Geoff.svg',
    },
    {
      name: 'Blob',
      file: 'Blob.svg',
    },
    {
      name: 'Geoff Blob',
      file: 'Geoff Blob.svg',
    },
  ];

  return (
    <div className="brand-page">
      <Layout title="Brand Assets" description="Bento brand assets for all your creative needs">
        <header className="hero">
          <div className="container text--center">
            <h1>Brand Assets</h1>
            <div className={`hero--subtitle ${styles.brandContent}`}>
              Begrudgingly boisterous Bento brand bits for boring stickers, mundane merch, and general nonsense.
            </div>
            <ThemedImage
              className={styles.headerImg}
              sources={{
                light: '/bento/img/foodparty.svg',
                dark: '/bento/img/foodparty_dark.svg',
              }}
              alt="Food party illustration"
            />
          </div>
        </header>
        <main>
          <div className="container">
            <div className="margin-vertical--xl">
              <div className="row">
                <div className="col col--12">
                  <div className="section padding-vert--lg">
                    <h2 className="text--center margin-bottom--lg">Download Our Brand Assets</h2>
                    <p className="text--center margin-bottom--xl">
                      Feel free to use these assets for your articles, presentations, or to make your own boring merch.
                    </p>
                    
                    <div className={styles.assetList}>
                      {brandAssets.map((asset, index) => (
                        <div key={index} className={styles.assetItem}>
                          <div className={styles.assetImage}>
                            <img 
                              src={`/bento/img/Brand/${asset.file}`} 
                              alt={asset.name} 
                            />
                          </div>
                          <div className={styles.assetInfo}>
                            <h3>{asset.name}</h3>
                            <Link
                              className={`button button--primary ${styles.downloadButton}`}
                              href={`/bento/img/Brand/${asset.file}`}
                              download
                              style={{ width: 'auto', display: 'inline-block' }}
                            >
                              Download
                            </Link>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </main>
      </Layout>
    </div>
  );
}

export default Brand; 