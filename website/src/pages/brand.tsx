import React from 'react';
import styles from './brand.module.css';
import Layout from '@theme/Layout';
import ThemedImage from '@theme/ThemedImage';

import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

function Brand() {
  const context = useDocusaurusContext();

  return (
    <Layout title="Brand Assets" description="Bento brand assets for all your creative needs">
      <header className="hero">
        <div className="container text--center">
          <h1>Brand Assets</h1>
          <div className={`hero--subtitle ${styles.brandContent}`}>
            Beautiful Bento brand bits for brilliant stickers, marvelous merch, and whatever fun nonsense you have in mind
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
          <div className="margin-vert--xl">
            <div className="row">
              <div className="col col--8 col--offset-2">
                <div className={`section padding-vert--lg text--center ${styles.brandContent}`}>
                  <h3>Coming Soon</h3>
                </div>
              </div>
            </div>
          </div>
        </div>
      </main>
    </Layout>
  );
}

export default Brand; 