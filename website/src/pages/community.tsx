import React from 'react';

import styles from './community.module.css';
import classnames from 'classnames';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import ThemedImage from '@theme/ThemedImage';

import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

function Community() {
  const context = useDocusaurusContext();

  return (
    <Layout title="Community / Support" description="Where to ask questions and find your soul mate">
      <header className="hero">
        <div className="container text--center">
          <h1>Community / Support</h1>
          <div className="hero--subtitle">
            These are places where you can ask questions and find your soul mate (no promises).
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
          <div className="row margin-vert--lg">
            <div className="col text--center padding-vert--md">
              <div className="card">
                <div className="card__header">
                  <i className={classnames(styles.icon, styles.discord)}></i>
                </div>
                <div className="card__body">
                  <p>Join the official Bento discord server</p>
                </div>
                <div className="card__footer">
                  <Link to="https://console.warpstream.com/socials/discord" className="button button--outline button--primary button--block">Join</Link>
                </div>
              </div>
            </div>

            <div className="col text--center padding-vert--md">
              <div className="card">
                <div className="card__header">
                  <i className={classnames(styles.icon, styles.twitter)}></i>
                </div>
                <div className="card__body">
                  <p>Aggressively &#64;mention WarpStream on Twitter</p>
                </div>
                <div className="card__footer">
                  <Link to="https://twitter.com/warpstream_labs" className="button button--outline button--primary button--block">Follow &#64;WarpStream</Link>
                </div>
              </div>
            </div>

            <div className="col text--center padding-vert--md">
              <div className="card">
                <div className="card__header">
                  <i className={classnames(styles.icon, styles.slack)}></i>
                </div>
                <div className="card__body">
                  <p>Join us on the &#35;bento channel in the WarpStream Labs Community slack</p>
                </div>
                <div className="card__footer">
                  <Link to="https://console.warpstream.com/socials/slack" className="button button--outline button--primary button--block">Open</Link>
                </div>
              </div>
            </div>
          </div>
        </div>
      </main>
    </Layout>
  );
}

export default Community;
