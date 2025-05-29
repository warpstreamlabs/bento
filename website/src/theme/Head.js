import React from 'react';
import Head from '@docusaurus/Head';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import useBaseUrl from '@docusaurus/useBaseUrl';

export default function HeadCustom(props) {
  const {siteConfig} = useDocusaurusContext();
  const {title, favicon} = siteConfig;
  const faviconPng = useBaseUrl('img/favicon.png');

  return (
    <>
      <Head {...props}>
        {/* Standard favicon */}
        <link rel="icon" href={useBaseUrl(favicon)} />
        
        {/* PNG favicon for better compatibility with Google and modern browsers */}
        <link rel="icon" type="image/png" href={faviconPng} />

        {/* Apple Touch Icon for iOS devices */}
        <link rel="apple-touch-icon" href={faviconPng} />
        
        {/* For IE */}
        <link rel="shortcut icon" href={useBaseUrl(favicon)} />
        
        {/* For Android */}
        <meta name="msapplication-TileImage" content={faviconPng} />
        <meta name="msapplication-TileColor" content="#FFBCBA" />
      </Head>
    </>
  );
} 