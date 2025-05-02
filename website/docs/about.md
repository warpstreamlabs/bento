---
title: What is Bento for?
sidebar_label: About
hide_title: false
---

import Link from '@docusaurus/Link';

<div style={{display: 'flex', alignItems: 'flex-start'}}>
  <div style={{flex: 1, paddingRight: '3rem'}}>
    <p>Bento is a declarative data streaming service that solves a wide range of data engineering problems with simple, chained, stateless <a href="/bento/docs/components/processors/about">processing steps</a>. It implements transaction based resiliency with back pressure, so when connecting to at-least-once sources and sinks it's able to guarantee at-least-once delivery without needing to persist messages during transit.</p>
    
    <p>It's <a href="/bento/docs/guides/getting_started">simple to deploy</a>, comes with a wide range of <a href="#components">connectors</a>, and is totally data agnostic, making it easy to drop into your existing infrastructure. Bento has functionality that overlaps with integration frameworks, log aggregators and ETL workflow engines, and can therefore be used to complement these traditional data engineering tools or act as a simpler alternative.</p>
    
    <p>Bento is ready to commit to this relationship, are you?</p>
    
    <Link to="/docs/guides/getting_started" className="button button--lg button--outline button--block button--primary">Get Started</Link>
  </div>
  <img src="/bento/img/what-is-blob.svg" alt="Bento mascot" style={{width: '250px', marginTop: '-30px'}} />
</div>

<style dangerouslySetInnerHTML={{__html: `
  .markdown h2 {
    font-size: 1.5rem !important;
  }
  
  .markdown h3 {
    font-size: 1.2rem !important;
  }
`}} />

## Components

import ComponentsByCategory from '@theme/ComponentsByCategory';

### Inputs

<ComponentsByCategory type="inputs"></ComponentsByCategory>

---

### Processors

<ComponentsByCategory type="processors"></ComponentsByCategory>

---

### Outputs

<ComponentsByCategory type="outputs"></ComponentsByCategory>

[guides]: /cookbooks
[docs.guides.getting_started]: /docs/guides/getting_started
[docs.processors]: /docs/components/processors/about
