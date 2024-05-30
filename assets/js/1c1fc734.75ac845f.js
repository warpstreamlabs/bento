"use strict";(self.webpackChunkbento=self.webpackChunkbento||[]).push([[8652],{6438:function(e,t,n){n.r(t),n.d(t,{assets:function(){return u},contentTitle:function(){return r},default:function(){return d},frontMatter:function(){return i},metadata:function(){return s},toc:function(){return a}});var o=n(5893),c=n(1151);const i={title:"Google Cloud Platform",description:"Find out about GCP components in Bento"},r=void 0,s={id:"guides/cloud/gcp",title:"Google Cloud Platform",description:"Find out about GCP components in Bento",source:"@site/docs/guides/cloud/gcp.md",sourceDirName:"guides/cloud",slug:"/guides/cloud/gcp",permalink:"/bento/docs/guides/cloud/gcp",draft:!1,unlisted:!1,editUrl:"https://github.com/warpstreamlabs/bento/edit/main/website/docs/guides/cloud/gcp.md",tags:[],version:"current",frontMatter:{title:"Google Cloud Platform",description:"Find out about GCP components in Bento"},sidebar:"docs",previous:{title:"Amazon Web Services",permalink:"/bento/docs/guides/cloud/aws"},next:{title:"About",permalink:"/bento/docs/guides/serverless/about"}},u={},a=[];function l(e){const t={a:"a",code:"code",p:"p",...(0,c.a)(),...e.components};return(0,o.jsxs)(o.Fragment,{children:[(0,o.jsx)(t.p,{children:"There are many components within Bento which utilise Google Cloud Platform (GCP) services. You will find that each of\nthese components require valid credentials."}),"\n",(0,o.jsxs)(t.p,{children:["When running Bento inside a Google Cloud environment that has a\n",(0,o.jsx)(t.a,{href:"https://cloud.google.com/iam/docs/service-accounts#default",children:"default service account"}),", it can automatically retrieve the\nservice account credentials to call Google Cloud APIs through a library called Application Default Credentials (ADC)."]}),"\n",(0,o.jsxs)(t.p,{children:["Otherwise, if your application runs outside Google Cloud environments that provide a default service account, you need\nto manually create one. Once you have a service account set up which has the required permissions, you can\n",(0,o.jsx)(t.a,{href:"https://console.cloud.google.com/apis/credentials/serviceaccountkey",children:"create"})," a new Service Account Key and download it\nas a JSON file. Then all you need to do set the path to this JSON file in the ",(0,o.jsx)(t.code,{children:"GOOGLE_APPLICATION_CREDENTIALS"}),"\nenvironment variable."]}),"\n",(0,o.jsxs)(t.p,{children:["Please refer to ",(0,o.jsx)(t.a,{href:"https://cloud.google.com/docs/authentication/production",children:"this document"})," for details."]})]})}function d(e={}){const{wrapper:t}={...(0,c.a)(),...e.components};return t?(0,o.jsx)(t,{...e,children:(0,o.jsx)(l,{...e})}):l(e)}},1151:function(e,t,n){n.d(t,{Z:function(){return s},a:function(){return r}});var o=n(7294);const c={},i=o.createContext(c);function r(e){const t=o.useContext(i);return o.useMemo((function(){return"function"==typeof e?e(t):{...t,...e}}),[t,e])}function s(e){let t;return t=e.disableParentContext?"function"==typeof e.components?e.components(c):e.components||c:r(e.components),o.createElement(i.Provider,{value:t},e.children)}}}]);