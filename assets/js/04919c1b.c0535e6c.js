"use strict";(self.webpackChunkbento=self.webpackChunkbento||[]).push([[3798],{1057:function(e,n,t){t.r(n),t.d(n,{assets:function(){return d},contentTitle:function(){return a},default:function(){return p},frontMatter:function(){return c},metadata:function(){return o},toc:function(){return u}});var s=t(5893),r=t(1151),l=t(4866),i=t(5162);const c={title:"nsq",slug:"nsq",type:"input",status:"stable",categories:["Services"],name:"nsq"},a=void 0,o={id:"components/inputs/nsq",title:"nsq",description:"\x3c!--",source:"@site/docs/components/inputs/nsq.md",sourceDirName:"components/inputs",slug:"/components/inputs/nsq",permalink:"/bento/docs/components/inputs/nsq",draft:!1,unlisted:!1,editUrl:"https://github.com/warpstreamlabs/bento/edit/main/website/docs/components/inputs/nsq.md",tags:[],version:"current",frontMatter:{title:"nsq",slug:"nsq",type:"input",status:"stable",categories:["Services"],name:"nsq"},sidebar:"docs",previous:{title:"nats_stream",permalink:"/bento/docs/components/inputs/nats_stream"},next:{title:"parquet",permalink:"/bento/docs/components/inputs/parquet"}},d={},u=[{value:"Metadata",id:"metadata",level:3},{value:"Fields",id:"fields",level:2},{value:"<code>nsqd_tcp_addresses</code>",id:"nsqd_tcp_addresses",level:3},{value:"<code>lookupd_http_addresses</code>",id:"lookupd_http_addresses",level:3},{value:"<code>tls</code>",id:"tls",level:3},{value:"<code>tls.enabled</code>",id:"tlsenabled",level:3},{value:"<code>tls.skip_cert_verify</code>",id:"tlsskip_cert_verify",level:3},{value:"<code>tls.enable_renegotiation</code>",id:"tlsenable_renegotiation",level:3},{value:"<code>tls.root_cas</code>",id:"tlsroot_cas",level:3},{value:"<code>tls.root_cas_file</code>",id:"tlsroot_cas_file",level:3},{value:"<code>tls.client_certs</code>",id:"tlsclient_certs",level:3},{value:"<code>tls.client_certs[].cert</code>",id:"tlsclient_certscert",level:3},{value:"<code>tls.client_certs[].key</code>",id:"tlsclient_certskey",level:3},{value:"<code>tls.client_certs[].cert_file</code>",id:"tlsclient_certscert_file",level:3},{value:"<code>tls.client_certs[].key_file</code>",id:"tlsclient_certskey_file",level:3},{value:"<code>tls.client_certs[].password</code>",id:"tlsclient_certspassword",level:3},{value:"<code>topic</code>",id:"topic",level:3},{value:"<code>channel</code>",id:"channel",level:3},{value:"<code>user_agent</code>",id:"user_agent",level:3},{value:"<code>max_in_flight</code>",id:"max_in_flight",level:3},{value:"<code>max_attempts</code>",id:"max_attempts",level:3}];function h(e){const n={a:"a",admonition:"admonition",br:"br",code:"code",h2:"h2",h3:"h3",p:"p",pre:"pre",...(0,r.a)(),...e.components};return(0,s.jsxs)(s.Fragment,{children:[(0,s.jsx)(n.p,{children:"Subscribe to an NSQ instance topic and channel."}),"\n",(0,s.jsxs)(l.Z,{defaultValue:"common",values:[{label:"Common",value:"common"},{label:"Advanced",value:"advanced"}],children:[(0,s.jsx)(i.Z,{value:"common",children:(0,s.jsx)(n.pre,{children:(0,s.jsx)(n.code,{className:"language-yml",children:'# Common config fields, showing default values\ninput:\n  label: ""\n  nsq:\n    nsqd_tcp_addresses: [] # No default (required)\n    lookupd_http_addresses: [] # No default (required)\n    topic: "" # No default (required)\n    channel: "" # No default (required)\n    user_agent: "" # No default (optional)\n    max_in_flight: 100\n    max_attempts: 5\n'})})}),(0,s.jsx)(i.Z,{value:"advanced",children:(0,s.jsx)(n.pre,{children:(0,s.jsx)(n.code,{className:"language-yml",children:'# All config fields, showing default values\ninput:\n  label: ""\n  nsq:\n    nsqd_tcp_addresses: [] # No default (required)\n    lookupd_http_addresses: [] # No default (required)\n    tls:\n      enabled: false\n      skip_cert_verify: false\n      enable_renegotiation: false\n      root_cas: ""\n      root_cas_file: ""\n      client_certs: []\n    topic: "" # No default (required)\n    channel: "" # No default (required)\n    user_agent: "" # No default (optional)\n    max_in_flight: 100\n    max_attempts: 5\n'})})})]}),"\n",(0,s.jsx)(n.h3,{id:"metadata",children:"Metadata"}),"\n",(0,s.jsx)(n.p,{children:"This input adds the following metadata fields to each message:"}),"\n",(0,s.jsx)(n.pre,{children:(0,s.jsx)(n.code,{className:"language-text",children:"- nsq_attempts\n- nsq_id\n- nsq_nsqd_address\n- nsq_timestamp\n"})}),"\n",(0,s.jsxs)(n.p,{children:["You can access these metadata fields using ",(0,s.jsx)(n.a,{href:"/docs/configuration/interpolation#bloblang-queries",children:"function interpolation"}),"."]}),"\n",(0,s.jsx)(n.h2,{id:"fields",children:"Fields"}),"\n",(0,s.jsx)(n.h3,{id:"nsqd_tcp_addresses",children:(0,s.jsx)(n.code,{children:"nsqd_tcp_addresses"})}),"\n",(0,s.jsx)(n.p,{children:"A list of nsqd addresses to connect to."}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"array"})]}),"\n",(0,s.jsx)(n.h3,{id:"lookupd_http_addresses",children:(0,s.jsx)(n.code,{children:"lookupd_http_addresses"})}),"\n",(0,s.jsx)(n.p,{children:"A list of nsqlookupd addresses to connect to."}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"array"})]}),"\n",(0,s.jsx)(n.h3,{id:"tls",children:(0,s.jsx)(n.code,{children:"tls"})}),"\n",(0,s.jsx)(n.p,{children:"Custom TLS settings can be used to override system defaults."}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"object"})]}),"\n",(0,s.jsx)(n.h3,{id:"tlsenabled",children:(0,s.jsx)(n.code,{children:"tls.enabled"})}),"\n",(0,s.jsx)(n.p,{children:"Whether custom TLS settings are enabled."}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"bool"}),(0,s.jsx)(n.br,{}),"\n","Default: ",(0,s.jsx)(n.code,{children:"false"})]}),"\n",(0,s.jsx)(n.h3,{id:"tlsskip_cert_verify",children:(0,s.jsx)(n.code,{children:"tls.skip_cert_verify"})}),"\n",(0,s.jsx)(n.p,{children:"Whether to skip server side certificate verification."}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"bool"}),(0,s.jsx)(n.br,{}),"\n","Default: ",(0,s.jsx)(n.code,{children:"false"})]}),"\n",(0,s.jsx)(n.h3,{id:"tlsenable_renegotiation",children:(0,s.jsx)(n.code,{children:"tls.enable_renegotiation"})}),"\n",(0,s.jsxs)(n.p,{children:["Whether to allow the remote server to repeatedly request renegotiation. Enable this option if you're seeing the error message ",(0,s.jsx)(n.code,{children:"local error: tls: no renegotiation"}),"."]}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"bool"}),(0,s.jsx)(n.br,{}),"\n","Default: ",(0,s.jsx)(n.code,{children:"false"}),(0,s.jsx)(n.br,{}),"\n","Requires version 3.45.0 or newer"]}),"\n",(0,s.jsx)(n.h3,{id:"tlsroot_cas",children:(0,s.jsx)(n.code,{children:"tls.root_cas"})}),"\n",(0,s.jsx)(n.p,{children:"An optional root certificate authority to use. This is a string, representing a certificate chain from the parent trusted root certificate, to possible intermediate signing certificates, to the host certificate."}),"\n",(0,s.jsx)(n.admonition,{title:"Secret",type:"warning",children:(0,s.jsxs)(n.p,{children:["This field contains sensitive information that usually shouldn't be added to a config directly, read our ",(0,s.jsx)(n.a,{href:"/docs/configuration/secrets",children:"secrets page for more info"}),"."]})}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"string"}),(0,s.jsx)(n.br,{}),"\n","Default: ",(0,s.jsx)(n.code,{children:'""'})]}),"\n",(0,s.jsx)(n.pre,{children:(0,s.jsx)(n.code,{className:"language-yml",children:"# Examples\n\nroot_cas: |-\n  -----BEGIN CERTIFICATE-----\n  ...\n  -----END CERTIFICATE-----\n"})}),"\n",(0,s.jsx)(n.h3,{id:"tlsroot_cas_file",children:(0,s.jsx)(n.code,{children:"tls.root_cas_file"})}),"\n",(0,s.jsx)(n.p,{children:"An optional path of a root certificate authority file to use. This is a file, often with a .pem extension, containing a certificate chain from the parent trusted root certificate, to possible intermediate signing certificates, to the host certificate."}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"string"}),(0,s.jsx)(n.br,{}),"\n","Default: ",(0,s.jsx)(n.code,{children:'""'})]}),"\n",(0,s.jsx)(n.pre,{children:(0,s.jsx)(n.code,{className:"language-yml",children:"# Examples\n\nroot_cas_file: ./root_cas.pem\n"})}),"\n",(0,s.jsx)(n.h3,{id:"tlsclient_certs",children:(0,s.jsx)(n.code,{children:"tls.client_certs"})}),"\n",(0,s.jsxs)(n.p,{children:["A list of client certificates to use. For each certificate either the fields ",(0,s.jsx)(n.code,{children:"cert"})," and ",(0,s.jsx)(n.code,{children:"key"}),", or ",(0,s.jsx)(n.code,{children:"cert_file"})," and ",(0,s.jsx)(n.code,{children:"key_file"})," should be specified, but not both."]}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"array"}),(0,s.jsx)(n.br,{}),"\n","Default: ",(0,s.jsx)(n.code,{children:"[]"})]}),"\n",(0,s.jsx)(n.pre,{children:(0,s.jsx)(n.code,{className:"language-yml",children:"# Examples\n\nclient_certs:\n  - cert: foo\n    key: bar\n\nclient_certs:\n  - cert_file: ./example.pem\n    key_file: ./example.key\n"})}),"\n",(0,s.jsx)(n.h3,{id:"tlsclient_certscert",children:(0,s.jsx)(n.code,{children:"tls.client_certs[].cert"})}),"\n",(0,s.jsx)(n.p,{children:"A plain text certificate to use."}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"string"}),(0,s.jsx)(n.br,{}),"\n","Default: ",(0,s.jsx)(n.code,{children:'""'})]}),"\n",(0,s.jsx)(n.h3,{id:"tlsclient_certskey",children:(0,s.jsx)(n.code,{children:"tls.client_certs[].key"})}),"\n",(0,s.jsx)(n.p,{children:"A plain text certificate key to use."}),"\n",(0,s.jsx)(n.admonition,{title:"Secret",type:"warning",children:(0,s.jsxs)(n.p,{children:["This field contains sensitive information that usually shouldn't be added to a config directly, read our ",(0,s.jsx)(n.a,{href:"/docs/configuration/secrets",children:"secrets page for more info"}),"."]})}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"string"}),(0,s.jsx)(n.br,{}),"\n","Default: ",(0,s.jsx)(n.code,{children:'""'})]}),"\n",(0,s.jsx)(n.h3,{id:"tlsclient_certscert_file",children:(0,s.jsx)(n.code,{children:"tls.client_certs[].cert_file"})}),"\n",(0,s.jsx)(n.p,{children:"The path of a certificate to use."}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"string"}),(0,s.jsx)(n.br,{}),"\n","Default: ",(0,s.jsx)(n.code,{children:'""'})]}),"\n",(0,s.jsx)(n.h3,{id:"tlsclient_certskey_file",children:(0,s.jsx)(n.code,{children:"tls.client_certs[].key_file"})}),"\n",(0,s.jsx)(n.p,{children:"The path of a certificate key to use."}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"string"}),(0,s.jsx)(n.br,{}),"\n","Default: ",(0,s.jsx)(n.code,{children:'""'})]}),"\n",(0,s.jsx)(n.h3,{id:"tlsclient_certspassword",children:(0,s.jsx)(n.code,{children:"tls.client_certs[].password"})}),"\n",(0,s.jsxs)(n.p,{children:["A plain text password for when the private key is password encrypted in PKCS#1 or PKCS#8 format. The obsolete ",(0,s.jsx)(n.code,{children:"pbeWithMD5AndDES-CBC"})," algorithm is not supported for the PKCS#8 format. Warning: Since it does not authenticate the ciphertext, it is vulnerable to padding oracle attacks that can let an attacker recover the plaintext."]}),"\n",(0,s.jsx)(n.admonition,{title:"Secret",type:"warning",children:(0,s.jsxs)(n.p,{children:["This field contains sensitive information that usually shouldn't be added to a config directly, read our ",(0,s.jsx)(n.a,{href:"/docs/configuration/secrets",children:"secrets page for more info"}),"."]})}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"string"}),(0,s.jsx)(n.br,{}),"\n","Default: ",(0,s.jsx)(n.code,{children:'""'})]}),"\n",(0,s.jsx)(n.pre,{children:(0,s.jsx)(n.code,{className:"language-yml",children:"# Examples\n\npassword: foo\n\npassword: ${KEY_PASSWORD}\n"})}),"\n",(0,s.jsx)(n.h3,{id:"topic",children:(0,s.jsx)(n.code,{children:"topic"})}),"\n",(0,s.jsx)(n.p,{children:"The topic to consume from."}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"string"})]}),"\n",(0,s.jsx)(n.h3,{id:"channel",children:(0,s.jsx)(n.code,{children:"channel"})}),"\n",(0,s.jsx)(n.p,{children:"The channel to consume from."}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"string"})]}),"\n",(0,s.jsx)(n.h3,{id:"user_agent",children:(0,s.jsx)(n.code,{children:"user_agent"})}),"\n",(0,s.jsx)(n.p,{children:"A user agent to assume when connecting."}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"string"})]}),"\n",(0,s.jsx)(n.h3,{id:"max_in_flight",children:(0,s.jsx)(n.code,{children:"max_in_flight"})}),"\n",(0,s.jsx)(n.p,{children:"The maximum number of pending messages to consume at any given time."}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"int"}),(0,s.jsx)(n.br,{}),"\n","Default: ",(0,s.jsx)(n.code,{children:"100"})]}),"\n",(0,s.jsx)(n.h3,{id:"max_attempts",children:(0,s.jsx)(n.code,{children:"max_attempts"})}),"\n",(0,s.jsx)(n.p,{children:"The maximum number of attempts to successfully consume a messages."}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"int"}),(0,s.jsx)(n.br,{}),"\n","Default: ",(0,s.jsx)(n.code,{children:"5"})]})]})}function p(e={}){const{wrapper:n}={...(0,r.a)(),...e.components};return n?(0,s.jsx)(n,{...e,children:(0,s.jsx)(h,{...e})}):h(e)}},5162:function(e,n,t){t.d(n,{Z:function(){return i}});t(7294);var s=t(6010),r={tabItem:"tabItem_Ymn6"},l=t(5893);function i(e){let{children:n,hidden:t,className:i}=e;return(0,l.jsx)("div",{role:"tabpanel",className:(0,s.Z)(r.tabItem,i),hidden:t,children:n})}},4866:function(e,n,t){t.d(n,{Z:function(){return y}});var s=t(7294),r=t(6010),l=t(2466),i=t(6550),c=t(469),a=t(1980),o=t(7392),d=t(12);function u(e){var n,t;return null!=(n=null==(t=s.Children.toArray(e).filter((e=>"\n"!==e)).map((e=>{if(!e||(0,s.isValidElement)(e)&&function(e){const{props:n}=e;return!!n&&"object"==typeof n&&"value"in n}(e))return e;throw new Error("Docusaurus error: Bad <Tabs> child <"+("string"==typeof e.type?e.type:e.type.name)+'>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.')})))?void 0:t.filter(Boolean))?n:[]}function h(e){const{values:n,children:t}=e;return(0,s.useMemo)((()=>{const e=null!=n?n:function(e){return u(e).map((e=>{let{props:{value:n,label:t,attributes:s,default:r}}=e;return{value:n,label:t,attributes:s,default:r}}))}(t);return function(e){const n=(0,o.l)(e,((e,n)=>e.value===n.value));if(n.length>0)throw new Error('Docusaurus error: Duplicate values "'+n.map((e=>e.value)).join(", ")+'" found in <Tabs>. Every value needs to be unique.')}(e),e}),[n,t])}function p(e){let{value:n,tabValues:t}=e;return t.some((e=>e.value===n))}function f(e){let{queryString:n=!1,groupId:t}=e;const r=(0,i.k6)(),l=function(e){let{queryString:n=!1,groupId:t}=e;if("string"==typeof n)return n;if(!1===n)return null;if(!0===n&&!t)throw new Error('Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".');return null!=t?t:null}({queryString:n,groupId:t});return[(0,a._X)(l),(0,s.useCallback)((e=>{if(!l)return;const n=new URLSearchParams(r.location.search);n.set(l,e),r.replace({...r.location,search:n.toString()})}),[l,r])]}function x(e){const{defaultValue:n,queryString:t=!1,groupId:r}=e,l=h(e),[i,a]=(0,s.useState)((()=>function(e){var n;let{defaultValue:t,tabValues:s}=e;if(0===s.length)throw new Error("Docusaurus error: the <Tabs> component requires at least one <TabItem> children component");if(t){if(!p({value:t,tabValues:s}))throw new Error('Docusaurus error: The <Tabs> has a defaultValue "'+t+'" but none of its children has the corresponding value. Available values are: '+s.map((e=>e.value)).join(", ")+". If you intend to show no default tab, use defaultValue={null} instead.");return t}const r=null!=(n=s.find((e=>e.default)))?n:s[0];if(!r)throw new Error("Unexpected error: 0 tabValues");return r.value}({defaultValue:n,tabValues:l}))),[o,u]=f({queryString:t,groupId:r}),[x,m]=function(e){let{groupId:n}=e;const t=function(e){return e?"docusaurus.tab."+e:null}(n),[r,l]=(0,d.Nk)(t);return[r,(0,s.useCallback)((e=>{t&&l.set(e)}),[t,l])]}({groupId:r}),j=(()=>{const e=null!=o?o:x;return p({value:e,tabValues:l})?e:null})();(0,c.Z)((()=>{j&&a(j)}),[j]);return{selectedValue:i,selectValue:(0,s.useCallback)((e=>{if(!p({value:e,tabValues:l}))throw new Error("Can't select invalid tab value="+e);a(e),u(e),m(e)}),[u,m,l]),tabValues:l}}var m=t(2389),j={tabList:"tabList__CuJ",tabItem:"tabItem_LNqP"},_=t(5893);function b(e){let{className:n,block:t,selectedValue:s,selectValue:i,tabValues:c}=e;const a=[],{blockElementScrollPositionUntilNextRender:o}=(0,l.o5)(),d=e=>{const n=e.currentTarget,t=a.indexOf(n),r=c[t].value;r!==s&&(o(n),i(r))},u=e=>{var n;let t=null;switch(e.key){case"Enter":d(e);break;case"ArrowRight":{var s;const n=a.indexOf(e.currentTarget)+1;t=null!=(s=a[n])?s:a[0];break}case"ArrowLeft":{var r;const n=a.indexOf(e.currentTarget)-1;t=null!=(r=a[n])?r:a[a.length-1];break}}null==(n=t)||n.focus()};return(0,_.jsx)("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,r.Z)("tabs",{"tabs--block":t},n),children:c.map((e=>{let{value:n,label:t,attributes:l}=e;return(0,_.jsx)("li",{role:"tab",tabIndex:s===n?0:-1,"aria-selected":s===n,ref:e=>a.push(e),onKeyDown:u,onClick:d,...l,className:(0,r.Z)("tabs__item",j.tabItem,null==l?void 0:l.className,{"tabs__item--active":s===n}),children:null!=t?t:n},n)}))})}function v(e){let{lazy:n,children:t,selectedValue:r}=e;const l=(Array.isArray(t)?t:[t]).filter(Boolean);if(n){const e=l.find((e=>e.props.value===r));return e?(0,s.cloneElement)(e,{className:"margin-top--md"}):null}return(0,_.jsx)("div",{className:"margin-top--md",children:l.map(((e,n)=>(0,s.cloneElement)(e,{key:n,hidden:e.props.value!==r})))})}function g(e){const n=x(e);return(0,_.jsxs)("div",{className:(0,r.Z)("tabs-container",j.tabList),children:[(0,_.jsx)(b,{...e,...n}),(0,_.jsx)(v,{...e,...n})]})}function y(e){const n=(0,m.Z)();return(0,_.jsx)(g,{...e,children:u(e.children)},String(n))}},1151:function(e,n,t){t.d(n,{Z:function(){return c},a:function(){return i}});var s=t(7294);const r={},l=s.createContext(r);function i(e){const n=s.useContext(l);return s.useMemo((function(){return"function"==typeof e?e(n):{...n,...e}}),[n,e])}function c(e){let n;return n=e.disableParentContext?"function"==typeof e.components?e.components(r):e.components||r:i(e.components),s.createElement(l.Provider,{value:n},e.children)}}}]);