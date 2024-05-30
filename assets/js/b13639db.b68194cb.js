"use strict";(self.webpackChunkbento=self.webpackChunkbento||[]).push([[875],{7729:function(e,n,t){t.r(n),t.d(n,{assets:function(){return d},contentTitle:function(){return a},default:function(){return p},frontMatter:function(){return c},metadata:function(){return o},toc:function(){return u}});var r=t(5893),s=t(1151),l=t(4866),i=t(5162);const c={title:"aws_s3",slug:"aws_s3",type:"cache",status:"stable",name:"aws_s3"},a=void 0,o={id:"components/caches/aws_s3",title:"aws_s3",description:"\x3c!--",source:"@site/docs/components/caches/aws_s3.md",sourceDirName:"components/caches",slug:"/components/caches/aws_s3",permalink:"/bento/docs/components/caches/aws_s3",draft:!1,unlisted:!1,editUrl:"https://github.com/warpstreamlabs/bento/edit/main/website/docs/components/caches/aws_s3.md",tags:[],version:"current",frontMatter:{title:"aws_s3",slug:"aws_s3",type:"cache",status:"stable",name:"aws_s3"},sidebar:"docs",previous:{title:"aws_dynamodb",permalink:"/bento/docs/components/caches/aws_dynamodb"},next:{title:"couchbase",permalink:"/bento/docs/components/caches/couchbase"}},d={},u=[{value:"Fields",id:"fields",level:2},{value:"<code>bucket</code>",id:"bucket",level:3},{value:"<code>content_type</code>",id:"content_type",level:3},{value:"<code>force_path_style_urls</code>",id:"force_path_style_urls",level:3},{value:"<code>retries</code>",id:"retries",level:3},{value:"<code>retries.initial_interval</code>",id:"retriesinitial_interval",level:3},{value:"<code>retries.max_interval</code>",id:"retriesmax_interval",level:3},{value:"<code>retries.max_elapsed_time</code>",id:"retriesmax_elapsed_time",level:3},{value:"<code>region</code>",id:"region",level:3},{value:"<code>endpoint</code>",id:"endpoint",level:3},{value:"<code>credentials</code>",id:"credentials",level:3},{value:"<code>credentials.profile</code>",id:"credentialsprofile",level:3},{value:"<code>credentials.id</code>",id:"credentialsid",level:3},{value:"<code>credentials.secret</code>",id:"credentialssecret",level:3},{value:"<code>credentials.token</code>",id:"credentialstoken",level:3},{value:"<code>credentials.from_ec2_role</code>",id:"credentialsfrom_ec2_role",level:3},{value:"<code>credentials.role</code>",id:"credentialsrole",level:3},{value:"<code>credentials.role_external_id</code>",id:"credentialsrole_external_id",level:3}];function h(e){const n={a:"a",admonition:"admonition",br:"br",code:"code",h2:"h2",h3:"h3",p:"p",pre:"pre",...(0,s.a)(),...e.components};return(0,r.jsxs)(r.Fragment,{children:[(0,r.jsx)(n.p,{children:"Stores each item in an S3 bucket as a file, where an item ID is the path of the item within the bucket."}),"\n",(0,r.jsx)(n.p,{children:"Introduced in version 3.36.0."}),"\n",(0,r.jsxs)(l.Z,{defaultValue:"common",values:[{label:"Common",value:"common"},{label:"Advanced",value:"advanced"}],children:[(0,r.jsx)(i.Z,{value:"common",children:(0,r.jsx)(n.pre,{children:(0,r.jsx)(n.code,{className:"language-yml",children:'# Common config fields, showing default values\nlabel: ""\naws_s3:\n  bucket: "" # No default (required)\n  content_type: application/octet-stream\n'})})}),(0,r.jsx)(i.Z,{value:"advanced",children:(0,r.jsx)(n.pre,{children:(0,r.jsx)(n.code,{className:"language-yml",children:'# All config fields, showing default values\nlabel: ""\naws_s3:\n  bucket: "" # No default (required)\n  content_type: application/octet-stream\n  force_path_style_urls: false\n  retries:\n    initial_interval: 1s\n    max_interval: 5s\n    max_elapsed_time: 30s\n  region: ""\n  endpoint: ""\n  credentials:\n    profile: ""\n    id: ""\n    secret: ""\n    token: ""\n    from_ec2_role: false\n    role: ""\n    role_external_id: ""\n'})})})]}),"\n",(0,r.jsx)(n.p,{children:"It is not possible to atomically upload S3 objects exclusively when the target does not already exist, therefore this cache is not suitable for deduplication."}),"\n",(0,r.jsx)(n.h2,{id:"fields",children:"Fields"}),"\n",(0,r.jsx)(n.h3,{id:"bucket",children:(0,r.jsx)(n.code,{children:"bucket"})}),"\n",(0,r.jsx)(n.p,{children:"The S3 bucket to store items in."}),"\n",(0,r.jsxs)(n.p,{children:["Type: ",(0,r.jsx)(n.code,{children:"string"})]}),"\n",(0,r.jsx)(n.h3,{id:"content_type",children:(0,r.jsx)(n.code,{children:"content_type"})}),"\n",(0,r.jsx)(n.p,{children:"The content type to set for each item."}),"\n",(0,r.jsxs)(n.p,{children:["Type: ",(0,r.jsx)(n.code,{children:"string"}),(0,r.jsx)(n.br,{}),"\n","Default: ",(0,r.jsx)(n.code,{children:'"application/octet-stream"'})]}),"\n",(0,r.jsx)(n.h3,{id:"force_path_style_urls",children:(0,r.jsx)(n.code,{children:"force_path_style_urls"})}),"\n",(0,r.jsx)(n.p,{children:"Forces the client API to use path style URLs, which helps when connecting to custom endpoints."}),"\n",(0,r.jsxs)(n.p,{children:["Type: ",(0,r.jsx)(n.code,{children:"bool"}),(0,r.jsx)(n.br,{}),"\n","Default: ",(0,r.jsx)(n.code,{children:"false"})]}),"\n",(0,r.jsx)(n.h3,{id:"retries",children:(0,r.jsx)(n.code,{children:"retries"})}),"\n",(0,r.jsx)(n.p,{children:"Determine time intervals and cut offs for retry attempts."}),"\n",(0,r.jsxs)(n.p,{children:["Type: ",(0,r.jsx)(n.code,{children:"object"})]}),"\n",(0,r.jsx)(n.h3,{id:"retriesinitial_interval",children:(0,r.jsx)(n.code,{children:"retries.initial_interval"})}),"\n",(0,r.jsx)(n.p,{children:"The initial period to wait between retry attempts."}),"\n",(0,r.jsxs)(n.p,{children:["Type: ",(0,r.jsx)(n.code,{children:"string"}),(0,r.jsx)(n.br,{}),"\n","Default: ",(0,r.jsx)(n.code,{children:'"1s"'})]}),"\n",(0,r.jsx)(n.pre,{children:(0,r.jsx)(n.code,{className:"language-yml",children:"# Examples\n\ninitial_interval: 50ms\n\ninitial_interval: 1s\n"})}),"\n",(0,r.jsx)(n.h3,{id:"retriesmax_interval",children:(0,r.jsx)(n.code,{children:"retries.max_interval"})}),"\n",(0,r.jsx)(n.p,{children:"The maximum period to wait between retry attempts"}),"\n",(0,r.jsxs)(n.p,{children:["Type: ",(0,r.jsx)(n.code,{children:"string"}),(0,r.jsx)(n.br,{}),"\n","Default: ",(0,r.jsx)(n.code,{children:'"5s"'})]}),"\n",(0,r.jsx)(n.pre,{children:(0,r.jsx)(n.code,{className:"language-yml",children:"# Examples\n\nmax_interval: 5s\n\nmax_interval: 1m\n"})}),"\n",(0,r.jsx)(n.h3,{id:"retriesmax_elapsed_time",children:(0,r.jsx)(n.code,{children:"retries.max_elapsed_time"})}),"\n",(0,r.jsx)(n.p,{children:"The maximum overall period of time to spend on retry attempts before the request is aborted."}),"\n",(0,r.jsxs)(n.p,{children:["Type: ",(0,r.jsx)(n.code,{children:"string"}),(0,r.jsx)(n.br,{}),"\n","Default: ",(0,r.jsx)(n.code,{children:'"30s"'})]}),"\n",(0,r.jsx)(n.pre,{children:(0,r.jsx)(n.code,{className:"language-yml",children:"# Examples\n\nmax_elapsed_time: 1m\n\nmax_elapsed_time: 1h\n"})}),"\n",(0,r.jsx)(n.h3,{id:"region",children:(0,r.jsx)(n.code,{children:"region"})}),"\n",(0,r.jsx)(n.p,{children:"The AWS region to target."}),"\n",(0,r.jsxs)(n.p,{children:["Type: ",(0,r.jsx)(n.code,{children:"string"}),(0,r.jsx)(n.br,{}),"\n","Default: ",(0,r.jsx)(n.code,{children:'""'})]}),"\n",(0,r.jsx)(n.h3,{id:"endpoint",children:(0,r.jsx)(n.code,{children:"endpoint"})}),"\n",(0,r.jsx)(n.p,{children:"Allows you to specify a custom endpoint for the AWS API."}),"\n",(0,r.jsxs)(n.p,{children:["Type: ",(0,r.jsx)(n.code,{children:"string"}),(0,r.jsx)(n.br,{}),"\n","Default: ",(0,r.jsx)(n.code,{children:'""'})]}),"\n",(0,r.jsx)(n.h3,{id:"credentials",children:(0,r.jsx)(n.code,{children:"credentials"})}),"\n",(0,r.jsxs)(n.p,{children:["Optional manual configuration of AWS credentials to use. More information can be found ",(0,r.jsx)(n.a,{href:"/docs/guides/cloud/aws",children:"in this document"}),"."]}),"\n",(0,r.jsxs)(n.p,{children:["Type: ",(0,r.jsx)(n.code,{children:"object"})]}),"\n",(0,r.jsx)(n.h3,{id:"credentialsprofile",children:(0,r.jsx)(n.code,{children:"credentials.profile"})}),"\n",(0,r.jsxs)(n.p,{children:["A profile from ",(0,r.jsx)(n.code,{children:"~/.aws/credentials"})," to use."]}),"\n",(0,r.jsxs)(n.p,{children:["Type: ",(0,r.jsx)(n.code,{children:"string"}),(0,r.jsx)(n.br,{}),"\n","Default: ",(0,r.jsx)(n.code,{children:'""'})]}),"\n",(0,r.jsx)(n.h3,{id:"credentialsid",children:(0,r.jsx)(n.code,{children:"credentials.id"})}),"\n",(0,r.jsx)(n.p,{children:"The ID of credentials to use."}),"\n",(0,r.jsxs)(n.p,{children:["Type: ",(0,r.jsx)(n.code,{children:"string"}),(0,r.jsx)(n.br,{}),"\n","Default: ",(0,r.jsx)(n.code,{children:'""'})]}),"\n",(0,r.jsx)(n.h3,{id:"credentialssecret",children:(0,r.jsx)(n.code,{children:"credentials.secret"})}),"\n",(0,r.jsx)(n.p,{children:"The secret for the credentials being used."}),"\n",(0,r.jsx)(n.admonition,{title:"Secret",type:"warning",children:(0,r.jsxs)(n.p,{children:["This field contains sensitive information that usually shouldn't be added to a config directly, read our ",(0,r.jsx)(n.a,{href:"/docs/configuration/secrets",children:"secrets page for more info"}),"."]})}),"\n",(0,r.jsxs)(n.p,{children:["Type: ",(0,r.jsx)(n.code,{children:"string"}),(0,r.jsx)(n.br,{}),"\n","Default: ",(0,r.jsx)(n.code,{children:'""'})]}),"\n",(0,r.jsx)(n.h3,{id:"credentialstoken",children:(0,r.jsx)(n.code,{children:"credentials.token"})}),"\n",(0,r.jsx)(n.p,{children:"The token for the credentials being used, required when using short term credentials."}),"\n",(0,r.jsxs)(n.p,{children:["Type: ",(0,r.jsx)(n.code,{children:"string"}),(0,r.jsx)(n.br,{}),"\n","Default: ",(0,r.jsx)(n.code,{children:'""'})]}),"\n",(0,r.jsx)(n.h3,{id:"credentialsfrom_ec2_role",children:(0,r.jsx)(n.code,{children:"credentials.from_ec2_role"})}),"\n",(0,r.jsxs)(n.p,{children:["Use the credentials of a host EC2 machine configured to assume ",(0,r.jsx)(n.a,{href:"https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-ec2.html",children:"an IAM role associated with the instance"}),"."]}),"\n",(0,r.jsxs)(n.p,{children:["Type: ",(0,r.jsx)(n.code,{children:"bool"}),(0,r.jsx)(n.br,{}),"\n","Default: ",(0,r.jsx)(n.code,{children:"false"}),(0,r.jsx)(n.br,{}),"\n","Requires version 4.2.0 or newer"]}),"\n",(0,r.jsx)(n.h3,{id:"credentialsrole",children:(0,r.jsx)(n.code,{children:"credentials.role"})}),"\n",(0,r.jsx)(n.p,{children:"A role ARN to assume."}),"\n",(0,r.jsxs)(n.p,{children:["Type: ",(0,r.jsx)(n.code,{children:"string"}),(0,r.jsx)(n.br,{}),"\n","Default: ",(0,r.jsx)(n.code,{children:'""'})]}),"\n",(0,r.jsx)(n.h3,{id:"credentialsrole_external_id",children:(0,r.jsx)(n.code,{children:"credentials.role_external_id"})}),"\n",(0,r.jsx)(n.p,{children:"An external ID to provide when assuming a role."}),"\n",(0,r.jsxs)(n.p,{children:["Type: ",(0,r.jsx)(n.code,{children:"string"}),(0,r.jsx)(n.br,{}),"\n","Default: ",(0,r.jsx)(n.code,{children:'""'})]})]})}function p(e={}){const{wrapper:n}={...(0,s.a)(),...e.components};return n?(0,r.jsx)(n,{...e,children:(0,r.jsx)(h,{...e})}):h(e)}},5162:function(e,n,t){t.d(n,{Z:function(){return i}});t(7294);var r=t(6010),s={tabItem:"tabItem_Ymn6"},l=t(5893);function i(e){let{children:n,hidden:t,className:i}=e;return(0,l.jsx)("div",{role:"tabpanel",className:(0,r.Z)(s.tabItem,i),hidden:t,children:n})}},4866:function(e,n,t){t.d(n,{Z:function(){return y}});var r=t(7294),s=t(6010),l=t(2466),i=t(6550),c=t(469),a=t(1980),o=t(7392),d=t(12);function u(e){var n,t;return null!=(n=null==(t=r.Children.toArray(e).filter((e=>"\n"!==e)).map((e=>{if(!e||(0,r.isValidElement)(e)&&function(e){const{props:n}=e;return!!n&&"object"==typeof n&&"value"in n}(e))return e;throw new Error("Docusaurus error: Bad <Tabs> child <"+("string"==typeof e.type?e.type:e.type.name)+'>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.')})))?void 0:t.filter(Boolean))?n:[]}function h(e){const{values:n,children:t}=e;return(0,r.useMemo)((()=>{const e=null!=n?n:function(e){return u(e).map((e=>{let{props:{value:n,label:t,attributes:r,default:s}}=e;return{value:n,label:t,attributes:r,default:s}}))}(t);return function(e){const n=(0,o.l)(e,((e,n)=>e.value===n.value));if(n.length>0)throw new Error('Docusaurus error: Duplicate values "'+n.map((e=>e.value)).join(", ")+'" found in <Tabs>. Every value needs to be unique.')}(e),e}),[n,t])}function p(e){let{value:n,tabValues:t}=e;return t.some((e=>e.value===n))}function x(e){let{queryString:n=!1,groupId:t}=e;const s=(0,i.k6)(),l=function(e){let{queryString:n=!1,groupId:t}=e;if("string"==typeof n)return n;if(!1===n)return null;if(!0===n&&!t)throw new Error('Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".');return null!=t?t:null}({queryString:n,groupId:t});return[(0,a._X)(l),(0,r.useCallback)((e=>{if(!l)return;const n=new URLSearchParams(s.location.search);n.set(l,e),s.replace({...s.location,search:n.toString()})}),[l,s])]}function m(e){const{defaultValue:n,queryString:t=!1,groupId:s}=e,l=h(e),[i,a]=(0,r.useState)((()=>function(e){var n;let{defaultValue:t,tabValues:r}=e;if(0===r.length)throw new Error("Docusaurus error: the <Tabs> component requires at least one <TabItem> children component");if(t){if(!p({value:t,tabValues:r}))throw new Error('Docusaurus error: The <Tabs> has a defaultValue "'+t+'" but none of its children has the corresponding value. Available values are: '+r.map((e=>e.value)).join(", ")+". If you intend to show no default tab, use defaultValue={null} instead.");return t}const s=null!=(n=r.find((e=>e.default)))?n:r[0];if(!s)throw new Error("Unexpected error: 0 tabValues");return s.value}({defaultValue:n,tabValues:l}))),[o,u]=x({queryString:t,groupId:s}),[m,f]=function(e){let{groupId:n}=e;const t=function(e){return e?"docusaurus.tab."+e:null}(n),[s,l]=(0,d.Nk)(t);return[s,(0,r.useCallback)((e=>{t&&l.set(e)}),[t,l])]}({groupId:s}),j=(()=>{const e=null!=o?o:m;return p({value:e,tabValues:l})?e:null})();(0,c.Z)((()=>{j&&a(j)}),[j]);return{selectedValue:i,selectValue:(0,r.useCallback)((e=>{if(!p({value:e,tabValues:l}))throw new Error("Can't select invalid tab value="+e);a(e),u(e),f(e)}),[u,f,l]),tabValues:l}}var f=t(2389),j={tabList:"tabList__CuJ",tabItem:"tabItem_LNqP"},b=t(5893);function v(e){let{className:n,block:t,selectedValue:r,selectValue:i,tabValues:c}=e;const a=[],{blockElementScrollPositionUntilNextRender:o}=(0,l.o5)(),d=e=>{const n=e.currentTarget,t=a.indexOf(n),s=c[t].value;s!==r&&(o(n),i(s))},u=e=>{var n;let t=null;switch(e.key){case"Enter":d(e);break;case"ArrowRight":{var r;const n=a.indexOf(e.currentTarget)+1;t=null!=(r=a[n])?r:a[0];break}case"ArrowLeft":{var s;const n=a.indexOf(e.currentTarget)-1;t=null!=(s=a[n])?s:a[a.length-1];break}}null==(n=t)||n.focus()};return(0,b.jsx)("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,s.Z)("tabs",{"tabs--block":t},n),children:c.map((e=>{let{value:n,label:t,attributes:l}=e;return(0,b.jsx)("li",{role:"tab",tabIndex:r===n?0:-1,"aria-selected":r===n,ref:e=>a.push(e),onKeyDown:u,onClick:d,...l,className:(0,s.Z)("tabs__item",j.tabItem,null==l?void 0:l.className,{"tabs__item--active":r===n}),children:null!=t?t:n},n)}))})}function _(e){let{lazy:n,children:t,selectedValue:s}=e;const l=(Array.isArray(t)?t:[t]).filter(Boolean);if(n){const e=l.find((e=>e.props.value===s));return e?(0,r.cloneElement)(e,{className:"margin-top--md"}):null}return(0,b.jsx)("div",{className:"margin-top--md",children:l.map(((e,n)=>(0,r.cloneElement)(e,{key:n,hidden:e.props.value!==s})))})}function g(e){const n=m(e);return(0,b.jsxs)("div",{className:(0,s.Z)("tabs-container",j.tabList),children:[(0,b.jsx)(v,{...e,...n}),(0,b.jsx)(_,{...e,...n})]})}function y(e){const n=(0,f.Z)();return(0,b.jsx)(g,{...e,children:u(e.children)},String(n))}},1151:function(e,n,t){t.d(n,{Z:function(){return c},a:function(){return i}});var r=t(7294);const s={},l=r.createContext(s);function i(e){const n=r.useContext(l);return r.useMemo((function(){return"function"==typeof e?e(n):{...n,...e}}),[n,e])}function c(e){let n;return n=e.disableParentContext?"function"==typeof e.components?e.components(s):e.components||s:i(e.components),r.createElement(l.Provider,{value:n},e.children)}}}]);