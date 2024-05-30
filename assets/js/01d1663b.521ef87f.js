"use strict";(self.webpackChunkbento=self.webpackChunkbento||[]).push([[8598],{4776:function(e,t,n){n.r(t),n.d(t,{assets:function(){return i},contentTitle:function(){return s},default:function(){return h},frontMatter:function(){return u},metadata:function(){return c},toc:function(){return d}});var r=n(5893),l=n(1151),a=n(4866),o=n(5162);const u={title:"multilevel",slug:"multilevel",type:"cache",status:"stable",name:"multilevel"},s=void 0,c={id:"components/caches/multilevel",title:"multilevel",description:"\x3c!--",source:"@site/docs/components/caches/multilevel.md",sourceDirName:"components/caches",slug:"/components/caches/multilevel",permalink:"/bento/docs/components/caches/multilevel",draft:!1,unlisted:!1,editUrl:"https://github.com/warpstreamlabs/bento/edit/main/website/docs/components/caches/multilevel.md",tags:[],version:"current",frontMatter:{title:"multilevel",slug:"multilevel",type:"cache",status:"stable",name:"multilevel"},sidebar:"docs",previous:{title:"mongodb",permalink:"/bento/docs/components/caches/mongodb"},next:{title:"nats_kv",permalink:"/bento/docs/components/caches/nats_kv"}},i={},d=[{value:"Examples",id:"examples",level:2}];function m(e){const t={code:"code",h2:"h2",p:"p",pre:"pre",...(0,l.a)(),...e.components};return(0,r.jsxs)(r.Fragment,{children:[(0,r.jsx)(t.p,{children:"Combines multiple caches as levels, performing read-through and write-through operations across them."}),"\n",(0,r.jsx)(t.pre,{children:(0,r.jsx)(t.code,{className:"language-yml",children:'# Config fields, showing default values\nlabel: ""\nmultilevel: [] # No default (required)\n'})}),"\n",(0,r.jsx)(t.h2,{id:"examples",children:"Examples"}),"\n",(0,r.jsx)(a.Z,{defaultValue:"Hot and cold cache",values:[{label:"Hot and cold cache",value:"Hot and cold cache"}],children:(0,r.jsxs)(o.Z,{value:"Hot and cold cache",children:[(0,r.jsx)(t.p,{children:"The multilevel cache is useful for reducing traffic against a remote cache by routing it through a local cache. In the following example requests will only go through to the memcached server if the local memory cache is missing the key."}),(0,r.jsx)(t.pre,{children:(0,r.jsx)(t.code,{className:"language-yaml",children:"pipeline:\n  processors:\n    - branch:\n        processors:\n          - cache:\n              resource: leveled\n              operator: get\n              key: ${! json(\"key\") }\n          - catch:\n            - mapping: 'root = {\"err\":error()}'\n        result_map: 'root.result = this'\n\ncache_resources:\n  - label: leveled\n    multilevel: [ hot, cold ]\n\n  - label: hot\n    memory:\n      default_ttl: 60s\n\n  - label: cold\n    memcached:\n      addresses: [ TODO:11211 ]\n      default_ttl: 60s\n"})})]})})]})}function h(e={}){const{wrapper:t}={...(0,l.a)(),...e.components};return t?(0,r.jsx)(t,{...e,children:(0,r.jsx)(m,{...e})}):m(e)}},5162:function(e,t,n){n.d(t,{Z:function(){return o}});n(7294);var r=n(6010),l={tabItem:"tabItem_Ymn6"},a=n(5893);function o(e){let{children:t,hidden:n,className:o}=e;return(0,a.jsx)("div",{role:"tabpanel",className:(0,r.Z)(l.tabItem,o),hidden:n,children:t})}},4866:function(e,t,n){n.d(t,{Z:function(){return k}});var r=n(7294),l=n(6010),a=n(2466),o=n(6550),u=n(469),s=n(1980),c=n(7392),i=n(12);function d(e){var t,n;return null!=(t=null==(n=r.Children.toArray(e).filter((e=>"\n"!==e)).map((e=>{if(!e||(0,r.isValidElement)(e)&&function(e){const{props:t}=e;return!!t&&"object"==typeof t&&"value"in t}(e))return e;throw new Error("Docusaurus error: Bad <Tabs> child <"+("string"==typeof e.type?e.type:e.type.name)+'>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.')})))?void 0:n.filter(Boolean))?t:[]}function m(e){const{values:t,children:n}=e;return(0,r.useMemo)((()=>{const e=null!=t?t:function(e){return d(e).map((e=>{let{props:{value:t,label:n,attributes:r,default:l}}=e;return{value:t,label:n,attributes:r,default:l}}))}(n);return function(e){const t=(0,c.l)(e,((e,t)=>e.value===t.value));if(t.length>0)throw new Error('Docusaurus error: Duplicate values "'+t.map((e=>e.value)).join(", ")+'" found in <Tabs>. Every value needs to be unique.')}(e),e}),[t,n])}function h(e){let{value:t,tabValues:n}=e;return n.some((e=>e.value===t))}function p(e){let{queryString:t=!1,groupId:n}=e;const l=(0,o.k6)(),a=function(e){let{queryString:t=!1,groupId:n}=e;if("string"==typeof t)return t;if(!1===t)return null;if(!0===t&&!n)throw new Error('Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".');return null!=n?n:null}({queryString:t,groupId:n});return[(0,s._X)(a),(0,r.useCallback)((e=>{if(!a)return;const t=new URLSearchParams(l.location.search);t.set(a,e),l.replace({...l.location,search:t.toString()})}),[a,l])]}function f(e){const{defaultValue:t,queryString:n=!1,groupId:l}=e,a=m(e),[o,s]=(0,r.useState)((()=>function(e){var t;let{defaultValue:n,tabValues:r}=e;if(0===r.length)throw new Error("Docusaurus error: the <Tabs> component requires at least one <TabItem> children component");if(n){if(!h({value:n,tabValues:r}))throw new Error('Docusaurus error: The <Tabs> has a defaultValue "'+n+'" but none of its children has the corresponding value. Available values are: '+r.map((e=>e.value)).join(", ")+". If you intend to show no default tab, use defaultValue={null} instead.");return n}const l=null!=(t=r.find((e=>e.default)))?t:r[0];if(!l)throw new Error("Unexpected error: 0 tabValues");return l.value}({defaultValue:t,tabValues:a}))),[c,d]=p({queryString:n,groupId:l}),[f,b]=function(e){let{groupId:t}=e;const n=function(e){return e?"docusaurus.tab."+e:null}(t),[l,a]=(0,i.Nk)(n);return[l,(0,r.useCallback)((e=>{n&&a.set(e)}),[n,a])]}({groupId:l}),v=(()=>{const e=null!=c?c:f;return h({value:e,tabValues:a})?e:null})();(0,u.Z)((()=>{v&&s(v)}),[v]);return{selectedValue:o,selectValue:(0,r.useCallback)((e=>{if(!h({value:e,tabValues:a}))throw new Error("Can't select invalid tab value="+e);s(e),d(e),b(e)}),[d,b,a]),tabValues:a}}var b=n(2389),v={tabList:"tabList__CuJ",tabItem:"tabItem_LNqP"},g=n(5893);function x(e){let{className:t,block:n,selectedValue:r,selectValue:o,tabValues:u}=e;const s=[],{blockElementScrollPositionUntilNextRender:c}=(0,a.o5)(),i=e=>{const t=e.currentTarget,n=s.indexOf(t),l=u[n].value;l!==r&&(c(t),o(l))},d=e=>{var t;let n=null;switch(e.key){case"Enter":i(e);break;case"ArrowRight":{var r;const t=s.indexOf(e.currentTarget)+1;n=null!=(r=s[t])?r:s[0];break}case"ArrowLeft":{var l;const t=s.indexOf(e.currentTarget)-1;n=null!=(l=s[t])?l:s[s.length-1];break}}null==(t=n)||t.focus()};return(0,g.jsx)("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,l.Z)("tabs",{"tabs--block":n},t),children:u.map((e=>{let{value:t,label:n,attributes:a}=e;return(0,g.jsx)("li",{role:"tab",tabIndex:r===t?0:-1,"aria-selected":r===t,ref:e=>s.push(e),onKeyDown:d,onClick:i,...a,className:(0,l.Z)("tabs__item",v.tabItem,null==a?void 0:a.className,{"tabs__item--active":r===t}),children:null!=n?n:t},t)}))})}function y(e){let{lazy:t,children:n,selectedValue:l}=e;const a=(Array.isArray(n)?n:[n]).filter(Boolean);if(t){const e=a.find((e=>e.props.value===l));return e?(0,r.cloneElement)(e,{className:"margin-top--md"}):null}return(0,g.jsx)("div",{className:"margin-top--md",children:a.map(((e,t)=>(0,r.cloneElement)(e,{key:t,hidden:e.props.value!==l})))})}function w(e){const t=f(e);return(0,g.jsxs)("div",{className:(0,l.Z)("tabs-container",v.tabList),children:[(0,g.jsx)(x,{...e,...t}),(0,g.jsx)(y,{...e,...t})]})}function k(e){const t=(0,b.Z)();return(0,g.jsx)(w,{...e,children:d(e.children)},String(t))}},1151:function(e,t,n){n.d(t,{Z:function(){return u},a:function(){return o}});var r=n(7294);const l={},a=r.createContext(l);function o(e){const t=r.useContext(a);return r.useMemo((function(){return"function"==typeof e?e(t):{...t,...e}}),[t,e])}function u(e){let t;return t=e.disableParentContext?"function"==typeof e.components?e.components(l):e.components||l:o(e.components),r.createElement(a.Provider,{value:t},e.children)}}}]);