/*! For license information please see 4ac72d28.bbfb92fd.js.LICENSE.txt */
(self.webpackChunkbento=self.webpackChunkbento||[]).push([[9906],{5876:function(e,t,n){"use strict";n.r(t),n.d(t,{assets:function(){return c},contentTitle:function(){return u},default:function(){return m},frontMatter:function(){return a},metadata:function(){return l},toc:function(){return p}});var s=n(5893),r=n(1151),o=n(5808),i=n(6971);const a={title:"Inputs",sidebar_label:"About"},u=void 0,l={id:"components/inputs/about",title:"Inputs",description:"An input is a source of data piped through an array of optional processors:",source:"@site/docs/components/inputs/about.md",sourceDirName:"components/inputs",slug:"/components/inputs/about",permalink:"/bento/docs/components/inputs/about",draft:!1,unlisted:!1,editUrl:"https://github.com/warpstreamlabs/bento/edit/main/website/docs/components/inputs/about.md",tags:[],version:"current",frontMatter:{title:"Inputs",sidebar_label:"About"},sidebar:"docs",previous:{title:"HTTP",permalink:"/bento/docs/components/http/about"},next:{title:"amqp_0_9",permalink:"/bento/docs/components/inputs/amqp_0_9"}},c={},p=[{value:"Brokering",id:"brokering",level:2},{value:"Labels",id:"labels",level:2},{value:"Sequential Reads",id:"sequential-reads",level:3},{value:"Generating Messages",id:"generating-messages",level:2},{value:"Categories",id:"categories",level:2}];function d(e){const t={a:"a",code:"code",h2:"h2",h3:"h3",p:"p",pre:"pre",...(0,r.a)(),...e.components};return(0,s.jsxs)(s.Fragment,{children:[(0,s.jsxs)(t.p,{children:["An input is a source of data piped through an array of optional ",(0,s.jsx)(t.a,{href:"/docs/components/processors/about",children:"processors"}),":"]}),"\n",(0,s.jsx)(t.pre,{children:(0,s.jsx)(t.code,{className:"language-yaml",children:'input:\n  label: my_redis_input\n\n  redis_streams:\n    url: tcp://localhost:6379\n    streams:\n      - bento_stream\n    body_key: body\n    consumer_group: bento_group\n\n  # Optional list of processing steps\n  processors:\n   - mapping: |\n       root.document = this.without("links")\n       root.link_count = this.links.length()\n'})}),"\n",(0,s.jsxs)(t.p,{children:["Some inputs have a logical end, for example a ",(0,s.jsxs)(t.a,{href:"/docs/components/inputs/csv",children:[(0,s.jsx)(t.code,{children:"csv"})," input"]})," ends once the last row is consumed, when this happens the input gracefully terminates and Bento will shut itself down once all messages have been processed fully."]}),"\n",(0,s.jsxs)(t.p,{children:["It's also possible to specify a logical end for an input that otherwise doesn't have one with the ",(0,s.jsxs)(t.a,{href:"/docs/components/inputs/read_until",children:[(0,s.jsx)(t.code,{children:"read_until"})," input"]}),", which checks a condition against each consumed message in order to determine whether it should be the last."]}),"\n",(0,s.jsx)(t.h2,{id:"brokering",children:"Brokering"}),"\n",(0,s.jsxs)(t.p,{children:["Only one input is configured at the root of a Bento config. However, the root input can be a ",(0,s.jsx)(t.a,{href:"/docs/components/inputs/broker",children:"broker"})," which combines multiple inputs and merges the streams:"]}),"\n",(0,s.jsx)(t.pre,{children:(0,s.jsx)(t.code,{className:"language-yaml",children:"input:\n  broker:\n    inputs:\n      - kafka:\n          addresses: [ TODO ]\n          topics: [ foo, bar ]\n          consumer_group: foogroup\n\n      - redis_streams:\n          url: tcp://localhost:6379\n          streams:\n            - bento_stream\n          body_key: body\n          consumer_group: bento_group\n"})}),"\n",(0,s.jsx)(t.h2,{id:"labels",children:"Labels"}),"\n",(0,s.jsxs)(t.p,{children:["Inputs have an optional field ",(0,s.jsx)(t.code,{children:"label"})," that can uniquely identify them in observability data such as metrics and logs. This can be useful when running configs with multiple inputs, otherwise their metrics labels will be generated based on their composition. For more information check out the ",(0,s.jsx)(t.a,{href:"/docs/components/metrics/about",children:"metrics documentation"}),"."]}),"\n",(0,s.jsx)(t.h3,{id:"sequential-reads",children:"Sequential Reads"}),"\n",(0,s.jsxs)(t.p,{children:["Sometimes it's useful to consume a sequence of inputs, where an input is only consumed once its predecessor is drained fully, you can achieve this with the ",(0,s.jsxs)(t.a,{href:"/docs/components/inputs/sequence",children:[(0,s.jsx)(t.code,{children:"sequence"})," input"]}),"."]}),"\n",(0,s.jsx)(t.h2,{id:"generating-messages",children:"Generating Messages"}),"\n",(0,s.jsxs)(t.p,{children:["It's possible to generate data with Bento using the ",(0,s.jsxs)(t.a,{href:"/docs/components/inputs/generate",children:[(0,s.jsx)(t.code,{children:"generate"})," input"]}),", which is also a convenient way to trigger scheduled pipelines."]}),"\n","\n",(0,s.jsx)(t.h2,{id:"categories",children:"Categories"}),"\n",(0,s.jsx)(o.Z,{type:"inputs"}),"\n","\n","\n",(0,s.jsx)(i.Z,{type:"inputs"})]})}function m(e={}){const{wrapper:t}={...(0,r.a)(),...e.components};return t?(0,s.jsx)(t,{...e,children:(0,s.jsx)(d,{...e})}):d(e)}},5162:function(e,t,n){"use strict";n.d(t,{Z:function(){return i}});n(7294);var s=n(6010),r={tabItem:"tabItem_Ymn6"},o=n(5893);function i(e){let{children:t,hidden:n,className:i}=e;return(0,o.jsx)("div",{role:"tabpanel",className:(0,s.Z)(r.tabItem,i),hidden:n,children:t})}},4866:function(e,t,n){"use strict";n.d(t,{Z:function(){return j}});var s=n(7294),r=n(6010),o=n(2466),i=n(6550),a=n(469),u=n(1980),l=n(7392),c=n(12);function p(e){var t,n;return null!=(t=null==(n=s.Children.toArray(e).filter((e=>"\n"!==e)).map((e=>{if(!e||(0,s.isValidElement)(e)&&function(e){const{props:t}=e;return!!t&&"object"==typeof t&&"value"in t}(e))return e;throw new Error("Docusaurus error: Bad <Tabs> child <"+("string"==typeof e.type?e.type:e.type.name)+'>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.')})))?void 0:n.filter(Boolean))?t:[]}function d(e){const{values:t,children:n}=e;return(0,s.useMemo)((()=>{const e=null!=t?t:function(e){return p(e).map((e=>{let{props:{value:t,label:n,attributes:s,default:r}}=e;return{value:t,label:n,attributes:s,default:r}}))}(n);return function(e){const t=(0,l.l)(e,((e,t)=>e.value===t.value));if(t.length>0)throw new Error('Docusaurus error: Duplicate values "'+t.map((e=>e.value)).join(", ")+'" found in <Tabs>. Every value needs to be unique.')}(e),e}),[t,n])}function m(e){let{value:t,tabValues:n}=e;return n.some((e=>e.value===t))}function h(e){let{queryString:t=!1,groupId:n}=e;const r=(0,i.k6)(),o=function(e){let{queryString:t=!1,groupId:n}=e;if("string"==typeof t)return t;if(!1===t)return null;if(!0===t&&!n)throw new Error('Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".');return null!=n?n:null}({queryString:t,groupId:n});return[(0,u._X)(o),(0,s.useCallback)((e=>{if(!o)return;const t=new URLSearchParams(r.location.search);t.set(o,e),r.replace({...r.location,search:t.toString()})}),[o,r])]}function f(e){const{defaultValue:t,queryString:n=!1,groupId:r}=e,o=d(e),[i,u]=(0,s.useState)((()=>function(e){var t;let{defaultValue:n,tabValues:s}=e;if(0===s.length)throw new Error("Docusaurus error: the <Tabs> component requires at least one <TabItem> children component");if(n){if(!m({value:n,tabValues:s}))throw new Error('Docusaurus error: The <Tabs> has a defaultValue "'+n+'" but none of its children has the corresponding value. Available values are: '+s.map((e=>e.value)).join(", ")+". If you intend to show no default tab, use defaultValue={null} instead.");return n}const r=null!=(t=s.find((e=>e.default)))?t:s[0];if(!r)throw new Error("Unexpected error: 0 tabValues");return r.value}({defaultValue:t,tabValues:o}))),[l,p]=h({queryString:n,groupId:r}),[f,b]=function(e){let{groupId:t}=e;const n=function(e){return e?"docusaurus.tab."+e:null}(t),[r,o]=(0,c.Nk)(n);return[r,(0,s.useCallback)((e=>{n&&o.set(e)}),[n,o])]}({groupId:r}),g=(()=>{const e=null!=l?l:f;return m({value:e,tabValues:o})?e:null})();(0,a.Z)((()=>{g&&u(g)}),[g]);return{selectedValue:i,selectValue:(0,s.useCallback)((e=>{if(!m({value:e,tabValues:o}))throw new Error("Can't select invalid tab value="+e);u(e),p(e),b(e)}),[p,b,o]),tabValues:o}}var b=n(2389),g={tabList:"tabList__CuJ",tabItem:"tabItem_LNqP"},v=n(5893);function y(e){let{className:t,block:n,selectedValue:s,selectValue:i,tabValues:a}=e;const u=[],{blockElementScrollPositionUntilNextRender:l}=(0,o.o5)(),c=e=>{const t=e.currentTarget,n=u.indexOf(t),r=a[n].value;r!==s&&(l(t),i(r))},p=e=>{var t;let n=null;switch(e.key){case"Enter":c(e);break;case"ArrowRight":{var s;const t=u.indexOf(e.currentTarget)+1;n=null!=(s=u[t])?s:u[0];break}case"ArrowLeft":{var r;const t=u.indexOf(e.currentTarget)-1;n=null!=(r=u[t])?r:u[u.length-1];break}}null==(t=n)||t.focus()};return(0,v.jsx)("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,r.Z)("tabs",{"tabs--block":n},t),children:a.map((e=>{let{value:t,label:n,attributes:o}=e;return(0,v.jsx)("li",{role:"tab",tabIndex:s===t?0:-1,"aria-selected":s===t,ref:e=>u.push(e),onKeyDown:p,onClick:c,...o,className:(0,r.Z)("tabs__item",g.tabItem,null==o?void 0:o.className,{"tabs__item--active":s===t}),children:null!=n?n:t},t)}))})}function w(e){let{lazy:t,children:n,selectedValue:r}=e;const o=(Array.isArray(n)?n:[n]).filter(Boolean);if(t){const e=o.find((e=>e.props.value===r));return e?(0,s.cloneElement)(e,{className:"margin-top--md"}):null}return(0,v.jsx)("div",{className:"margin-top--md",children:o.map(((e,t)=>(0,s.cloneElement)(e,{key:t,hidden:e.props.value!==r})))})}function x(e){const t=f(e);return(0,v.jsxs)("div",{className:(0,r.Z)("tabs-container",g.tabList),children:[(0,v.jsx)(y,{...e,...t}),(0,v.jsx)(w,{...e,...t})]})}function j(e){const t=(0,b.Z)();return(0,v.jsx)(x,{...e,children:p(e.children)},String(t))}},6971:function(e,t,n){"use strict";n.d(t,{Z:function(){return u}});n(7294);var s=n(2263),r=n(4184),o=n.n(r),i="componentList_axqy",a=n(5893);var u=function(e){let{type:t,singular:n}=e;const r=(0,s.Z)().siteConfig.customFields.components[t];return"string"!=typeof n&&(n=t,/s$/.test(n)&&(n=t.slice(0,-1))),(0,a.jsxs)("div",{className:"dropdown dropdown--hoverable",children:[(0,a.jsxs)("button",{className:"button button--outline button--primary",children:["Jump to ",n]}),(0,a.jsx)("ul",{className:o()(i,"dropdown__menu"),children:r.map((e=>(0,a.jsx)("li",{children:(0,a.jsx)("a",{className:"dropdown__link",href:"/docs/components/"+t+"/"+e.name,children:e.name})},e.name)))})]})}},5808:function(e,t,n){"use strict";n.d(t,{Z:function(){return p}});n(7294);var s=n(2263),r=n(4866),o=n(5162),i=n(9960),a="componentCard_EbTf",u=n(5893);var l=function(e){const{type:t,component:n}=e;return(0,u.jsx)(i.Z,{to:"/docs/components/"+t+"/"+n.name,className:a,children:(0,u.jsx)("strong",{children:n.name})})};let c={inputs:[{name:"Services",description:"Inputs that consume from storage or message streaming services."},{name:"Network",description:"Inputs that consume directly from low level network protocols."},{name:"AWS",description:"Inputs that consume from Amazon Web Services products."},{name:"GCP",description:"Inputs that consume from Google Cloud Platform services."},{name:"Azure",description:"Inputs that consume from Microsoft Azure services."},{name:"Social",description:"Inputs that consume from social applications and services."},{name:"Local",description:"Inputs that consume from the local machine/filesystem."},{name:"Utility",description:"Inputs that provide utility by generating data or combining/wrapping other inputs."}],buffers:[{name:"Windowing",description:"Buffers that provide message windowing capabilities."},{name:"Utility",description:"Buffers that are intended for niche but general use."}],processors:[{name:"Mapping",description:"Processors that specialize in restructuring messages."},{name:"Integration",description:"Processors that interact with external services."},{name:"Parsing",description:"Processors that specialize in translating messages from one format to another."},{name:"Composition",description:"Higher level processors that compose other processors and modify their behavior."},{name:"Utility",description:"Processors that provide general utility or do not fit in another category."}],outputs:[{name:"Services",description:"Outputs that write to storage or message streaming services."},{name:"Network",description:"Outputs that write directly to low level network protocols."},{name:"AWS",description:"Outputs that write to Amazon Web Services products."},{name:"GCP",description:"Outputs that write to Google Cloud Platform services."},{name:"Azure",description:"Outputs that write to Microsoft Azure services."},{name:"Social",description:"Outputs that write to social applications and services."},{name:"Local",description:"Outputs that write to the local machine/filesystem."},{name:"Utility",description:"Outputs that provide utility by combining/wrapping other outputs."}]};var p=function(e){let{type:t}=e;const n=(0,s.Z)().siteConfig.customFields.components[t];let i=c[t]||[],a={},p=[];for(let s=0;s<i.length;s++)p.push(i[s].name),a[i[s].name.toLowerCase()]={summary:i[s].description,items:[]};for(let s=0;s<n.length;s++){let e=n[s].categories;if(Array.isArray(e))for(let t=0;t<e.length;t++){let r=e[t].toLowerCase();void 0===a[r]?(p.push(r.charAt(0).toUpperCase()+r.slice(1)),a[r]={summary:"",items:[n[s]]}):a[r].items.push(n[s])}}return(0,u.jsx)(r.Z,{defaultValue:p[0].toLowerCase(),values:p.map((e=>({label:e,value:e.toLowerCase()}))),children:p.map((e=>(0,u.jsxs)(o.Z,{value:e.toLowerCase(),children:[(0,u.jsx)("p",{children:a[e.toLowerCase()].summary}),a[e.toLowerCase()].items.map(((e,n)=>(0,u.jsx)(l,{type:t,component:e},n)))]},e.toLowerCase())))})}},4184:function(e,t){var n;!function(){"use strict";var s={}.hasOwnProperty;function r(){for(var e=[],t=0;t<arguments.length;t++){var n=arguments[t];if(n){var o=typeof n;if("string"===o||"number"===o)e.push(n);else if(Array.isArray(n)){if(n.length){var i=r.apply(null,n);i&&e.push(i)}}else if("object"===o){if(n.toString!==Object.prototype.toString&&!n.toString.toString().includes("[native code]")){e.push(n.toString());continue}for(var a in n)s.call(n,a)&&n[a]&&e.push(a)}}}return e.join(" ")}e.exports?(r.default=r,e.exports=r):void 0===(n=function(){return r}.apply(t,[]))||(e.exports=n)}()},1151:function(e,t,n){"use strict";n.d(t,{Z:function(){return a},a:function(){return i}});var s=n(7294);const r={},o=s.createContext(r);function i(e){const t=s.useContext(o);return s.useMemo((function(){return"function"==typeof e?e(t):{...t,...e}}),[t,e])}function a(e){let t;return t=e.disableParentContext?"function"==typeof e.components?e.components(r):e.components||r:i(e.components),s.createElement(o.Provider,{value:t},e.children)}}}]);