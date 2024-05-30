"use strict";(self.webpackChunkbento=self.webpackChunkbento||[]).push([[8738],{9135:function(e,n,t){t.r(n),t.d(n,{assets:function(){return c},contentTitle:function(){return i},default:function(){return p},frontMatter:function(){return o},metadata:function(){return u},toc:function(){return d}});var a=t(5893),r=t(1151),s=t(4866),l=t(5162);const o={title:"file",slug:"file",type:"input",status:"stable",categories:["Local"],name:"file"},i=void 0,u={id:"components/inputs/file",title:"file",description:"\x3c!--",source:"@site/docs/components/inputs/file.md",sourceDirName:"components/inputs",slug:"/components/inputs/file",permalink:"/bento/docs/components/inputs/file",draft:!1,unlisted:!1,editUrl:"https://github.com/warpstreamlabs/bento/edit/main/website/docs/components/inputs/file.md",tags:[],version:"current",frontMatter:{title:"file",slug:"file",type:"input",status:"stable",categories:["Local"],name:"file"},sidebar:"docs",previous:{title:"dynamic",permalink:"/bento/docs/components/inputs/dynamic"},next:{title:"gcp_bigquery_select",permalink:"/bento/docs/components/inputs/gcp_bigquery_select"}},c={},d=[{value:"Metadata",id:"metadata",level:3},{value:"Fields",id:"fields",level:2},{value:"<code>paths</code>",id:"paths",level:3},{value:"<code>scanner</code>",id:"scanner",level:3},{value:"<code>delete_on_finish</code>",id:"delete_on_finish",level:3},{value:"<code>auto_replay_nacks</code>",id:"auto_replay_nacks",level:3},{value:"Examples",id:"examples",level:2}];function h(e){const n={a:"a",br:"br",code:"code",h2:"h2",h3:"h3",p:"p",pre:"pre",...(0,r.a)(),...e.components};return(0,a.jsxs)(a.Fragment,{children:[(0,a.jsx)(n.p,{children:"Consumes data from files on disk, emitting messages according to a chosen codec."}),"\n",(0,a.jsxs)(s.Z,{defaultValue:"common",values:[{label:"Common",value:"common"},{label:"Advanced",value:"advanced"}],children:[(0,a.jsx)(l.Z,{value:"common",children:(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{className:"language-yml",children:'# Common config fields, showing default values\ninput:\n  label: ""\n  file:\n    paths: [] # No default (required)\n    scanner:\n      lines: {}\n    auto_replay_nacks: true\n'})})}),(0,a.jsx)(l.Z,{value:"advanced",children:(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{className:"language-yml",children:'# All config fields, showing default values\ninput:\n  label: ""\n  file:\n    paths: [] # No default (required)\n    scanner:\n      lines: {}\n    delete_on_finish: false\n    auto_replay_nacks: true\n'})})})]}),"\n",(0,a.jsx)(n.h3,{id:"metadata",children:"Metadata"}),"\n",(0,a.jsx)(n.p,{children:"This input adds the following metadata fields to each message:"}),"\n",(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{className:"language-text",children:"- path\n- mod_time_unix\n- mod_time (RFC3339)\n"})}),"\n",(0,a.jsxs)(n.p,{children:["You can access these metadata fields using\n",(0,a.jsx)(n.a,{href:"/docs/configuration/interpolation#bloblang-queries",children:"function interpolation"}),"."]}),"\n",(0,a.jsx)(n.h2,{id:"fields",children:"Fields"}),"\n",(0,a.jsx)(n.h3,{id:"paths",children:(0,a.jsx)(n.code,{children:"paths"})}),"\n",(0,a.jsx)(n.p,{children:"A list of paths to consume sequentially. Glob patterns are supported, including super globs (double star)."}),"\n",(0,a.jsxs)(n.p,{children:["Type: ",(0,a.jsx)(n.code,{children:"array"})]}),"\n",(0,a.jsx)(n.h3,{id:"scanner",children:(0,a.jsx)(n.code,{children:"scanner"})}),"\n",(0,a.jsxs)(n.p,{children:["The ",(0,a.jsx)(n.a,{href:"/docs/components/scanners/about",children:"scanner"})," by which the stream of bytes consumed will be broken out into individual messages. Scanners are useful for processing large sources of data without holding the entirety of it within memory. For example, the ",(0,a.jsx)(n.code,{children:"csv"})," scanner allows you to process individual CSV rows without loading the entire CSV file in memory at once."]}),"\n",(0,a.jsxs)(n.p,{children:["Type: ",(0,a.jsx)(n.code,{children:"scanner"}),(0,a.jsx)(n.br,{}),"\n","Default: ",(0,a.jsx)(n.code,{children:'{"lines":{}}'}),(0,a.jsx)(n.br,{}),"\n","Requires version 4.25.0 or newer"]}),"\n",(0,a.jsx)(n.h3,{id:"delete_on_finish",children:(0,a.jsx)(n.code,{children:"delete_on_finish"})}),"\n",(0,a.jsx)(n.p,{children:"Whether to delete input files from the disk once they are fully consumed."}),"\n",(0,a.jsxs)(n.p,{children:["Type: ",(0,a.jsx)(n.code,{children:"bool"}),(0,a.jsx)(n.br,{}),"\n","Default: ",(0,a.jsx)(n.code,{children:"false"})]}),"\n",(0,a.jsx)(n.h3,{id:"auto_replay_nacks",children:(0,a.jsx)(n.code,{children:"auto_replay_nacks"})}),"\n",(0,a.jsxs)(n.p,{children:["Whether messages that are rejected (nacked) at the output level should be automatically replayed indefinitely, eventually resulting in back pressure if the cause of the rejections is persistent. If set to ",(0,a.jsx)(n.code,{children:"false"})," these messages will instead be deleted. Disabling auto replays can greatly improve memory efficiency of high throughput streams as the original shape of the data can be discarded immediately upon consumption and mutation."]}),"\n",(0,a.jsxs)(n.p,{children:["Type: ",(0,a.jsx)(n.code,{children:"bool"}),(0,a.jsx)(n.br,{}),"\n","Default: ",(0,a.jsx)(n.code,{children:"true"})]}),"\n",(0,a.jsx)(n.h2,{id:"examples",children:"Examples"}),"\n",(0,a.jsx)(s.Z,{defaultValue:"Read a Bunch of CSVs",values:[{label:"Read a Bunch of CSVs",value:"Read a Bunch of CSVs"}],children:(0,a.jsxs)(l.Z,{value:"Read a Bunch of CSVs",children:[(0,a.jsxs)(n.p,{children:["If we wished to consume a directory of CSV files as structured documents we can use a glob pattern and the ",(0,a.jsx)(n.code,{children:"csv"})," scanner:"]}),(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{className:"language-yaml",children:"input:\n  file:\n    paths: [ ./data/*.csv ]\n    scanner:\n      csv: {}\n"})})]})})]})}function p(e={}){const{wrapper:n}={...(0,r.a)(),...e.components};return n?(0,a.jsx)(n,{...e,children:(0,a.jsx)(h,{...e})}):h(e)}},5162:function(e,n,t){t.d(n,{Z:function(){return l}});t(7294);var a=t(6010),r={tabItem:"tabItem_Ymn6"},s=t(5893);function l(e){let{children:n,hidden:t,className:l}=e;return(0,s.jsx)("div",{role:"tabpanel",className:(0,a.Z)(r.tabItem,l),hidden:t,children:n})}},4866:function(e,n,t){t.d(n,{Z:function(){return w}});var a=t(7294),r=t(6010),s=t(2466),l=t(6550),o=t(469),i=t(1980),u=t(7392),c=t(12);function d(e){var n,t;return null!=(n=null==(t=a.Children.toArray(e).filter((e=>"\n"!==e)).map((e=>{if(!e||(0,a.isValidElement)(e)&&function(e){const{props:n}=e;return!!n&&"object"==typeof n&&"value"in n}(e))return e;throw new Error("Docusaurus error: Bad <Tabs> child <"+("string"==typeof e.type?e.type:e.type.name)+'>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.')})))?void 0:t.filter(Boolean))?n:[]}function h(e){const{values:n,children:t}=e;return(0,a.useMemo)((()=>{const e=null!=n?n:function(e){return d(e).map((e=>{let{props:{value:n,label:t,attributes:a,default:r}}=e;return{value:n,label:t,attributes:a,default:r}}))}(t);return function(e){const n=(0,u.l)(e,((e,n)=>e.value===n.value));if(n.length>0)throw new Error('Docusaurus error: Duplicate values "'+n.map((e=>e.value)).join(", ")+'" found in <Tabs>. Every value needs to be unique.')}(e),e}),[n,t])}function p(e){let{value:n,tabValues:t}=e;return t.some((e=>e.value===n))}function f(e){let{queryString:n=!1,groupId:t}=e;const r=(0,l.k6)(),s=function(e){let{queryString:n=!1,groupId:t}=e;if("string"==typeof n)return n;if(!1===n)return null;if(!0===n&&!t)throw new Error('Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".');return null!=t?t:null}({queryString:n,groupId:t});return[(0,i._X)(s),(0,a.useCallback)((e=>{if(!s)return;const n=new URLSearchParams(r.location.search);n.set(s,e),r.replace({...r.location,search:n.toString()})}),[s,r])]}function m(e){const{defaultValue:n,queryString:t=!1,groupId:r}=e,s=h(e),[l,i]=(0,a.useState)((()=>function(e){var n;let{defaultValue:t,tabValues:a}=e;if(0===a.length)throw new Error("Docusaurus error: the <Tabs> component requires at least one <TabItem> children component");if(t){if(!p({value:t,tabValues:a}))throw new Error('Docusaurus error: The <Tabs> has a defaultValue "'+t+'" but none of its children has the corresponding value. Available values are: '+a.map((e=>e.value)).join(", ")+". If you intend to show no default tab, use defaultValue={null} instead.");return t}const r=null!=(n=a.find((e=>e.default)))?n:a[0];if(!r)throw new Error("Unexpected error: 0 tabValues");return r.value}({defaultValue:n,tabValues:s}))),[u,d]=f({queryString:t,groupId:r}),[m,b]=function(e){let{groupId:n}=e;const t=function(e){return e?"docusaurus.tab."+e:null}(n),[r,s]=(0,c.Nk)(t);return[r,(0,a.useCallback)((e=>{t&&s.set(e)}),[t,s])]}({groupId:r}),v=(()=>{const e=null!=u?u:m;return p({value:e,tabValues:s})?e:null})();(0,o.Z)((()=>{v&&i(v)}),[v]);return{selectedValue:l,selectValue:(0,a.useCallback)((e=>{if(!p({value:e,tabValues:s}))throw new Error("Can't select invalid tab value="+e);i(e),d(e),b(e)}),[d,b,s]),tabValues:s}}var b=t(2389),v={tabList:"tabList__CuJ",tabItem:"tabItem_LNqP"},x=t(5893);function g(e){let{className:n,block:t,selectedValue:a,selectValue:l,tabValues:o}=e;const i=[],{blockElementScrollPositionUntilNextRender:u}=(0,s.o5)(),c=e=>{const n=e.currentTarget,t=i.indexOf(n),r=o[t].value;r!==a&&(u(n),l(r))},d=e=>{var n;let t=null;switch(e.key){case"Enter":c(e);break;case"ArrowRight":{var a;const n=i.indexOf(e.currentTarget)+1;t=null!=(a=i[n])?a:i[0];break}case"ArrowLeft":{var r;const n=i.indexOf(e.currentTarget)-1;t=null!=(r=i[n])?r:i[i.length-1];break}}null==(n=t)||n.focus()};return(0,x.jsx)("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,r.Z)("tabs",{"tabs--block":t},n),children:o.map((e=>{let{value:n,label:t,attributes:s}=e;return(0,x.jsx)("li",{role:"tab",tabIndex:a===n?0:-1,"aria-selected":a===n,ref:e=>i.push(e),onKeyDown:d,onClick:c,...s,className:(0,r.Z)("tabs__item",v.tabItem,null==s?void 0:s.className,{"tabs__item--active":a===n}),children:null!=t?t:n},n)}))})}function j(e){let{lazy:n,children:t,selectedValue:r}=e;const s=(Array.isArray(t)?t:[t]).filter(Boolean);if(n){const e=s.find((e=>e.props.value===r));return e?(0,a.cloneElement)(e,{className:"margin-top--md"}):null}return(0,x.jsx)("div",{className:"margin-top--md",children:s.map(((e,n)=>(0,a.cloneElement)(e,{key:n,hidden:e.props.value!==r})))})}function y(e){const n=m(e);return(0,x.jsxs)("div",{className:(0,r.Z)("tabs-container",v.tabList),children:[(0,x.jsx)(g,{...e,...n}),(0,x.jsx)(j,{...e,...n})]})}function w(e){const n=(0,b.Z)();return(0,x.jsx)(y,{...e,children:d(e.children)},String(n))}},1151:function(e,n,t){t.d(n,{Z:function(){return o},a:function(){return l}});var a=t(7294);const r={},s=a.createContext(r);function l(e){const n=a.useContext(s);return a.useMemo((function(){return"function"==typeof e?e(n):{...n,...e}}),[n,e])}function o(e){let n;return n=e.disableParentContext?"function"==typeof e.components?e.components(r):e.components||r:l(e.components),a.createElement(s.Provider,{value:n},e.children)}}}]);