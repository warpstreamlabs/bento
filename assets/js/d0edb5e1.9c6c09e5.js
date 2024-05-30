"use strict";(self.webpackChunkbento=self.webpackChunkbento||[]).push([[3535],{244:function(e,n,r){r.r(n),r.d(n,{assets:function(){return d},contentTitle:function(){return c},default:function(){return p},frontMatter:function(){return a},metadata:function(){return i},toc:function(){return u}});var s=r(5893),t=r(1151),o=r(4866),l=r(5162);const a={title:"sql",slug:"sql",type:"processor",status:"deprecated",categories:["Integration"],name:"sql"},c=void 0,i={id:"components/processors/sql",title:"sql",description:"\x3c!--",source:"@site/docs/components/processors/sql.md",sourceDirName:"components/processors",slug:"/components/processors/sql",permalink:"/bento/docs/components/processors/sql",draft:!1,unlisted:!1,editUrl:"https://github.com/warpstreamlabs/bento/edit/main/website/docs/components/processors/sql.md",tags:[],version:"current",frontMatter:{title:"sql",slug:"sql",type:"processor",status:"deprecated",categories:["Integration"],name:"sql"},sidebar:"docs",previous:{title:"parquet",permalink:"/bento/docs/components/processors/parquet"},next:{title:"About",permalink:"/bento/docs/components/outputs/about"}},d={},u=[{value:"Alternatives",id:"alternatives",level:2},{value:"Fields",id:"fields",level:2},{value:"<code>driver</code>",id:"driver",level:3},{value:"<code>data_source_name</code>",id:"data_source_name",level:3},{value:"<code>query</code>",id:"query",level:3},{value:"<code>unsafe_dynamic_query</code>",id:"unsafe_dynamic_query",level:3},{value:"<code>args_mapping</code>",id:"args_mapping",level:3},{value:"<code>result_codec</code>",id:"result_codec",level:3}];function h(e){const n={a:"a",admonition:"admonition",br:"br",code:"code",h2:"h2",h3:"h3",p:"p",pre:"pre",table:"table",tbody:"tbody",td:"td",th:"th",thead:"thead",tr:"tr",...(0,t.a)(),...e.components};return(0,s.jsxs)(s.Fragment,{children:[(0,s.jsx)(n.admonition,{title:"DEPRECATED",type:"warning",children:(0,s.jsxs)(n.p,{children:["This component is deprecated and will be removed in the next major version release. Please consider moving onto ",(0,s.jsx)(n.a,{href:"#alternatives",children:"alternative components"}),"."]})}),"\n",(0,s.jsx)(n.p,{children:"Runs an arbitrary SQL query against a database and (optionally) returns the result as an array of objects, one for each row returned."}),"\n",(0,s.jsx)(n.p,{children:"Introduced in version 3.65.0."}),"\n",(0,s.jsxs)(o.Z,{defaultValue:"common",values:[{label:"Common",value:"common"},{label:"Advanced",value:"advanced"}],children:[(0,s.jsx)(l.Z,{value:"common",children:(0,s.jsx)(n.pre,{children:(0,s.jsx)(n.code,{className:"language-yml",children:'# Common config fields, showing default values\nlabel: ""\nsql:\n  driver: "" # No default (required)\n  data_source_name: "" # No default (required)\n  query: INSERT INTO footable (foo, bar, baz) VALUES (?, ?, ?); # No default (required)\n  args_mapping: root = [ this.cat.meow, this.doc.woofs[0] ] # No default (optional)\n  result_codec: none\n'})})}),(0,s.jsx)(l.Z,{value:"advanced",children:(0,s.jsx)(n.pre,{children:(0,s.jsx)(n.code,{className:"language-yml",children:'# All config fields, showing default values\nlabel: ""\nsql:\n  driver: "" # No default (required)\n  data_source_name: "" # No default (required)\n  query: INSERT INTO footable (foo, bar, baz) VALUES (?, ?, ?); # No default (required)\n  unsafe_dynamic_query: false\n  args_mapping: root = [ this.cat.meow, this.doc.woofs[0] ] # No default (optional)\n  result_codec: none\n'})})})]}),"\n",(0,s.jsxs)(n.p,{children:["If the query fails to execute then the message will remain unchanged and the error can be caught using error handling methods outlined ",(0,s.jsx)(n.a,{href:"/docs/configuration/error_handling",children:"here"}),"."]}),"\n",(0,s.jsx)(n.h2,{id:"alternatives",children:"Alternatives"}),"\n",(0,s.jsxs)(n.p,{children:["For basic inserts or select queries use either the ",(0,s.jsx)(n.a,{href:"/docs/components/processors/sql_insert",children:(0,s.jsx)(n.code,{children:"sql_insert"})})," or the ",(0,s.jsx)(n.a,{href:"/docs/components/processors/sql_select",children:(0,s.jsx)(n.code,{children:"sql_select"})})," processor. For more complex queries use the ",(0,s.jsx)(n.a,{href:"/docs/components/processors/sql_raw",children:(0,s.jsx)(n.code,{children:"sql_raw"})})," processor."]}),"\n",(0,s.jsx)(n.h2,{id:"fields",children:"Fields"}),"\n",(0,s.jsx)(n.h3,{id:"driver",children:(0,s.jsx)(n.code,{children:"driver"})}),"\n",(0,s.jsxs)(n.p,{children:["A database ",(0,s.jsx)(n.a,{href:"#drivers",children:"driver"})," to use."]}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"string"}),(0,s.jsx)(n.br,{}),"\n","Options: ",(0,s.jsx)(n.code,{children:"mysql"}),", ",(0,s.jsx)(n.code,{children:"postgres"}),", ",(0,s.jsx)(n.code,{children:"clickhouse"}),", ",(0,s.jsx)(n.code,{children:"mssql"}),", ",(0,s.jsx)(n.code,{children:"sqlite"}),", ",(0,s.jsx)(n.code,{children:"oracle"}),", ",(0,s.jsx)(n.code,{children:"snowflake"}),", ",(0,s.jsx)(n.code,{children:"trino"}),", ",(0,s.jsx)(n.code,{children:"gocosmos"}),"."]}),"\n",(0,s.jsx)(n.h3,{id:"data_source_name",children:(0,s.jsx)(n.code,{children:"data_source_name"})}),"\n",(0,s.jsx)(n.p,{children:"Data source name."}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"string"})]}),"\n",(0,s.jsx)(n.h3,{id:"query",children:(0,s.jsx)(n.code,{children:"query"})}),"\n",(0,s.jsxs)(n.p,{children:["The query to execute. The style of placeholder to use depends on the driver, some drivers require question marks (",(0,s.jsx)(n.code,{children:"?"}),") whereas others expect incrementing dollar signs (",(0,s.jsx)(n.code,{children:"$1"}),", ",(0,s.jsx)(n.code,{children:"$2"}),", and so on) or colons (",(0,s.jsx)(n.code,{children:":1"}),", ",(0,s.jsx)(n.code,{children:":2"})," and so on). The style to use is outlined in this table:"]}),"\n",(0,s.jsxs)(n.table,{children:[(0,s.jsx)(n.thead,{children:(0,s.jsxs)(n.tr,{children:[(0,s.jsx)(n.th,{children:"Driver"}),(0,s.jsx)(n.th,{children:"Placeholder Style"})]})}),(0,s.jsxs)(n.tbody,{children:[(0,s.jsxs)(n.tr,{children:[(0,s.jsx)(n.td,{children:(0,s.jsx)(n.code,{children:"clickhouse"})}),(0,s.jsx)(n.td,{children:"Dollar sign"})]}),(0,s.jsxs)(n.tr,{children:[(0,s.jsx)(n.td,{children:(0,s.jsx)(n.code,{children:"mysql"})}),(0,s.jsx)(n.td,{children:"Question mark"})]}),(0,s.jsxs)(n.tr,{children:[(0,s.jsx)(n.td,{children:(0,s.jsx)(n.code,{children:"postgres"})}),(0,s.jsx)(n.td,{children:"Dollar sign"})]}),(0,s.jsxs)(n.tr,{children:[(0,s.jsx)(n.td,{children:(0,s.jsx)(n.code,{children:"mssql"})}),(0,s.jsx)(n.td,{children:"Question mark"})]}),(0,s.jsxs)(n.tr,{children:[(0,s.jsx)(n.td,{children:(0,s.jsx)(n.code,{children:"sqlite"})}),(0,s.jsx)(n.td,{children:"Question mark"})]}),(0,s.jsxs)(n.tr,{children:[(0,s.jsx)(n.td,{children:(0,s.jsx)(n.code,{children:"oracle"})}),(0,s.jsx)(n.td,{children:"Colon"})]}),(0,s.jsxs)(n.tr,{children:[(0,s.jsx)(n.td,{children:(0,s.jsx)(n.code,{children:"snowflake"})}),(0,s.jsx)(n.td,{children:"Question mark"})]}),(0,s.jsxs)(n.tr,{children:[(0,s.jsx)(n.td,{children:(0,s.jsx)(n.code,{children:"trino"})}),(0,s.jsx)(n.td,{children:"Question mark"})]}),(0,s.jsxs)(n.tr,{children:[(0,s.jsx)(n.td,{children:(0,s.jsx)(n.code,{children:"gocosmos"})}),(0,s.jsx)(n.td,{children:"Colon"})]})]})]}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"string"})]}),"\n",(0,s.jsx)(n.pre,{children:(0,s.jsx)(n.code,{className:"language-yml",children:"# Examples\n\nquery: INSERT INTO footable (foo, bar, baz) VALUES (?, ?, ?);\n"})}),"\n",(0,s.jsx)(n.h3,{id:"unsafe_dynamic_query",children:(0,s.jsx)(n.code,{children:"unsafe_dynamic_query"})}),"\n",(0,s.jsxs)(n.p,{children:["Whether to enable ",(0,s.jsx)(n.a,{href:"/docs/configuration/interpolation/#bloblang-queries",children:"interpolation functions"})," in the query. Great care should be made to ensure your queries are defended against injection attacks."]}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"bool"}),(0,s.jsx)(n.br,{}),"\n","Default: ",(0,s.jsx)(n.code,{children:"false"})]}),"\n",(0,s.jsx)(n.h3,{id:"args_mapping",children:(0,s.jsx)(n.code,{children:"args_mapping"})}),"\n",(0,s.jsxs)(n.p,{children:["An optional ",(0,s.jsx)(n.a,{href:"/docs/guides/bloblang/about",children:"Bloblang mapping"})," which should evaluate to an array of values matching in size to the number of placeholder arguments in the field ",(0,s.jsx)(n.code,{children:"query"}),"."]}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"string"})]}),"\n",(0,s.jsx)(n.pre,{children:(0,s.jsx)(n.code,{className:"language-yml",children:'# Examples\n\nargs_mapping: root = [ this.cat.meow, this.doc.woofs[0] ]\n\nargs_mapping: root = [ meta("user.id") ]\n'})}),"\n",(0,s.jsx)(n.h3,{id:"result_codec",children:(0,s.jsx)(n.code,{children:"result_codec"})}),"\n",(0,s.jsx)(n.p,{children:"Result codec."}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"string"}),(0,s.jsx)(n.br,{}),"\n","Default: ",(0,s.jsx)(n.code,{children:'"none"'})]})]})}function p(e={}){const{wrapper:n}={...(0,t.a)(),...e.components};return n?(0,s.jsx)(n,{...e,children:(0,s.jsx)(h,{...e})}):h(e)}},5162:function(e,n,r){r.d(n,{Z:function(){return l}});r(7294);var s=r(6010),t={tabItem:"tabItem_Ymn6"},o=r(5893);function l(e){let{children:n,hidden:r,className:l}=e;return(0,o.jsx)("div",{role:"tabpanel",className:(0,s.Z)(t.tabItem,l),hidden:r,children:n})}},4866:function(e,n,r){r.d(n,{Z:function(){return q}});var s=r(7294),t=r(6010),o=r(2466),l=r(6550),a=r(469),c=r(1980),i=r(7392),d=r(12);function u(e){var n,r;return null!=(n=null==(r=s.Children.toArray(e).filter((e=>"\n"!==e)).map((e=>{if(!e||(0,s.isValidElement)(e)&&function(e){const{props:n}=e;return!!n&&"object"==typeof n&&"value"in n}(e))return e;throw new Error("Docusaurus error: Bad <Tabs> child <"+("string"==typeof e.type?e.type:e.type.name)+'>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.')})))?void 0:r.filter(Boolean))?n:[]}function h(e){const{values:n,children:r}=e;return(0,s.useMemo)((()=>{const e=null!=n?n:function(e){return u(e).map((e=>{let{props:{value:n,label:r,attributes:s,default:t}}=e;return{value:n,label:r,attributes:s,default:t}}))}(r);return function(e){const n=(0,i.l)(e,((e,n)=>e.value===n.value));if(n.length>0)throw new Error('Docusaurus error: Duplicate values "'+n.map((e=>e.value)).join(", ")+'" found in <Tabs>. Every value needs to be unique.')}(e),e}),[n,r])}function p(e){let{value:n,tabValues:r}=e;return r.some((e=>e.value===n))}function m(e){let{queryString:n=!1,groupId:r}=e;const t=(0,l.k6)(),o=function(e){let{queryString:n=!1,groupId:r}=e;if("string"==typeof n)return n;if(!1===n)return null;if(!0===n&&!r)throw new Error('Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".');return null!=r?r:null}({queryString:n,groupId:r});return[(0,c._X)(o),(0,s.useCallback)((e=>{if(!o)return;const n=new URLSearchParams(t.location.search);n.set(o,e),t.replace({...t.location,search:n.toString()})}),[o,t])]}function x(e){const{defaultValue:n,queryString:r=!1,groupId:t}=e,o=h(e),[l,c]=(0,s.useState)((()=>function(e){var n;let{defaultValue:r,tabValues:s}=e;if(0===s.length)throw new Error("Docusaurus error: the <Tabs> component requires at least one <TabItem> children component");if(r){if(!p({value:r,tabValues:s}))throw new Error('Docusaurus error: The <Tabs> has a defaultValue "'+r+'" but none of its children has the corresponding value. Available values are: '+s.map((e=>e.value)).join(", ")+". If you intend to show no default tab, use defaultValue={null} instead.");return r}const t=null!=(n=s.find((e=>e.default)))?n:s[0];if(!t)throw new Error("Unexpected error: 0 tabValues");return t.value}({defaultValue:n,tabValues:o}))),[i,u]=m({queryString:r,groupId:t}),[x,f]=function(e){let{groupId:n}=e;const r=function(e){return e?"docusaurus.tab."+e:null}(n),[t,o]=(0,d.Nk)(r);return[t,(0,s.useCallback)((e=>{r&&o.set(e)}),[r,o])]}({groupId:t}),j=(()=>{const e=null!=i?i:x;return p({value:e,tabValues:o})?e:null})();(0,a.Z)((()=>{j&&c(j)}),[j]);return{selectedValue:l,selectValue:(0,s.useCallback)((e=>{if(!p({value:e,tabValues:o}))throw new Error("Can't select invalid tab value="+e);c(e),u(e),f(e)}),[u,f,o]),tabValues:o}}var f=r(2389),j={tabList:"tabList__CuJ",tabItem:"tabItem_LNqP"},b=r(5893);function v(e){let{className:n,block:r,selectedValue:s,selectValue:l,tabValues:a}=e;const c=[],{blockElementScrollPositionUntilNextRender:i}=(0,o.o5)(),d=e=>{const n=e.currentTarget,r=c.indexOf(n),t=a[r].value;t!==s&&(i(n),l(t))},u=e=>{var n;let r=null;switch(e.key){case"Enter":d(e);break;case"ArrowRight":{var s;const n=c.indexOf(e.currentTarget)+1;r=null!=(s=c[n])?s:c[0];break}case"ArrowLeft":{var t;const n=c.indexOf(e.currentTarget)-1;r=null!=(t=c[n])?t:c[c.length-1];break}}null==(n=r)||n.focus()};return(0,b.jsx)("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,t.Z)("tabs",{"tabs--block":r},n),children:a.map((e=>{let{value:n,label:r,attributes:o}=e;return(0,b.jsx)("li",{role:"tab",tabIndex:s===n?0:-1,"aria-selected":s===n,ref:e=>c.push(e),onKeyDown:u,onClick:d,...o,className:(0,t.Z)("tabs__item",j.tabItem,null==o?void 0:o.className,{"tabs__item--active":s===n}),children:null!=r?r:n},n)}))})}function g(e){let{lazy:n,children:r,selectedValue:t}=e;const o=(Array.isArray(r)?r:[r]).filter(Boolean);if(n){const e=o.find((e=>e.props.value===t));return e?(0,s.cloneElement)(e,{className:"margin-top--md"}):null}return(0,b.jsx)("div",{className:"margin-top--md",children:o.map(((e,n)=>(0,s.cloneElement)(e,{key:n,hidden:e.props.value!==t})))})}function y(e){const n=x(e);return(0,b.jsxs)("div",{className:(0,t.Z)("tabs-container",j.tabList),children:[(0,b.jsx)(v,{...e,...n}),(0,b.jsx)(g,{...e,...n})]})}function q(e){const n=(0,f.Z)();return(0,b.jsx)(y,{...e,children:u(e.children)},String(n))}},1151:function(e,n,r){r.d(n,{Z:function(){return a},a:function(){return l}});var s=r(7294);const t={},o=s.createContext(t);function l(e){const n=s.useContext(o);return s.useMemo((function(){return"function"==typeof e?e(n):{...n,...e}}),[n,e])}function a(e){let n;return n=e.disableParentContext?"function"==typeof e.components?e.components(t):e.components||t:l(e.components),s.createElement(o.Provider,{value:n},e.children)}}}]);