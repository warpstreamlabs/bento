"use strict";(self.webpackChunkbento=self.webpackChunkbento||[]).push([[8440],{691:function(e,n,t){t.r(n),t.d(n,{assets:function(){return i},contentTitle:function(){return o},default:function(){return d},frontMatter:function(){return s},metadata:function(){return l},toc:function(){return u}});var r=t(5893),a=t(1151);t(4866),t(5162);const s={title:"csv",slug:"csv",type:"scanner",status:"stable",name:"csv"},o=void 0,l={id:"components/scanners/csv",title:"csv",description:"\x3c!--",source:"@site/docs/components/scanners/csv.md",sourceDirName:"components/scanners",slug:"/components/scanners/csv",permalink:"/bento/docs/components/scanners/csv",draft:!1,unlisted:!1,editUrl:"https://github.com/warpstreamlabs/bento/edit/main/website/docs/components/scanners/csv.md",tags:[],version:"current",frontMatter:{title:"csv",slug:"csv",type:"scanner",status:"stable",name:"csv"},sidebar:"docs",previous:{title:"chunker",permalink:"/bento/docs/components/scanners/chunker"},next:{title:"decompress",permalink:"/bento/docs/components/scanners/decompress"}},i={},u=[{value:"Metadata",id:"metadata",level:3},{value:"Fields",id:"fields",level:2},{value:"<code>custom_delimiter</code>",id:"custom_delimiter",level:3},{value:"<code>parse_header_row</code>",id:"parse_header_row",level:3},{value:"<code>lazy_quotes</code>",id:"lazy_quotes",level:3},{value:"<code>continue_on_error</code>",id:"continue_on_error",level:3}];function c(e){const n={br:"br",code:"code",h2:"h2",h3:"h3",li:"li",p:"p",pre:"pre",ul:"ul",...(0,a.a)(),...e.components};return(0,r.jsxs)(r.Fragment,{children:[(0,r.jsx)(n.p,{children:"Consume comma-separated values row by row, including support for custom delimiters."}),"\n",(0,r.jsx)(n.pre,{children:(0,r.jsx)(n.code,{className:"language-yml",children:'# Config fields, showing default values\ncsv:\n  custom_delimiter: "" # No default (optional)\n  parse_header_row: true\n  lazy_quotes: false\n  continue_on_error: false\n'})}),"\n",(0,r.jsx)(n.h3,{id:"metadata",children:"Metadata"}),"\n",(0,r.jsx)(n.p,{children:"This scanner adds the following metadata to each message:"}),"\n",(0,r.jsxs)(n.ul,{children:["\n",(0,r.jsxs)(n.li,{children:[(0,r.jsx)(n.code,{children:"csv_row"})," The index of each row, beginning at 0."]}),"\n"]}),"\n",(0,r.jsx)(n.h2,{id:"fields",children:"Fields"}),"\n",(0,r.jsx)(n.h3,{id:"custom_delimiter",children:(0,r.jsx)(n.code,{children:"custom_delimiter"})}),"\n",(0,r.jsx)(n.p,{children:"Use a provided custom delimiter instead of the default comma."}),"\n",(0,r.jsxs)(n.p,{children:["Type: ",(0,r.jsx)(n.code,{children:"string"})]}),"\n",(0,r.jsx)(n.h3,{id:"parse_header_row",children:(0,r.jsx)(n.code,{children:"parse_header_row"})}),"\n",(0,r.jsx)(n.p,{children:"Whether to reference the first row as a header row. If set to true the output structure for messages will be an object where field keys are determined by the header row. Otherwise, each message will consist of an array of values from the corresponding CSV row."}),"\n",(0,r.jsxs)(n.p,{children:["Type: ",(0,r.jsx)(n.code,{children:"bool"}),(0,r.jsx)(n.br,{}),"\n","Default: ",(0,r.jsx)(n.code,{children:"true"})]}),"\n",(0,r.jsx)(n.h3,{id:"lazy_quotes",children:(0,r.jsx)(n.code,{children:"lazy_quotes"})}),"\n",(0,r.jsxs)(n.p,{children:["If set to ",(0,r.jsx)(n.code,{children:"true"}),", a quote may appear in an unquoted field and a non-doubled quote may appear in a quoted field."]}),"\n",(0,r.jsxs)(n.p,{children:["Type: ",(0,r.jsx)(n.code,{children:"bool"}),(0,r.jsx)(n.br,{}),"\n","Default: ",(0,r.jsx)(n.code,{children:"false"})]}),"\n",(0,r.jsx)(n.h3,{id:"continue_on_error",children:(0,r.jsx)(n.code,{children:"continue_on_error"})}),"\n",(0,r.jsx)(n.p,{children:"If a row fails to parse due to any error emit an empty message marked with the error and then continue consuming subsequent rows when possible. This can sometimes be useful in situations where input data contains individual rows which are malformed. However, when a row encounters a parsing error it is impossible to guarantee that following rows are valid, as this indicates that the input data is unreliable and could potentially emit misaligned rows."}),"\n",(0,r.jsxs)(n.p,{children:["Type: ",(0,r.jsx)(n.code,{children:"bool"}),(0,r.jsx)(n.br,{}),"\n","Default: ",(0,r.jsx)(n.code,{children:"false"})]})]})}function d(e={}){const{wrapper:n}={...(0,a.a)(),...e.components};return n?(0,r.jsx)(n,{...e,children:(0,r.jsx)(c,{...e})}):c(e)}},5162:function(e,n,t){t.d(n,{Z:function(){return o}});t(7294);var r=t(6010),a={tabItem:"tabItem_Ymn6"},s=t(5893);function o(e){let{children:n,hidden:t,className:o}=e;return(0,s.jsx)("div",{role:"tabpanel",className:(0,r.Z)(a.tabItem,o),hidden:t,children:n})}},4866:function(e,n,t){t.d(n,{Z:function(){return y}});var r=t(7294),a=t(6010),s=t(2466),o=t(6550),l=t(469),i=t(1980),u=t(7392),c=t(12);function d(e){var n,t;return null!=(n=null==(t=r.Children.toArray(e).filter((e=>"\n"!==e)).map((e=>{if(!e||(0,r.isValidElement)(e)&&function(e){const{props:n}=e;return!!n&&"object"==typeof n&&"value"in n}(e))return e;throw new Error("Docusaurus error: Bad <Tabs> child <"+("string"==typeof e.type?e.type:e.type.name)+'>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.')})))?void 0:t.filter(Boolean))?n:[]}function h(e){const{values:n,children:t}=e;return(0,r.useMemo)((()=>{const e=null!=n?n:function(e){return d(e).map((e=>{let{props:{value:n,label:t,attributes:r,default:a}}=e;return{value:n,label:t,attributes:r,default:a}}))}(t);return function(e){const n=(0,u.l)(e,((e,n)=>e.value===n.value));if(n.length>0)throw new Error('Docusaurus error: Duplicate values "'+n.map((e=>e.value)).join(", ")+'" found in <Tabs>. Every value needs to be unique.')}(e),e}),[n,t])}function p(e){let{value:n,tabValues:t}=e;return t.some((e=>e.value===n))}function m(e){let{queryString:n=!1,groupId:t}=e;const a=(0,o.k6)(),s=function(e){let{queryString:n=!1,groupId:t}=e;if("string"==typeof n)return n;if(!1===n)return null;if(!0===n&&!t)throw new Error('Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".');return null!=t?t:null}({queryString:n,groupId:t});return[(0,i._X)(s),(0,r.useCallback)((e=>{if(!s)return;const n=new URLSearchParams(a.location.search);n.set(s,e),a.replace({...a.location,search:n.toString()})}),[s,a])]}function f(e){const{defaultValue:n,queryString:t=!1,groupId:a}=e,s=h(e),[o,i]=(0,r.useState)((()=>function(e){var n;let{defaultValue:t,tabValues:r}=e;if(0===r.length)throw new Error("Docusaurus error: the <Tabs> component requires at least one <TabItem> children component");if(t){if(!p({value:t,tabValues:r}))throw new Error('Docusaurus error: The <Tabs> has a defaultValue "'+t+'" but none of its children has the corresponding value. Available values are: '+r.map((e=>e.value)).join(", ")+". If you intend to show no default tab, use defaultValue={null} instead.");return t}const a=null!=(n=r.find((e=>e.default)))?n:r[0];if(!a)throw new Error("Unexpected error: 0 tabValues");return a.value}({defaultValue:n,tabValues:s}))),[u,d]=m({queryString:t,groupId:a}),[f,b]=function(e){let{groupId:n}=e;const t=function(e){return e?"docusaurus.tab."+e:null}(n),[a,s]=(0,c.Nk)(t);return[a,(0,r.useCallback)((e=>{t&&s.set(e)}),[t,s])]}({groupId:a}),v=(()=>{const e=null!=u?u:f;return p({value:e,tabValues:s})?e:null})();(0,l.Z)((()=>{v&&i(v)}),[v]);return{selectedValue:o,selectValue:(0,r.useCallback)((e=>{if(!p({value:e,tabValues:s}))throw new Error("Can't select invalid tab value="+e);i(e),d(e),b(e)}),[d,b,s]),tabValues:s}}var b=t(2389),v={tabList:"tabList__CuJ",tabItem:"tabItem_LNqP"},x=t(5893);function w(e){let{className:n,block:t,selectedValue:r,selectValue:o,tabValues:l}=e;const i=[],{blockElementScrollPositionUntilNextRender:u}=(0,s.o5)(),c=e=>{const n=e.currentTarget,t=i.indexOf(n),a=l[t].value;a!==r&&(u(n),o(a))},d=e=>{var n;let t=null;switch(e.key){case"Enter":c(e);break;case"ArrowRight":{var r;const n=i.indexOf(e.currentTarget)+1;t=null!=(r=i[n])?r:i[0];break}case"ArrowLeft":{var a;const n=i.indexOf(e.currentTarget)-1;t=null!=(a=i[n])?a:i[i.length-1];break}}null==(n=t)||n.focus()};return(0,x.jsx)("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,a.Z)("tabs",{"tabs--block":t},n),children:l.map((e=>{let{value:n,label:t,attributes:s}=e;return(0,x.jsx)("li",{role:"tab",tabIndex:r===n?0:-1,"aria-selected":r===n,ref:e=>i.push(e),onKeyDown:d,onClick:c,...s,className:(0,a.Z)("tabs__item",v.tabItem,null==s?void 0:s.className,{"tabs__item--active":r===n}),children:null!=t?t:n},n)}))})}function g(e){let{lazy:n,children:t,selectedValue:a}=e;const s=(Array.isArray(t)?t:[t]).filter(Boolean);if(n){const e=s.find((e=>e.props.value===a));return e?(0,r.cloneElement)(e,{className:"margin-top--md"}):null}return(0,x.jsx)("div",{className:"margin-top--md",children:s.map(((e,n)=>(0,r.cloneElement)(e,{key:n,hidden:e.props.value!==a})))})}function j(e){const n=f(e);return(0,x.jsxs)("div",{className:(0,a.Z)("tabs-container",v.tabList),children:[(0,x.jsx)(w,{...e,...n}),(0,x.jsx)(g,{...e,...n})]})}function y(e){const n=(0,b.Z)();return(0,x.jsx)(j,{...e,children:d(e.children)},String(n))}},1151:function(e,n,t){t.d(n,{Z:function(){return l},a:function(){return o}});var r=t(7294);const a={},s=r.createContext(a);function o(e){const n=r.useContext(s);return r.useMemo((function(){return"function"==typeof e?e(n):{...n,...e}}),[n,e])}function l(e){let n;return n=e.disableParentContext?"function"==typeof e.components?e.components(a):e.components||a:o(e.components),r.createElement(s.Provider,{value:n},e.children)}}}]);