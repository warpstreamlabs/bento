"use strict";(self.webpackChunkbento=self.webpackChunkbento||[]).push([[7601],{856:function(e,n,t){t.r(n),t.d(n,{assets:function(){return i},contentTitle:function(){return l},default:function(){return d},frontMatter:function(){return s},metadata:function(){return a},toc:function(){return u}});var r=t(5893),o=t(1151);t(4866),t(5162);const s={title:"nanomsg",slug:"nanomsg",type:"output",status:"stable",categories:["Network"],name:"nanomsg"},l=void 0,a={id:"components/outputs/nanomsg",title:"nanomsg",description:"\x3c!--",source:"@site/docs/components/outputs/nanomsg.md",sourceDirName:"components/outputs",slug:"/components/outputs/nanomsg",permalink:"/bento/docs/components/outputs/nanomsg",draft:!1,unlisted:!1,editUrl:"https://github.com/warpstreamlabs/bento/edit/main/website/docs/components/outputs/nanomsg.md",tags:[],version:"current",frontMatter:{title:"nanomsg",slug:"nanomsg",type:"output",status:"stable",categories:["Network"],name:"nanomsg"},sidebar:"docs",previous:{title:"mqtt",permalink:"/bento/docs/components/outputs/mqtt"},next:{title:"nats",permalink:"/bento/docs/components/outputs/nats"}},i={},u=[{value:"Performance",id:"performance",level:2},{value:"Fields",id:"fields",level:2},{value:"<code>urls</code>",id:"urls",level:3},{value:"<code>bind</code>",id:"bind",level:3},{value:"<code>socket_type</code>",id:"socket_type",level:3},{value:"<code>poll_timeout</code>",id:"poll_timeout",level:3},{value:"<code>max_in_flight</code>",id:"max_in_flight",level:3}];function c(e){const n={br:"br",code:"code",h2:"h2",h3:"h3",p:"p",pre:"pre",...(0,o.a)(),...e.components};return(0,r.jsxs)(r.Fragment,{children:[(0,r.jsx)(n.p,{children:"Send messages over a Nanomsg socket."}),"\n",(0,r.jsx)(n.pre,{children:(0,r.jsx)(n.code,{className:"language-yml",children:'# Config fields, showing default values\noutput:\n  label: ""\n  nanomsg:\n    urls: [] # No default (required)\n    bind: false\n    socket_type: PUSH\n    poll_timeout: 5s\n    max_in_flight: 64\n'})}),"\n",(0,r.jsx)(n.p,{children:"Currently only PUSH and PUB sockets are supported."}),"\n",(0,r.jsx)(n.h2,{id:"performance",children:"Performance"}),"\n",(0,r.jsxs)(n.p,{children:["This output benefits from sending multiple messages in flight in parallel for improved performance. You can tune the max number of in flight messages (or message batches) with the field ",(0,r.jsx)(n.code,{children:"max_in_flight"}),"."]}),"\n",(0,r.jsx)(n.h2,{id:"fields",children:"Fields"}),"\n",(0,r.jsx)(n.h3,{id:"urls",children:(0,r.jsx)(n.code,{children:"urls"})}),"\n",(0,r.jsx)(n.p,{children:"A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs."}),"\n",(0,r.jsxs)(n.p,{children:["Type: ",(0,r.jsx)(n.code,{children:"array"})]}),"\n",(0,r.jsx)(n.h3,{id:"bind",children:(0,r.jsx)(n.code,{children:"bind"})}),"\n",(0,r.jsx)(n.p,{children:"Whether the URLs listed should be bind (otherwise they are connected to)."}),"\n",(0,r.jsxs)(n.p,{children:["Type: ",(0,r.jsx)(n.code,{children:"bool"}),(0,r.jsx)(n.br,{}),"\n","Default: ",(0,r.jsx)(n.code,{children:"false"})]}),"\n",(0,r.jsx)(n.h3,{id:"socket_type",children:(0,r.jsx)(n.code,{children:"socket_type"})}),"\n",(0,r.jsx)(n.p,{children:"The socket type to send with."}),"\n",(0,r.jsxs)(n.p,{children:["Type: ",(0,r.jsx)(n.code,{children:"string"}),(0,r.jsx)(n.br,{}),"\n","Default: ",(0,r.jsx)(n.code,{children:'"PUSH"'}),(0,r.jsx)(n.br,{}),"\n","Options: ",(0,r.jsx)(n.code,{children:"PUSH"}),", ",(0,r.jsx)(n.code,{children:"PUB"}),"."]}),"\n",(0,r.jsx)(n.h3,{id:"poll_timeout",children:(0,r.jsx)(n.code,{children:"poll_timeout"})}),"\n",(0,r.jsx)(n.p,{children:"The maximum period of time to wait for a message to send before the request is abandoned and reattempted."}),"\n",(0,r.jsxs)(n.p,{children:["Type: ",(0,r.jsx)(n.code,{children:"string"}),(0,r.jsx)(n.br,{}),"\n","Default: ",(0,r.jsx)(n.code,{children:'"5s"'})]}),"\n",(0,r.jsx)(n.h3,{id:"max_in_flight",children:(0,r.jsx)(n.code,{children:"max_in_flight"})}),"\n",(0,r.jsx)(n.p,{children:"The maximum number of messages to have in flight at a given time. Increase this to improve throughput."}),"\n",(0,r.jsxs)(n.p,{children:["Type: ",(0,r.jsx)(n.code,{children:"int"}),(0,r.jsx)(n.br,{}),"\n","Default: ",(0,r.jsx)(n.code,{children:"64"})]})]})}function d(e={}){const{wrapper:n}={...(0,o.a)(),...e.components};return n?(0,r.jsx)(n,{...e,children:(0,r.jsx)(c,{...e})}):c(e)}},5162:function(e,n,t){t.d(n,{Z:function(){return l}});t(7294);var r=t(6010),o={tabItem:"tabItem_Ymn6"},s=t(5893);function l(e){let{children:n,hidden:t,className:l}=e;return(0,s.jsx)("div",{role:"tabpanel",className:(0,r.Z)(o.tabItem,l),hidden:t,children:n})}},4866:function(e,n,t){t.d(n,{Z:function(){return w}});var r=t(7294),o=t(6010),s=t(2466),l=t(6550),a=t(469),i=t(1980),u=t(7392),c=t(12);function d(e){var n,t;return null!=(n=null==(t=r.Children.toArray(e).filter((e=>"\n"!==e)).map((e=>{if(!e||(0,r.isValidElement)(e)&&function(e){const{props:n}=e;return!!n&&"object"==typeof n&&"value"in n}(e))return e;throw new Error("Docusaurus error: Bad <Tabs> child <"+("string"==typeof e.type?e.type:e.type.name)+'>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.')})))?void 0:t.filter(Boolean))?n:[]}function h(e){const{values:n,children:t}=e;return(0,r.useMemo)((()=>{const e=null!=n?n:function(e){return d(e).map((e=>{let{props:{value:n,label:t,attributes:r,default:o}}=e;return{value:n,label:t,attributes:r,default:o}}))}(t);return function(e){const n=(0,u.l)(e,((e,n)=>e.value===n.value));if(n.length>0)throw new Error('Docusaurus error: Duplicate values "'+n.map((e=>e.value)).join(", ")+'" found in <Tabs>. Every value needs to be unique.')}(e),e}),[n,t])}function p(e){let{value:n,tabValues:t}=e;return t.some((e=>e.value===n))}function m(e){let{queryString:n=!1,groupId:t}=e;const o=(0,l.k6)(),s=function(e){let{queryString:n=!1,groupId:t}=e;if("string"==typeof n)return n;if(!1===n)return null;if(!0===n&&!t)throw new Error('Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".');return null!=t?t:null}({queryString:n,groupId:t});return[(0,i._X)(s),(0,r.useCallback)((e=>{if(!s)return;const n=new URLSearchParams(o.location.search);n.set(s,e),o.replace({...o.location,search:n.toString()})}),[s,o])]}function f(e){const{defaultValue:n,queryString:t=!1,groupId:o}=e,s=h(e),[l,i]=(0,r.useState)((()=>function(e){var n;let{defaultValue:t,tabValues:r}=e;if(0===r.length)throw new Error("Docusaurus error: the <Tabs> component requires at least one <TabItem> children component");if(t){if(!p({value:t,tabValues:r}))throw new Error('Docusaurus error: The <Tabs> has a defaultValue "'+t+'" but none of its children has the corresponding value. Available values are: '+r.map((e=>e.value)).join(", ")+". If you intend to show no default tab, use defaultValue={null} instead.");return t}const o=null!=(n=r.find((e=>e.default)))?n:r[0];if(!o)throw new Error("Unexpected error: 0 tabValues");return o.value}({defaultValue:n,tabValues:s}))),[u,d]=m({queryString:t,groupId:o}),[f,b]=function(e){let{groupId:n}=e;const t=function(e){return e?"docusaurus.tab."+e:null}(n),[o,s]=(0,c.Nk)(t);return[o,(0,r.useCallback)((e=>{t&&s.set(e)}),[t,s])]}({groupId:o}),x=(()=>{const e=null!=u?u:f;return p({value:e,tabValues:s})?e:null})();(0,a.Z)((()=>{x&&i(x)}),[x]);return{selectedValue:l,selectValue:(0,r.useCallback)((e=>{if(!p({value:e,tabValues:s}))throw new Error("Can't select invalid tab value="+e);i(e),d(e),b(e)}),[d,b,s]),tabValues:s}}var b=t(2389),x={tabList:"tabList__CuJ",tabItem:"tabItem_LNqP"},g=t(5893);function v(e){let{className:n,block:t,selectedValue:r,selectValue:l,tabValues:a}=e;const i=[],{blockElementScrollPositionUntilNextRender:u}=(0,s.o5)(),c=e=>{const n=e.currentTarget,t=i.indexOf(n),o=a[t].value;o!==r&&(u(n),l(o))},d=e=>{var n;let t=null;switch(e.key){case"Enter":c(e);break;case"ArrowRight":{var r;const n=i.indexOf(e.currentTarget)+1;t=null!=(r=i[n])?r:i[0];break}case"ArrowLeft":{var o;const n=i.indexOf(e.currentTarget)-1;t=null!=(o=i[n])?o:i[i.length-1];break}}null==(n=t)||n.focus()};return(0,g.jsx)("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,o.Z)("tabs",{"tabs--block":t},n),children:a.map((e=>{let{value:n,label:t,attributes:s}=e;return(0,g.jsx)("li",{role:"tab",tabIndex:r===n?0:-1,"aria-selected":r===n,ref:e=>i.push(e),onKeyDown:d,onClick:c,...s,className:(0,o.Z)("tabs__item",x.tabItem,null==s?void 0:s.className,{"tabs__item--active":r===n}),children:null!=t?t:n},n)}))})}function j(e){let{lazy:n,children:t,selectedValue:o}=e;const s=(Array.isArray(t)?t:[t]).filter(Boolean);if(n){const e=s.find((e=>e.props.value===o));return e?(0,r.cloneElement)(e,{className:"margin-top--md"}):null}return(0,g.jsx)("div",{className:"margin-top--md",children:s.map(((e,n)=>(0,r.cloneElement)(e,{key:n,hidden:e.props.value!==o})))})}function y(e){const n=f(e);return(0,g.jsxs)("div",{className:(0,o.Z)("tabs-container",x.tabList),children:[(0,g.jsx)(v,{...e,...n}),(0,g.jsx)(j,{...e,...n})]})}function w(e){const n=(0,b.Z)();return(0,g.jsx)(y,{...e,children:d(e.children)},String(n))}},1151:function(e,n,t){t.d(n,{Z:function(){return a},a:function(){return l}});var r=t(7294);const o={},s=r.createContext(o);function l(e){const n=r.useContext(s);return r.useMemo((function(){return"function"==typeof e?e(n):{...n,...e}}),[n,e])}function a(e){let n;return n=e.disableParentContext?"function"==typeof e.components?e.components(o):e.components||o:l(e.components),r.createElement(s.Provider,{value:n},e.children)}}}]);