"use strict";(self.webpackChunkbento=self.webpackChunkbento||[]).push([[3030],{9038:function(e,n,t){t.r(n),t.d(n,{assets:function(){return u},contentTitle:function(){return l},default:function(){return p},frontMatter:function(){return a},metadata:function(){return c},toc:function(){return d}});var s=t(5893),r=t(1151),o=t(4866),i=t(5162);const a={title:"broker",slug:"broker",type:"input",status:"stable",categories:["Utility"],name:"broker"},l=void 0,c={id:"components/inputs/broker",title:"broker",description:"\x3c!--",source:"@site/docs/components/inputs/broker.md",sourceDirName:"components/inputs",slug:"/components/inputs/broker",permalink:"/bento/docs/components/inputs/broker",draft:!1,unlisted:!1,editUrl:"https://github.com/warpstreamlabs/bento/edit/main/website/docs/components/inputs/broker.md",tags:[],version:"current",frontMatter:{title:"broker",slug:"broker",type:"input",status:"stable",categories:["Utility"],name:"broker"},sidebar:"docs",previous:{title:"beanstalkd",permalink:"/bento/docs/components/inputs/beanstalkd"},next:{title:"cassandra",permalink:"/bento/docs/components/inputs/cassandra"}},u={},d=[{value:"Batching",id:"batching",level:3},{value:"Processors",id:"processors",level:3},{value:"Fields",id:"fields",level:2},{value:"<code>copies</code>",id:"copies",level:3},{value:"<code>inputs</code>",id:"inputs",level:3},{value:"<code>batching</code>",id:"batching-1",level:3},{value:"<code>batching.count</code>",id:"batchingcount",level:3},{value:"<code>batching.byte_size</code>",id:"batchingbyte_size",level:3},{value:"<code>batching.period</code>",id:"batchingperiod",level:3},{value:"<code>batching.check</code>",id:"batchingcheck",level:3},{value:"<code>batching.processors</code>",id:"batchingprocessors",level:3}];function h(e){const n={a:"a",br:"br",code:"code",em:"em",h2:"h2",h3:"h3",p:"p",pre:"pre",...(0,r.a)(),...e.components};return(0,s.jsxs)(s.Fragment,{children:[(0,s.jsx)(n.p,{children:"Allows you to combine multiple inputs into a single stream of data, where each input will be read in parallel."}),"\n",(0,s.jsxs)(o.Z,{defaultValue:"common",values:[{label:"Common",value:"common"},{label:"Advanced",value:"advanced"}],children:[(0,s.jsx)(i.Z,{value:"common",children:(0,s.jsx)(n.pre,{children:(0,s.jsx)(n.code,{className:"language-yml",children:'# Common config fields, showing default values\ninput:\n  label: ""\n  broker:\n    inputs: [] # No default (required)\n    batching:\n      count: 0\n      byte_size: 0\n      period: ""\n      check: ""\n'})})}),(0,s.jsx)(i.Z,{value:"advanced",children:(0,s.jsx)(n.pre,{children:(0,s.jsx)(n.code,{className:"language-yml",children:'# All config fields, showing default values\ninput:\n  label: ""\n  broker:\n    copies: 1\n    inputs: [] # No default (required)\n    batching:\n      count: 0\n      byte_size: 0\n      period: ""\n      check: ""\n      processors: [] # No default (optional)\n'})})})]}),"\n",(0,s.jsx)(n.p,{children:"A broker type is configured with its own list of input configurations and a field to specify how many copies of the list of inputs should be created."}),"\n",(0,s.jsx)(n.p,{children:"Adding more input types allows you to combine streams from multiple sources into one. For example, reading from both RabbitMQ and Kafka:"}),"\n",(0,s.jsx)(n.pre,{children:(0,s.jsx)(n.code,{className:"language-yaml",children:"input:\n  broker:\n    copies: 1\n    inputs:\n      - amqp_0_9:\n          urls:\n            - amqp://guest:guest@localhost:5672/\n          consumer_tag: bento-consumer\n          queue: bento-queue\n\n        # Optional list of input specific processing steps\n        processors:\n          - mapping: |\n              root.message = this\n              root.meta.link_count = this.links.length()\n              root.user.age = this.user.age.number()\n\n      - kafka:\n          addresses:\n            - localhost:9092\n          client_id: bento_kafka_input\n          consumer_group: bento_consumer_group\n          topics: [ bento_stream:0 ]\n"})}),"\n",(0,s.jsx)(n.p,{children:"If the number of copies is greater than zero the list will be copied that number of times. For example, if your inputs were of type foo and bar, with 'copies' set to '2', you would end up with two 'foo' inputs and two 'bar' inputs."}),"\n",(0,s.jsx)(n.h3,{id:"batching",children:"Batching"}),"\n",(0,s.jsxs)(n.p,{children:["It's possible to configure a ",(0,s.jsx)(n.a,{href:"/docs/configuration/batching#batch-policy",children:"batch policy"})," with a broker using the ",(0,s.jsx)(n.code,{children:"batching"})," fields. When doing this the feeds from all child inputs are combined. Some inputs do not support broker based batching and specify this in their documentation."]}),"\n",(0,s.jsx)(n.h3,{id:"processors",children:"Processors"}),"\n",(0,s.jsxs)(n.p,{children:["It is possible to configure ",(0,s.jsx)(n.a,{href:"/docs/components/processors/about",children:"processors"})," at the broker level, where they will be applied to ",(0,s.jsx)(n.em,{children:"all"})," child inputs, as well as on the individual child inputs. If you have processors at both the broker level ",(0,s.jsx)(n.em,{children:"and"})," on child inputs then the broker processors will be applied ",(0,s.jsx)(n.em,{children:"after"})," the child nodes processors."]}),"\n",(0,s.jsx)(n.h2,{id:"fields",children:"Fields"}),"\n",(0,s.jsx)(n.h3,{id:"copies",children:(0,s.jsx)(n.code,{children:"copies"})}),"\n",(0,s.jsxs)(n.p,{children:["Whatever is specified within ",(0,s.jsx)(n.code,{children:"inputs"})," will be created this many times."]}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"int"}),(0,s.jsx)(n.br,{}),"\n","Default: ",(0,s.jsx)(n.code,{children:"1"})]}),"\n",(0,s.jsx)(n.h3,{id:"inputs",children:(0,s.jsx)(n.code,{children:"inputs"})}),"\n",(0,s.jsx)(n.p,{children:"A list of inputs to create."}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"array"})]}),"\n",(0,s.jsx)(n.h3,{id:"batching-1",children:(0,s.jsx)(n.code,{children:"batching"})}),"\n",(0,s.jsxs)(n.p,{children:["Allows you to configure a ",(0,s.jsx)(n.a,{href:"/docs/configuration/batching",children:"batching policy"}),"."]}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"object"})]}),"\n",(0,s.jsx)(n.pre,{children:(0,s.jsx)(n.code,{className:"language-yml",children:'# Examples\n\nbatching:\n  byte_size: 5000\n  count: 0\n  period: 1s\n\nbatching:\n  count: 10\n  period: 1s\n\nbatching:\n  check: this.contains("END BATCH")\n  count: 0\n  period: 1m\n'})}),"\n",(0,s.jsx)(n.h3,{id:"batchingcount",children:(0,s.jsx)(n.code,{children:"batching.count"})}),"\n",(0,s.jsxs)(n.p,{children:["A number of messages at which the batch should be flushed. If ",(0,s.jsx)(n.code,{children:"0"})," disables count based batching."]}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"int"}),(0,s.jsx)(n.br,{}),"\n","Default: ",(0,s.jsx)(n.code,{children:"0"})]}),"\n",(0,s.jsx)(n.h3,{id:"batchingbyte_size",children:(0,s.jsx)(n.code,{children:"batching.byte_size"})}),"\n",(0,s.jsxs)(n.p,{children:["An amount of bytes at which the batch should be flushed. If ",(0,s.jsx)(n.code,{children:"0"})," disables size based batching."]}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"int"}),(0,s.jsx)(n.br,{}),"\n","Default: ",(0,s.jsx)(n.code,{children:"0"})]}),"\n",(0,s.jsx)(n.h3,{id:"batchingperiod",children:(0,s.jsx)(n.code,{children:"batching.period"})}),"\n",(0,s.jsx)(n.p,{children:"A period in which an incomplete batch should be flushed regardless of its size."}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"string"}),(0,s.jsx)(n.br,{}),"\n","Default: ",(0,s.jsx)(n.code,{children:'""'})]}),"\n",(0,s.jsx)(n.pre,{children:(0,s.jsx)(n.code,{className:"language-yml",children:"# Examples\n\nperiod: 1s\n\nperiod: 1m\n\nperiod: 500ms\n"})}),"\n",(0,s.jsx)(n.h3,{id:"batchingcheck",children:(0,s.jsx)(n.code,{children:"batching.check"})}),"\n",(0,s.jsxs)(n.p,{children:["A ",(0,s.jsx)(n.a,{href:"/docs/guides/bloblang/about/",children:"Bloblang query"})," that should return a boolean value indicating whether a message should end a batch."]}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"string"}),(0,s.jsx)(n.br,{}),"\n","Default: ",(0,s.jsx)(n.code,{children:'""'})]}),"\n",(0,s.jsx)(n.pre,{children:(0,s.jsx)(n.code,{className:"language-yml",children:'# Examples\n\ncheck: this.type == "end_of_transaction"\n'})}),"\n",(0,s.jsx)(n.h3,{id:"batchingprocessors",children:(0,s.jsx)(n.code,{children:"batching.processors"})}),"\n",(0,s.jsxs)(n.p,{children:["A list of ",(0,s.jsx)(n.a,{href:"/docs/components/processors/about",children:"processors"})," to apply to a batch as it is flushed. This allows you to aggregate and archive the batch however you see fit. Please note that all resulting messages are flushed as a single batch, therefore splitting the batch into smaller batches using these processors is a no-op."]}),"\n",(0,s.jsxs)(n.p,{children:["Type: ",(0,s.jsx)(n.code,{children:"array"})]}),"\n",(0,s.jsx)(n.pre,{children:(0,s.jsx)(n.code,{className:"language-yml",children:"# Examples\n\nprocessors:\n  - archive:\n      format: concatenate\n\nprocessors:\n  - archive:\n      format: lines\n\nprocessors:\n  - archive:\n      format: json_array\n"})})]})}function p(e={}){const{wrapper:n}={...(0,r.a)(),...e.components};return n?(0,s.jsx)(n,{...e,children:(0,s.jsx)(h,{...e})}):h(e)}},5162:function(e,n,t){t.d(n,{Z:function(){return i}});t(7294);var s=t(6010),r={tabItem:"tabItem_Ymn6"},o=t(5893);function i(e){let{children:n,hidden:t,className:i}=e;return(0,o.jsx)("div",{role:"tabpanel",className:(0,s.Z)(r.tabItem,i),hidden:t,children:n})}},4866:function(e,n,t){t.d(n,{Z:function(){return k}});var s=t(7294),r=t(6010),o=t(2466),i=t(6550),a=t(469),l=t(1980),c=t(7392),u=t(12);function d(e){var n,t;return null!=(n=null==(t=s.Children.toArray(e).filter((e=>"\n"!==e)).map((e=>{if(!e||(0,s.isValidElement)(e)&&function(e){const{props:n}=e;return!!n&&"object"==typeof n&&"value"in n}(e))return e;throw new Error("Docusaurus error: Bad <Tabs> child <"+("string"==typeof e.type?e.type:e.type.name)+'>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.')})))?void 0:t.filter(Boolean))?n:[]}function h(e){const{values:n,children:t}=e;return(0,s.useMemo)((()=>{const e=null!=n?n:function(e){return d(e).map((e=>{let{props:{value:n,label:t,attributes:s,default:r}}=e;return{value:n,label:t,attributes:s,default:r}}))}(t);return function(e){const n=(0,c.l)(e,((e,n)=>e.value===n.value));if(n.length>0)throw new Error('Docusaurus error: Duplicate values "'+n.map((e=>e.value)).join(", ")+'" found in <Tabs>. Every value needs to be unique.')}(e),e}),[n,t])}function p(e){let{value:n,tabValues:t}=e;return t.some((e=>e.value===n))}function b(e){let{queryString:n=!1,groupId:t}=e;const r=(0,i.k6)(),o=function(e){let{queryString:n=!1,groupId:t}=e;if("string"==typeof n)return n;if(!1===n)return null;if(!0===n&&!t)throw new Error('Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".');return null!=t?t:null}({queryString:n,groupId:t});return[(0,l._X)(o),(0,s.useCallback)((e=>{if(!o)return;const n=new URLSearchParams(r.location.search);n.set(o,e),r.replace({...r.location,search:n.toString()})}),[o,r])]}function f(e){const{defaultValue:n,queryString:t=!1,groupId:r}=e,o=h(e),[i,l]=(0,s.useState)((()=>function(e){var n;let{defaultValue:t,tabValues:s}=e;if(0===s.length)throw new Error("Docusaurus error: the <Tabs> component requires at least one <TabItem> children component");if(t){if(!p({value:t,tabValues:s}))throw new Error('Docusaurus error: The <Tabs> has a defaultValue "'+t+'" but none of its children has the corresponding value. Available values are: '+s.map((e=>e.value)).join(", ")+". If you intend to show no default tab, use defaultValue={null} instead.");return t}const r=null!=(n=s.find((e=>e.default)))?n:s[0];if(!r)throw new Error("Unexpected error: 0 tabValues");return r.value}({defaultValue:n,tabValues:o}))),[c,d]=b({queryString:t,groupId:r}),[f,m]=function(e){let{groupId:n}=e;const t=function(e){return e?"docusaurus.tab."+e:null}(n),[r,o]=(0,u.Nk)(t);return[r,(0,s.useCallback)((e=>{t&&o.set(e)}),[t,o])]}({groupId:r}),g=(()=>{const e=null!=c?c:f;return p({value:e,tabValues:o})?e:null})();(0,a.Z)((()=>{g&&l(g)}),[g]);return{selectedValue:i,selectValue:(0,s.useCallback)((e=>{if(!p({value:e,tabValues:o}))throw new Error("Can't select invalid tab value="+e);l(e),d(e),m(e)}),[d,m,o]),tabValues:o}}var m=t(2389),g={tabList:"tabList__CuJ",tabItem:"tabItem_LNqP"},x=t(5893);function j(e){let{className:n,block:t,selectedValue:s,selectValue:i,tabValues:a}=e;const l=[],{blockElementScrollPositionUntilNextRender:c}=(0,o.o5)(),u=e=>{const n=e.currentTarget,t=l.indexOf(n),r=a[t].value;r!==s&&(c(n),i(r))},d=e=>{var n;let t=null;switch(e.key){case"Enter":u(e);break;case"ArrowRight":{var s;const n=l.indexOf(e.currentTarget)+1;t=null!=(s=l[n])?s:l[0];break}case"ArrowLeft":{var r;const n=l.indexOf(e.currentTarget)-1;t=null!=(r=l[n])?r:l[l.length-1];break}}null==(n=t)||n.focus()};return(0,x.jsx)("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,r.Z)("tabs",{"tabs--block":t},n),children:a.map((e=>{let{value:n,label:t,attributes:o}=e;return(0,x.jsx)("li",{role:"tab",tabIndex:s===n?0:-1,"aria-selected":s===n,ref:e=>l.push(e),onKeyDown:d,onClick:u,...o,className:(0,r.Z)("tabs__item",g.tabItem,null==o?void 0:o.className,{"tabs__item--active":s===n}),children:null!=t?t:n},n)}))})}function v(e){let{lazy:n,children:t,selectedValue:r}=e;const o=(Array.isArray(t)?t:[t]).filter(Boolean);if(n){const e=o.find((e=>e.props.value===r));return e?(0,s.cloneElement)(e,{className:"margin-top--md"}):null}return(0,x.jsx)("div",{className:"margin-top--md",children:o.map(((e,n)=>(0,s.cloneElement)(e,{key:n,hidden:e.props.value!==r})))})}function y(e){const n=f(e);return(0,x.jsxs)("div",{className:(0,r.Z)("tabs-container",g.tabList),children:[(0,x.jsx)(j,{...e,...n}),(0,x.jsx)(v,{...e,...n})]})}function k(e){const n=(0,m.Z)();return(0,x.jsx)(y,{...e,children:d(e.children)},String(n))}},1151:function(e,n,t){t.d(n,{Z:function(){return a},a:function(){return i}});var s=t(7294);const r={},o=s.createContext(r);function i(e){const n=s.useContext(o);return s.useMemo((function(){return"function"==typeof e?e(n):{...n,...e}}),[n,e])}function a(e){let n;return n=e.disableParentContext?"function"==typeof e.components?e.components(r):e.components||r:i(e.components),s.createElement(o.Provider,{value:n},e.children)}}}]);