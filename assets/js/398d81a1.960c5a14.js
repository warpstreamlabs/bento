"use strict";(self.webpackChunkbento=self.webpackChunkbento||[]).push([[3360],{257:function(e,n,t){t.r(n),t.d(n,{assets:function(){return d},contentTitle:function(){return a},default:function(){return p},frontMatter:function(){return c},metadata:function(){return l},toc:function(){return u}});var o=t(5893),r=t(1151),s=t(4866),i=t(5162);const c={title:"mongodb",slug:"mongodb",type:"output",status:"experimental",categories:["Services"],name:"mongodb"},a=void 0,l={id:"components/outputs/mongodb",title:"mongodb",description:"\x3c!--",source:"@site/docs/components/outputs/mongodb.md",sourceDirName:"components/outputs",slug:"/components/outputs/mongodb",permalink:"/bento/docs/components/outputs/mongodb",draft:!1,unlisted:!1,editUrl:"https://github.com/warpstreamlabs/bento/edit/main/website/docs/components/outputs/mongodb.md",tags:[],version:"current",frontMatter:{title:"mongodb",slug:"mongodb",type:"output",status:"experimental",categories:["Services"],name:"mongodb"},sidebar:"docs",previous:{title:"kafka_franz",permalink:"/bento/docs/components/outputs/kafka_franz"},next:{title:"mqtt",permalink:"/bento/docs/components/outputs/mqtt"}},d={},u=[{value:"Performance",id:"performance",level:2},{value:"Fields",id:"fields",level:2},{value:"<code>url</code>",id:"url",level:3},{value:"<code>database</code>",id:"database",level:3},{value:"<code>username</code>",id:"username",level:3},{value:"<code>password</code>",id:"password",level:3},{value:"<code>collection</code>",id:"collection",level:3},{value:"<code>operation</code>",id:"operation",level:3},{value:"<code>write_concern</code>",id:"write_concern",level:3},{value:"<code>write_concern.w</code>",id:"write_concernw",level:3},{value:"<code>write_concern.j</code>",id:"write_concernj",level:3},{value:"<code>write_concern.w_timeout</code>",id:"write_concernw_timeout",level:3},{value:"<code>document_map</code>",id:"document_map",level:3},{value:"<code>filter_map</code>",id:"filter_map",level:3},{value:"<code>hint_map</code>",id:"hint_map",level:3},{value:"<code>upsert</code>",id:"upsert",level:3},{value:"<code>max_in_flight</code>",id:"max_in_flight",level:3},{value:"<code>batching</code>",id:"batching",level:3},{value:"<code>batching.count</code>",id:"batchingcount",level:3},{value:"<code>batching.byte_size</code>",id:"batchingbyte_size",level:3},{value:"<code>batching.period</code>",id:"batchingperiod",level:3},{value:"<code>batching.check</code>",id:"batchingcheck",level:3},{value:"<code>batching.processors</code>",id:"batchingprocessors",level:3}];function h(e){const n={a:"a",admonition:"admonition",br:"br",code:"code",h2:"h2",h3:"h3",p:"p",pre:"pre",...(0,r.a)(),...e.components};return(0,o.jsxs)(o.Fragment,{children:[(0,o.jsx)(n.admonition,{title:"EXPERIMENTAL",type:"caution",children:(0,o.jsx)(n.p,{children:"This component is experimental and therefore subject to change or removal outside of major version releases."})}),"\n",(0,o.jsx)(n.p,{children:"Inserts items into a MongoDB collection."}),"\n",(0,o.jsx)(n.p,{children:"Introduced in version 3.43.0."}),"\n",(0,o.jsxs)(s.Z,{defaultValue:"common",values:[{label:"Common",value:"common"},{label:"Advanced",value:"advanced"}],children:[(0,o.jsx)(i.Z,{value:"common",children:(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-yml",children:'# Common config fields, showing default values\noutput:\n  label: ""\n  mongodb:\n    url: mongodb://localhost:27017 # No default (required)\n    database: "" # No default (required)\n    username: ""\n    password: ""\n    collection: "" # No default (required)\n    operation: update-one\n    write_concern:\n      w: ""\n      j: false\n      w_timeout: ""\n    document_map: ""\n    filter_map: ""\n    hint_map: ""\n    upsert: false\n    max_in_flight: 64\n    batching:\n      count: 0\n      byte_size: 0\n      period: ""\n      check: ""\n'})})}),(0,o.jsx)(i.Z,{value:"advanced",children:(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-yml",children:'# All config fields, showing default values\noutput:\n  label: ""\n  mongodb:\n    url: mongodb://localhost:27017 # No default (required)\n    database: "" # No default (required)\n    username: ""\n    password: ""\n    collection: "" # No default (required)\n    operation: update-one\n    write_concern:\n      w: ""\n      j: false\n      w_timeout: ""\n    document_map: ""\n    filter_map: ""\n    hint_map: ""\n    upsert: false\n    max_in_flight: 64\n    batching:\n      count: 0\n      byte_size: 0\n      period: ""\n      check: ""\n      processors: [] # No default (optional)\n'})})})]}),"\n",(0,o.jsx)(n.h2,{id:"performance",children:"Performance"}),"\n",(0,o.jsxs)(n.p,{children:["This output benefits from sending multiple messages in flight in parallel for improved performance. You can tune the max number of in flight messages (or message batches) with the field ",(0,o.jsx)(n.code,{children:"max_in_flight"}),"."]}),"\n",(0,o.jsxs)(n.p,{children:["This output benefits from sending messages as a batch for improved performance. Batches can be formed at both the input and output level. You can find out more ",(0,o.jsx)(n.a,{href:"/docs/configuration/batching",children:"in this doc"}),"."]}),"\n",(0,o.jsx)(n.h2,{id:"fields",children:"Fields"}),"\n",(0,o.jsx)(n.h3,{id:"url",children:(0,o.jsx)(n.code,{children:"url"})}),"\n",(0,o.jsx)(n.p,{children:"The URL of the target MongoDB server."}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"string"})]}),"\n",(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-yml",children:"# Examples\n\nurl: mongodb://localhost:27017\n"})}),"\n",(0,o.jsx)(n.h3,{id:"database",children:(0,o.jsx)(n.code,{children:"database"})}),"\n",(0,o.jsx)(n.p,{children:"The name of the target MongoDB database."}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"string"})]}),"\n",(0,o.jsx)(n.h3,{id:"username",children:(0,o.jsx)(n.code,{children:"username"})}),"\n",(0,o.jsx)(n.p,{children:"The username to connect to the database."}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"string"}),(0,o.jsx)(n.br,{}),"\n","Default: ",(0,o.jsx)(n.code,{children:'""'})]}),"\n",(0,o.jsx)(n.h3,{id:"password",children:(0,o.jsx)(n.code,{children:"password"})}),"\n",(0,o.jsx)(n.p,{children:"The password to connect to the database."}),"\n",(0,o.jsx)(n.admonition,{title:"Secret",type:"warning",children:(0,o.jsxs)(n.p,{children:["This field contains sensitive information that usually shouldn't be added to a config directly, read our ",(0,o.jsx)(n.a,{href:"/docs/configuration/secrets",children:"secrets page for more info"}),"."]})}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"string"}),(0,o.jsx)(n.br,{}),"\n","Default: ",(0,o.jsx)(n.code,{children:'""'})]}),"\n",(0,o.jsx)(n.h3,{id:"collection",children:(0,o.jsx)(n.code,{children:"collection"})}),"\n",(0,o.jsx)(n.p,{children:"The name of the target collection."}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"string"})]}),"\n",(0,o.jsx)(n.h3,{id:"operation",children:(0,o.jsx)(n.code,{children:"operation"})}),"\n",(0,o.jsx)(n.p,{children:"The mongodb operation to perform."}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"string"}),(0,o.jsx)(n.br,{}),"\n","Default: ",(0,o.jsx)(n.code,{children:'"update-one"'}),(0,o.jsx)(n.br,{}),"\n","Options: ",(0,o.jsx)(n.code,{children:"insert-one"}),", ",(0,o.jsx)(n.code,{children:"delete-one"}),", ",(0,o.jsx)(n.code,{children:"delete-many"}),", ",(0,o.jsx)(n.code,{children:"replace-one"}),", ",(0,o.jsx)(n.code,{children:"update-one"}),"."]}),"\n",(0,o.jsx)(n.h3,{id:"write_concern",children:(0,o.jsx)(n.code,{children:"write_concern"})}),"\n",(0,o.jsx)(n.p,{children:"The write concern settings for the mongo connection."}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"object"})]}),"\n",(0,o.jsx)(n.h3,{id:"write_concernw",children:(0,o.jsx)(n.code,{children:"write_concern.w"})}),"\n",(0,o.jsx)(n.p,{children:"W requests acknowledgement that write operations propagate to the specified number of mongodb instances."}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"string"}),(0,o.jsx)(n.br,{}),"\n","Default: ",(0,o.jsx)(n.code,{children:'""'})]}),"\n",(0,o.jsx)(n.h3,{id:"write_concernj",children:(0,o.jsx)(n.code,{children:"write_concern.j"})}),"\n",(0,o.jsx)(n.p,{children:"J requests acknowledgement from MongoDB that write operations are written to the journal."}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"bool"}),(0,o.jsx)(n.br,{}),"\n","Default: ",(0,o.jsx)(n.code,{children:"false"})]}),"\n",(0,o.jsx)(n.h3,{id:"write_concernw_timeout",children:(0,o.jsx)(n.code,{children:"write_concern.w_timeout"})}),"\n",(0,o.jsx)(n.p,{children:"The write concern timeout."}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"string"}),(0,o.jsx)(n.br,{}),"\n","Default: ",(0,o.jsx)(n.code,{children:'""'})]}),"\n",(0,o.jsx)(n.h3,{id:"document_map",children:(0,o.jsx)(n.code,{children:"document_map"})}),"\n",(0,o.jsxs)(n.p,{children:["A bloblang map representing a document to store within MongoDB, expressed as ",(0,o.jsx)(n.a,{href:"https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/",children:"extended JSON in canonical form"}),". The document map is required for the operations insert-one, replace-one and update-one."]}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"string"}),(0,o.jsx)(n.br,{}),"\n","Default: ",(0,o.jsx)(n.code,{children:'""'})]}),"\n",(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-yml",children:"# Examples\n\ndocument_map: |-\n  root.a = this.foo\n  root.b = this.bar\n"})}),"\n",(0,o.jsx)(n.h3,{id:"filter_map",children:(0,o.jsx)(n.code,{children:"filter_map"})}),"\n",(0,o.jsxs)(n.p,{children:["A bloblang map representing a filter for a MongoDB command, expressed as ",(0,o.jsx)(n.a,{href:"https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/",children:"extended JSON in canonical form"}),". The filter map is required for all operations except insert-one. It is used to find the document(s) for the operation. For example in a delete-one case, the filter map should have the fields required to locate the document to delete."]}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"string"}),(0,o.jsx)(n.br,{}),"\n","Default: ",(0,o.jsx)(n.code,{children:'""'})]}),"\n",(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-yml",children:"# Examples\n\nfilter_map: |-\n  root.a = this.foo\n  root.b = this.bar\n"})}),"\n",(0,o.jsx)(n.h3,{id:"hint_map",children:(0,o.jsx)(n.code,{children:"hint_map"})}),"\n",(0,o.jsxs)(n.p,{children:["A bloblang map representing the hint for the MongoDB command, expressed as ",(0,o.jsx)(n.a,{href:"https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/",children:"extended JSON in canonical form"}),". This map is optional and is used with all operations except insert-one. It is used to improve performance of finding the documents in the mongodb."]}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"string"}),(0,o.jsx)(n.br,{}),"\n","Default: ",(0,o.jsx)(n.code,{children:'""'})]}),"\n",(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-yml",children:"# Examples\n\nhint_map: |-\n  root.a = this.foo\n  root.b = this.bar\n"})}),"\n",(0,o.jsx)(n.h3,{id:"upsert",children:(0,o.jsx)(n.code,{children:"upsert"})}),"\n",(0,o.jsx)(n.p,{children:"The upsert setting is optional and only applies for update-one and replace-one operations. If the filter specified in filter_map matches, the document is updated or replaced accordingly, otherwise it is created."}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"bool"}),(0,o.jsx)(n.br,{}),"\n","Default: ",(0,o.jsx)(n.code,{children:"false"}),(0,o.jsx)(n.br,{}),"\n","Requires version 3.60.0 or newer"]}),"\n",(0,o.jsx)(n.h3,{id:"max_in_flight",children:(0,o.jsx)(n.code,{children:"max_in_flight"})}),"\n",(0,o.jsx)(n.p,{children:"The maximum number of messages to have in flight at a given time. Increase this to improve throughput."}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"int"}),(0,o.jsx)(n.br,{}),"\n","Default: ",(0,o.jsx)(n.code,{children:"64"})]}),"\n",(0,o.jsx)(n.h3,{id:"batching",children:(0,o.jsx)(n.code,{children:"batching"})}),"\n",(0,o.jsxs)(n.p,{children:["Allows you to configure a ",(0,o.jsx)(n.a,{href:"/docs/configuration/batching",children:"batching policy"}),"."]}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"object"})]}),"\n",(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-yml",children:'# Examples\n\nbatching:\n  byte_size: 5000\n  count: 0\n  period: 1s\n\nbatching:\n  count: 10\n  period: 1s\n\nbatching:\n  check: this.contains("END BATCH")\n  count: 0\n  period: 1m\n'})}),"\n",(0,o.jsx)(n.h3,{id:"batchingcount",children:(0,o.jsx)(n.code,{children:"batching.count"})}),"\n",(0,o.jsxs)(n.p,{children:["A number of messages at which the batch should be flushed. If ",(0,o.jsx)(n.code,{children:"0"})," disables count based batching."]}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"int"}),(0,o.jsx)(n.br,{}),"\n","Default: ",(0,o.jsx)(n.code,{children:"0"})]}),"\n",(0,o.jsx)(n.h3,{id:"batchingbyte_size",children:(0,o.jsx)(n.code,{children:"batching.byte_size"})}),"\n",(0,o.jsxs)(n.p,{children:["An amount of bytes at which the batch should be flushed. If ",(0,o.jsx)(n.code,{children:"0"})," disables size based batching."]}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"int"}),(0,o.jsx)(n.br,{}),"\n","Default: ",(0,o.jsx)(n.code,{children:"0"})]}),"\n",(0,o.jsx)(n.h3,{id:"batchingperiod",children:(0,o.jsx)(n.code,{children:"batching.period"})}),"\n",(0,o.jsx)(n.p,{children:"A period in which an incomplete batch should be flushed regardless of its size."}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"string"}),(0,o.jsx)(n.br,{}),"\n","Default: ",(0,o.jsx)(n.code,{children:'""'})]}),"\n",(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-yml",children:"# Examples\n\nperiod: 1s\n\nperiod: 1m\n\nperiod: 500ms\n"})}),"\n",(0,o.jsx)(n.h3,{id:"batchingcheck",children:(0,o.jsx)(n.code,{children:"batching.check"})}),"\n",(0,o.jsxs)(n.p,{children:["A ",(0,o.jsx)(n.a,{href:"/docs/guides/bloblang/about/",children:"Bloblang query"})," that should return a boolean value indicating whether a message should end a batch."]}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"string"}),(0,o.jsx)(n.br,{}),"\n","Default: ",(0,o.jsx)(n.code,{children:'""'})]}),"\n",(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-yml",children:'# Examples\n\ncheck: this.type == "end_of_transaction"\n'})}),"\n",(0,o.jsx)(n.h3,{id:"batchingprocessors",children:(0,o.jsx)(n.code,{children:"batching.processors"})}),"\n",(0,o.jsxs)(n.p,{children:["A list of ",(0,o.jsx)(n.a,{href:"/docs/components/processors/about",children:"processors"})," to apply to a batch as it is flushed. This allows you to aggregate and archive the batch however you see fit. Please note that all resulting messages are flushed as a single batch, therefore splitting the batch into smaller batches using these processors is a no-op."]}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"array"})]}),"\n",(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-yml",children:"# Examples\n\nprocessors:\n  - archive:\n      format: concatenate\n\nprocessors:\n  - archive:\n      format: lines\n\nprocessors:\n  - archive:\n      format: json_array\n"})})]})}function p(e={}){const{wrapper:n}={...(0,r.a)(),...e.components};return n?(0,o.jsx)(n,{...e,children:(0,o.jsx)(h,{...e})}):h(e)}},5162:function(e,n,t){t.d(n,{Z:function(){return i}});t(7294);var o=t(6010),r={tabItem:"tabItem_Ymn6"},s=t(5893);function i(e){let{children:n,hidden:t,className:i}=e;return(0,s.jsx)("div",{role:"tabpanel",className:(0,o.Z)(r.tabItem,i),hidden:t,children:n})}},4866:function(e,n,t){t.d(n,{Z:function(){return y}});var o=t(7294),r=t(6010),s=t(2466),i=t(6550),c=t(469),a=t(1980),l=t(7392),d=t(12);function u(e){var n,t;return null!=(n=null==(t=o.Children.toArray(e).filter((e=>"\n"!==e)).map((e=>{if(!e||(0,o.isValidElement)(e)&&function(e){const{props:n}=e;return!!n&&"object"==typeof n&&"value"in n}(e))return e;throw new Error("Docusaurus error: Bad <Tabs> child <"+("string"==typeof e.type?e.type:e.type.name)+'>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.')})))?void 0:t.filter(Boolean))?n:[]}function h(e){const{values:n,children:t}=e;return(0,o.useMemo)((()=>{const e=null!=n?n:function(e){return u(e).map((e=>{let{props:{value:n,label:t,attributes:o,default:r}}=e;return{value:n,label:t,attributes:o,default:r}}))}(t);return function(e){const n=(0,l.l)(e,((e,n)=>e.value===n.value));if(n.length>0)throw new Error('Docusaurus error: Duplicate values "'+n.map((e=>e.value)).join(", ")+'" found in <Tabs>. Every value needs to be unique.')}(e),e}),[n,t])}function p(e){let{value:n,tabValues:t}=e;return t.some((e=>e.value===n))}function m(e){let{queryString:n=!1,groupId:t}=e;const r=(0,i.k6)(),s=function(e){let{queryString:n=!1,groupId:t}=e;if("string"==typeof n)return n;if(!1===n)return null;if(!0===n&&!t)throw new Error('Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".');return null!=t?t:null}({queryString:n,groupId:t});return[(0,a._X)(s),(0,o.useCallback)((e=>{if(!s)return;const n=new URLSearchParams(r.location.search);n.set(s,e),r.replace({...r.location,search:n.toString()})}),[s,r])]}function x(e){const{defaultValue:n,queryString:t=!1,groupId:r}=e,s=h(e),[i,a]=(0,o.useState)((()=>function(e){var n;let{defaultValue:t,tabValues:o}=e;if(0===o.length)throw new Error("Docusaurus error: the <Tabs> component requires at least one <TabItem> children component");if(t){if(!p({value:t,tabValues:o}))throw new Error('Docusaurus error: The <Tabs> has a defaultValue "'+t+'" but none of its children has the corresponding value. Available values are: '+o.map((e=>e.value)).join(", ")+". If you intend to show no default tab, use defaultValue={null} instead.");return t}const r=null!=(n=o.find((e=>e.default)))?n:o[0];if(!r)throw new Error("Unexpected error: 0 tabValues");return r.value}({defaultValue:n,tabValues:s}))),[l,u]=m({queryString:t,groupId:r}),[x,f]=function(e){let{groupId:n}=e;const t=function(e){return e?"docusaurus.tab."+e:null}(n),[r,s]=(0,d.Nk)(t);return[r,(0,o.useCallback)((e=>{t&&s.set(e)}),[t,s])]}({groupId:r}),b=(()=>{const e=null!=l?l:x;return p({value:e,tabValues:s})?e:null})();(0,c.Z)((()=>{b&&a(b)}),[b]);return{selectedValue:i,selectValue:(0,o.useCallback)((e=>{if(!p({value:e,tabValues:s}))throw new Error("Can't select invalid tab value="+e);a(e),u(e),f(e)}),[u,f,s]),tabValues:s}}var f=t(2389),b={tabList:"tabList__CuJ",tabItem:"tabItem_LNqP"},j=t(5893);function g(e){let{className:n,block:t,selectedValue:o,selectValue:i,tabValues:c}=e;const a=[],{blockElementScrollPositionUntilNextRender:l}=(0,s.o5)(),d=e=>{const n=e.currentTarget,t=a.indexOf(n),r=c[t].value;r!==o&&(l(n),i(r))},u=e=>{var n;let t=null;switch(e.key){case"Enter":d(e);break;case"ArrowRight":{var o;const n=a.indexOf(e.currentTarget)+1;t=null!=(o=a[n])?o:a[0];break}case"ArrowLeft":{var r;const n=a.indexOf(e.currentTarget)-1;t=null!=(r=a[n])?r:a[a.length-1];break}}null==(n=t)||n.focus()};return(0,j.jsx)("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,r.Z)("tabs",{"tabs--block":t},n),children:c.map((e=>{let{value:n,label:t,attributes:s}=e;return(0,j.jsx)("li",{role:"tab",tabIndex:o===n?0:-1,"aria-selected":o===n,ref:e=>a.push(e),onKeyDown:u,onClick:d,...s,className:(0,r.Z)("tabs__item",b.tabItem,null==s?void 0:s.className,{"tabs__item--active":o===n}),children:null!=t?t:n},n)}))})}function v(e){let{lazy:n,children:t,selectedValue:r}=e;const s=(Array.isArray(t)?t:[t]).filter(Boolean);if(n){const e=s.find((e=>e.props.value===r));return e?(0,o.cloneElement)(e,{className:"margin-top--md"}):null}return(0,j.jsx)("div",{className:"margin-top--md",children:s.map(((e,n)=>(0,o.cloneElement)(e,{key:n,hidden:e.props.value!==r})))})}function w(e){const n=x(e);return(0,j.jsxs)("div",{className:(0,r.Z)("tabs-container",b.tabList),children:[(0,j.jsx)(g,{...e,...n}),(0,j.jsx)(v,{...e,...n})]})}function y(e){const n=(0,f.Z)();return(0,j.jsx)(w,{...e,children:u(e.children)},String(n))}},1151:function(e,n,t){t.d(n,{Z:function(){return c},a:function(){return i}});var o=t(7294);const r={},s=o.createContext(r);function i(e){const n=o.useContext(s);return o.useMemo((function(){return"function"==typeof e?e(n):{...n,...e}}),[n,e])}function c(e){let n;return n=e.disableParentContext?"function"==typeof e.components?e.components(r):e.components||r:i(e.components),o.createElement(s.Provider,{value:n},e.children)}}}]);