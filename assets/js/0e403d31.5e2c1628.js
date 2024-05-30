"use strict";(self.webpackChunkbento=self.webpackChunkbento||[]).push([[5806],{2755:function(e,t,a){a.r(t),a.d(t,{assets:function(){return d},contentTitle:function(){return i},default:function(){return p},frontMatter:function(){return o},metadata:function(){return r},toc:function(){return c}});var n=a(5893),s=a(1151);const o={title:"Metadata"},i=void 0,r={id:"configuration/metadata",title:"Metadata",description:"In Bento each message has raw contents and metadata, which is a map of key/value pairs representing an arbitrary amount of complementary data.",source:"@site/docs/configuration/metadata.md",sourceDirName:"configuration",slug:"/configuration/metadata",permalink:"/bento/docs/configuration/metadata",draft:!1,unlisted:!1,editUrl:"https://github.com/warpstreamlabs/bento/edit/main/website/docs/configuration/metadata.md",tags:[],version:"current",frontMatter:{title:"Metadata"},sidebar:"docs",previous:{title:"Windowed Processing",permalink:"/bento/docs/configuration/windowed_processing"},next:{title:"Error Handling",permalink:"/bento/docs/configuration/error_handling"}},d={},c=[{value:"Editing Metadata",id:"editing-metadata",level:2},{value:"Using Metadata",id:"using-metadata",level:2},{value:"Restricting Metadata",id:"restricting-metadata",level:2}];function l(e){const t={a:"a",code:"code",em:"em",h2:"h2",p:"p",pre:"pre",...(0,s.a)(),...e.components};return(0,n.jsxs)(n.Fragment,{children:[(0,n.jsx)(t.p,{children:"In Bento each message has raw contents and metadata, which is a map of key/value pairs representing an arbitrary amount of complementary data."}),"\n",(0,n.jsx)(t.p,{children:"When an input protocol supports attributes or metadata they will automatically be added to your messages, refer to the respective input documentation for a list of metadata keys. When an output supports attributes or metadata any metadata key/value pairs in a message will be sent (subject to service limits)."}),"\n",(0,n.jsx)(t.h2,{id:"editing-metadata",children:"Editing Metadata"}),"\n",(0,n.jsxs)(t.p,{children:["Bento allows you to add and remove metadata using the ",(0,n.jsxs)(t.a,{href:"/docs/components/processors/mapping",children:[(0,n.jsx)(t.code,{children:"mapping"})," processor"]}),". For example, you can do something like this in your pipeline:"]}),"\n",(0,n.jsx)(t.pre,{children:(0,n.jsx)(t.code,{className:"language-yaml",children:"pipeline:\n  processors:\n  - mapping: |\n      # Remove all existing metadata from messages\n      meta = deleted()\n\n      # Add a new metadata field `time` from the contents of a JSON\n      # field `event.timestamp`\n      meta time = event.timestamp\n"})}),"\n",(0,n.jsxs)(t.p,{children:["You can also use ",(0,n.jsx)(t.a,{href:"/docs/guides/bloblang/about",children:"Bloblang"})," to delete individual metadata keys with:"]}),"\n",(0,n.jsx)(t.pre,{children:(0,n.jsx)(t.code,{className:"language-coffee",children:"meta foo = deleted()\n"})}),"\n",(0,n.jsx)(t.p,{children:"Or do more interesting things like remove all metadata keys with a certain prefix:"}),"\n",(0,n.jsx)(t.pre,{children:(0,n.jsx)(t.code,{className:"language-coffee",children:'meta = @.filter(kv -> !kv.key.has_prefix("kafka_"))\n'})}),"\n",(0,n.jsx)(t.h2,{id:"using-metadata",children:"Using Metadata"}),"\n",(0,n.jsxs)(t.p,{children:["Metadata values can be referenced in any field that supports ",(0,n.jsx)(t.a,{href:"/docs/configuration/interpolation",children:"interpolation functions"}),". For example, you can route messages to Kafka topics using interpolation of metadata keys:"]}),"\n",(0,n.jsx)(t.pre,{children:(0,n.jsx)(t.code,{className:"language-yaml",children:'output:\n  kafka:\n    addresses: [ TODO ]\n    topic: ${! meta("target_topic") }\n'})}),"\n",(0,n.jsxs)(t.p,{children:["Bento also allows you to conditionally process messages based on their metadata with the ",(0,n.jsxs)(t.a,{href:"/docs/components/processors/switch",children:[(0,n.jsx)(t.code,{children:"switch"})," processor"]}),":"]}),"\n",(0,n.jsx)(t.pre,{children:(0,n.jsx)(t.code,{className:"language-yaml",children:"pipeline:\n  processors:\n  - switch:\n    - check: '@doc_type == \"nested\"'\n      processors:\n        - sql_insert:\n            driver: mysql\n            dsn: foouser:foopassword@tcp(localhost:3306)/foodb\n            table: footable\n            columns: [ foo, bar, baz ]\n            args_mapping: |\n              root = [\n                this.document.foo,\n                this.document.bar,\n                @kafka_topic,\n              ]\n"})}),"\n",(0,n.jsx)(t.h2,{id:"restricting-metadata",children:"Restricting Metadata"}),"\n",(0,n.jsxs)(t.p,{children:["Outputs that support metadata, headers or some other variant of enriched fields on messages will attempt to send all metadata key/value pairs by default. However, sometimes it's useful to refer to metadata fields at the output level even though we do not wish to send them with our data. In this case it's possible to restrict the metadata keys that are sent with the field ",(0,n.jsx)(t.code,{children:"metadata.exclude_prefixes"})," within the respective output config."]}),"\n",(0,n.jsxs)(t.p,{children:["For example, if we were sending messages to kafka using a metadata key ",(0,n.jsx)(t.code,{children:"target_topic"})," to determine the topic but we wished to prevent that metadata key from being sent as a header we could use the following configuration:"]}),"\n",(0,n.jsx)(t.pre,{children:(0,n.jsx)(t.code,{className:"language-yaml",children:'output:\n  kafka:\n    addresses: [ TODO ]\n    topic: ${! meta("target_topic") }\n    metadata:\n      exclude_prefixes:\n        - target_topic\n'})}),"\n",(0,n.jsxs)(t.p,{children:["And when the list of metadata keys that we do ",(0,n.jsx)(t.em,{children:"not"})," want to send is large it can be helpful to use a ",(0,n.jsx)(t.a,{href:"/docs/guides/bloblang/about",children:"Bloblang mapping"}),' in order to give all of these "private" keys a common prefix:']}),"\n",(0,n.jsx)(t.pre,{children:(0,n.jsx)(t.code,{className:"language-yaml",children:'pipeline:\n  processors:\n    # Has an explicit list of public metadata keys, and everything else is given\n    # an underscore prefix.\n    - mapping: |\n        let allowed_meta = [\n          "foo",\n          "bar",\n          "baz",\n        ]\n        meta = @.map_each_key(key -> if !$allowed_meta.contains(key) {\n          "_" + key\n        })\n\noutput:\n  kafka:\n    addresses: [ TODO ]\n    topic: ${! meta("_target_topic") }\n    metadata:\n      exclude_prefixes: [ "_" ]\n'})})]})}function p(e={}){const{wrapper:t}={...(0,s.a)(),...e.components};return t?(0,n.jsx)(t,{...e,children:(0,n.jsx)(l,{...e})}):l(e)}},1151:function(e,t,a){a.d(t,{Z:function(){return r},a:function(){return i}});var n=a(7294);const s={},o=n.createContext(s);function i(e){const t=n.useContext(o);return n.useMemo((function(){return"function"==typeof e?e(t):{...t,...e}}),[t,e])}function r(e){let t;return t=e.disableParentContext?"function"==typeof e.components?e.components(s):e.components||s:i(e.components),n.createElement(o.Provider,{value:t},e.children)}}}]);