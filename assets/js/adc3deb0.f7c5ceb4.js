"use strict";(self.webpackChunkbento=self.webpackChunkbento||[]).push([[6534],{3804:function(e,n,t){t.r(n),t.d(n,{assets:function(){return d},contentTitle:function(){return c},default:function(){return p},frontMatter:function(){return a},metadata:function(){return l},toc:function(){return h}});var o=t(5893),s=t(1151),i=t(4866),r=t(5162);const a={title:"azure_cosmosdb",slug:"azure_cosmosdb",type:"output",status:"experimental",categories:["Azure"],name:"azure_cosmosdb"},c=void 0,l={id:"components/outputs/azure_cosmosdb",title:"azure_cosmosdb",description:"\x3c!--",source:"@site/docs/components/outputs/azure_cosmosdb.md",sourceDirName:"components/outputs",slug:"/components/outputs/azure_cosmosdb",permalink:"/bento/docs/components/outputs/azure_cosmosdb",draft:!1,unlisted:!1,editUrl:"https://github.com/warpstreamlabs/bento/edit/main/website/docs/components/outputs/azure_cosmosdb.md",tags:[],version:"current",frontMatter:{title:"azure_cosmosdb",slug:"azure_cosmosdb",type:"output",status:"experimental",categories:["Azure"],name:"azure_cosmosdb"},sidebar:"docs",previous:{title:"azure_blob_storage",permalink:"/bento/docs/components/outputs/azure_blob_storage"},next:{title:"azure_queue_storage",permalink:"/bento/docs/components/outputs/azure_queue_storage"}},d={},h=[{value:"Credentials",id:"credentials",level:2},{value:"Batching",id:"batching",level:2},{value:"Performance",id:"performance",level:2},{value:"Examples",id:"examples",level:2},{value:"Fields",id:"fields",level:2},{value:"<code>endpoint</code>",id:"endpoint",level:3},{value:"<code>account_key</code>",id:"account_key",level:3},{value:"<code>connection_string</code>",id:"connection_string",level:3},{value:"<code>database</code>",id:"database",level:3},{value:"<code>container</code>",id:"container",level:3},{value:"<code>partition_keys_map</code>",id:"partition_keys_map",level:3},{value:"<code>operation</code>",id:"operation",level:3},{value:"<code>patch_operations</code>",id:"patch_operations",level:3},{value:"<code>patch_operations[].operation</code>",id:"patch_operationsoperation",level:3},{value:"<code>patch_operations[].path</code>",id:"patch_operationspath",level:3},{value:"<code>patch_operations[].value_map</code>",id:"patch_operationsvalue_map",level:3},{value:"<code>patch_condition</code>",id:"patch_condition",level:3},{value:"<code>auto_id</code>",id:"auto_id",level:3},{value:"<code>item_id</code>",id:"item_id",level:3},{value:"<code>batching</code>",id:"batching-1",level:3},{value:"<code>batching.count</code>",id:"batchingcount",level:3},{value:"<code>batching.byte_size</code>",id:"batchingbyte_size",level:3},{value:"<code>batching.period</code>",id:"batchingperiod",level:3},{value:"<code>batching.check</code>",id:"batchingcheck",level:3},{value:"<code>batching.processors</code>",id:"batchingprocessors",level:3},{value:"<code>max_in_flight</code>",id:"max_in_flight",level:3},{value:"CosmosDB Emulator",id:"cosmosdb-emulator",level:2}];function u(e){const n={a:"a",admonition:"admonition",br:"br",code:"code",h2:"h2",h3:"h3",li:"li",p:"p",pre:"pre",table:"table",tbody:"tbody",td:"td",th:"th",thead:"thead",tr:"tr",ul:"ul",...(0,s.a)(),...e.components};return(0,o.jsxs)(o.Fragment,{children:[(0,o.jsx)(n.admonition,{title:"EXPERIMENTAL",type:"caution",children:(0,o.jsx)(n.p,{children:"This component is experimental and therefore subject to change or removal outside of major version releases."})}),"\n",(0,o.jsxs)(n.p,{children:["Creates or updates messages as JSON documents in ",(0,o.jsx)(n.a,{href:"https://learn.microsoft.com/en-us/azure/cosmos-db/introduction",children:"Azure CosmosDB"}),"."]}),"\n",(0,o.jsx)(n.p,{children:"Introduced in version v4.25.0."}),"\n",(0,o.jsxs)(i.Z,{defaultValue:"common",values:[{label:"Common",value:"common"},{label:"Advanced",value:"advanced"}],children:[(0,o.jsx)(r.Z,{value:"common",children:(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-yml",children:'# Common config fields, showing default values\noutput:\n  label: ""\n  azure_cosmosdb:\n    endpoint: https://localhost:8081 # No default (optional)\n    account_key: \'!!!SECRET_SCRUBBED!!!\' # No default (optional)\n    connection_string: \'!!!SECRET_SCRUBBED!!!\' # No default (optional)\n    database: testdb # No default (required)\n    container: testcontainer # No default (required)\n    partition_keys_map: root = "blobfish" # No default (required)\n    operation: Create\n    item_id: ${! json("id") } # No default (optional)\n    batching:\n      count: 0\n      byte_size: 0\n      period: ""\n      check: ""\n    max_in_flight: 64\n'})})}),(0,o.jsx)(r.Z,{value:"advanced",children:(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-yml",children:'# All config fields, showing default values\noutput:\n  label: ""\n  azure_cosmosdb:\n    endpoint: https://localhost:8081 # No default (optional)\n    account_key: \'!!!SECRET_SCRUBBED!!!\' # No default (optional)\n    connection_string: \'!!!SECRET_SCRUBBED!!!\' # No default (optional)\n    database: testdb # No default (required)\n    container: testcontainer # No default (required)\n    partition_keys_map: root = "blobfish" # No default (required)\n    operation: Create\n    patch_operations: [] # No default (optional)\n    patch_condition: from c where not is_defined(c.blobfish) # No default (optional)\n    auto_id: true\n    item_id: ${! json("id") } # No default (optional)\n    batching:\n      count: 0\n      byte_size: 0\n      period: ""\n      check: ""\n      processors: [] # No default (optional)\n    max_in_flight: 64\n'})})})]}),"\n",(0,o.jsxs)(n.p,{children:["When creating documents, each message must have the ",(0,o.jsx)(n.code,{children:"id"})," property (case-sensitive) set (or use ",(0,o.jsx)(n.code,{children:"auto_id: true"}),"). It is the unique name that identifies the document, that is, no two documents share the same ",(0,o.jsx)(n.code,{children:"id"})," within a logical partition. The ",(0,o.jsx)(n.code,{children:"id"})," field must not exceed 255 characters. More details can be found ",(0,o.jsx)(n.a,{href:"https://learn.microsoft.com/en-us/rest/api/cosmos-db/documents",children:"here"}),"."]}),"\n",(0,o.jsxs)(n.p,{children:["The ",(0,o.jsx)(n.code,{children:"partition_keys"})," field must resolve to the same value(s) across the entire message batch."]}),"\n",(0,o.jsx)(n.h2,{id:"credentials",children:"Credentials"}),"\n",(0,o.jsx)(n.p,{children:"You can use one of the following authentication mechanisms:"}),"\n",(0,o.jsxs)(n.ul,{children:["\n",(0,o.jsxs)(n.li,{children:["Set the ",(0,o.jsx)(n.code,{children:"endpoint"})," field and the ",(0,o.jsx)(n.code,{children:"account_key"})," field"]}),"\n",(0,o.jsxs)(n.li,{children:["Set only the ",(0,o.jsx)(n.code,{children:"endpoint"})," field to use ",(0,o.jsx)(n.a,{href:"https://pkg.go.dev/github.com/Azure/azure-sdk-for-go/sdk/azidentity#DefaultAzureCredential",children:"DefaultAzureCredential"})]}),"\n",(0,o.jsxs)(n.li,{children:["Set the ",(0,o.jsx)(n.code,{children:"connection_string"})," field"]}),"\n"]}),"\n",(0,o.jsx)(n.h2,{id:"batching",children:"Batching"}),"\n",(0,o.jsxs)(n.p,{children:["CosmosDB limits the maximum batch size to 100 messages and the payload must not exceed 2MB (details ",(0,o.jsx)(n.a,{href:"https://learn.microsoft.com/en-us/azure/cosmos-db/concepts-limits#per-request-limits",children:"here"}),")."]}),"\n",(0,o.jsx)(n.h2,{id:"performance",children:"Performance"}),"\n",(0,o.jsxs)(n.p,{children:["This output benefits from sending multiple messages in flight in parallel for improved performance. You can tune the max number of in flight messages (or message batches) with the field ",(0,o.jsx)(n.code,{children:"max_in_flight"}),"."]}),"\n",(0,o.jsxs)(n.p,{children:["This output benefits from sending messages as a batch for improved performance. Batches can be formed at both the input and output level. You can find out more ",(0,o.jsx)(n.a,{href:"/docs/configuration/batching",children:"in this doc"}),"."]}),"\n",(0,o.jsx)(n.h2,{id:"examples",children:"Examples"}),"\n",(0,o.jsxs)(i.Z,{defaultValue:"Create documents",values:[{label:"Create documents",value:"Create documents"},{label:"Patch documents",value:"Patch documents"}],children:[(0,o.jsxs)(r.Z,{value:"Create documents",children:[(0,o.jsxs)(n.p,{children:["Create new documents in the ",(0,o.jsx)(n.code,{children:"blobfish"})," container with partition key ",(0,o.jsx)(n.code,{children:"/habitat"}),"."]}),(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-yaml",children:'output:\n  azure_cosmosdb:\n    endpoint: http://localhost:8080\n    account_key: C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==\n    database: blobbase\n    container: blobfish\n    partition_keys_map: root = json("habitat")\n    operation: Create\n'})})]}),(0,o.jsxs)(r.Z,{value:"Patch documents",children:[(0,o.jsxs)(n.p,{children:["Execute the Patch operation on documents from the ",(0,o.jsx)(n.code,{children:"blobfish"})," container."]}),(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-yaml",children:'output:\n  azure_cosmosdb:\n    endpoint: http://localhost:8080\n    account_key: C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==\n    database: testdb\n    container: blobfish\n    partition_keys_map: root = json("habitat")\n    item_id: ${! json("id") }\n    operation: Patch\n    patch_operations:\n      # Add a new /diet field\n      - operation: Add\n        path: /diet\n        value_map: root = json("diet")\n      # Remove the first location from the /locations array field\n      - operation: Remove\n        path: /locations/0\n      # Add new location at the end of the /locations array field\n      - operation: Add\n        path: /locations/-\n        value_map: root = "Challenger Deep"\n'})})]})]}),"\n",(0,o.jsx)(n.h2,{id:"fields",children:"Fields"}),"\n",(0,o.jsx)(n.h3,{id:"endpoint",children:(0,o.jsx)(n.code,{children:"endpoint"})}),"\n",(0,o.jsx)(n.p,{children:"CosmosDB endpoint."}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"string"})]}),"\n",(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-yml",children:"# Examples\n\nendpoint: https://localhost:8081\n"})}),"\n",(0,o.jsx)(n.h3,{id:"account_key",children:(0,o.jsx)(n.code,{children:"account_key"})}),"\n",(0,o.jsx)(n.p,{children:"Account key."}),"\n",(0,o.jsx)(n.admonition,{title:"Secret",type:"warning",children:(0,o.jsxs)(n.p,{children:["This field contains sensitive information that usually shouldn't be added to a config directly, read our ",(0,o.jsx)(n.a,{href:"/docs/configuration/secrets",children:"secrets page for more info"}),"."]})}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"string"})]}),"\n",(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-yml",children:"# Examples\n\naccount_key: C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==\n"})}),"\n",(0,o.jsx)(n.h3,{id:"connection_string",children:(0,o.jsx)(n.code,{children:"connection_string"})}),"\n",(0,o.jsx)(n.p,{children:"Connection string."}),"\n",(0,o.jsx)(n.admonition,{title:"Secret",type:"warning",children:(0,o.jsxs)(n.p,{children:["This field contains sensitive information that usually shouldn't be added to a config directly, read our ",(0,o.jsx)(n.a,{href:"/docs/configuration/secrets",children:"secrets page for more info"}),"."]})}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"string"})]}),"\n",(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-yml",children:"# Examples\n\nconnection_string: AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==;\n"})}),"\n",(0,o.jsx)(n.h3,{id:"database",children:(0,o.jsx)(n.code,{children:"database"})}),"\n",(0,o.jsx)(n.p,{children:"Database."}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"string"})]}),"\n",(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-yml",children:"# Examples\n\ndatabase: testdb\n"})}),"\n",(0,o.jsx)(n.h3,{id:"container",children:(0,o.jsx)(n.code,{children:"container"})}),"\n",(0,o.jsx)(n.p,{children:"Container."}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"string"})]}),"\n",(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-yml",children:"# Examples\n\ncontainer: testcontainer\n"})}),"\n",(0,o.jsx)(n.h3,{id:"partition_keys_map",children:(0,o.jsx)(n.code,{children:"partition_keys_map"})}),"\n",(0,o.jsxs)(n.p,{children:["A ",(0,o.jsx)(n.a,{href:"/docs/guides/bloblang/about",children:"Bloblang mapping"})," which should evaluate to a single partition key value or an array of partition key values of type string, integer or boolean. Currently, hierarchical partition keys are not supported so only one value may be provided."]}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"string"})]}),"\n",(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-yml",children:'# Examples\n\npartition_keys_map: root = "blobfish"\n\npartition_keys_map: root = 41\n\npartition_keys_map: root = true\n\npartition_keys_map: root = null\n\npartition_keys_map: root = json("blobfish").depth\n'})}),"\n",(0,o.jsx)(n.h3,{id:"operation",children:(0,o.jsx)(n.code,{children:"operation"})}),"\n",(0,o.jsx)(n.p,{children:"Operation."}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"string"}),(0,o.jsx)(n.br,{}),"\n","Default: ",(0,o.jsx)(n.code,{children:'"Create"'})]}),"\n",(0,o.jsxs)(n.table,{children:[(0,o.jsx)(n.thead,{children:(0,o.jsxs)(n.tr,{children:[(0,o.jsx)(n.th,{children:"Option"}),(0,o.jsx)(n.th,{children:"Summary"})]})}),(0,o.jsxs)(n.tbody,{children:[(0,o.jsxs)(n.tr,{children:[(0,o.jsx)(n.td,{children:(0,o.jsx)(n.code,{children:"Create"})}),(0,o.jsx)(n.td,{children:"Create operation."})]}),(0,o.jsxs)(n.tr,{children:[(0,o.jsx)(n.td,{children:(0,o.jsx)(n.code,{children:"Delete"})}),(0,o.jsx)(n.td,{children:"Delete operation."})]}),(0,o.jsxs)(n.tr,{children:[(0,o.jsx)(n.td,{children:(0,o.jsx)(n.code,{children:"Patch"})}),(0,o.jsx)(n.td,{children:"Patch operation."})]}),(0,o.jsxs)(n.tr,{children:[(0,o.jsx)(n.td,{children:(0,o.jsx)(n.code,{children:"Replace"})}),(0,o.jsx)(n.td,{children:"Replace operation."})]}),(0,o.jsxs)(n.tr,{children:[(0,o.jsx)(n.td,{children:(0,o.jsx)(n.code,{children:"Upsert"})}),(0,o.jsx)(n.td,{children:"Upsert operation."})]})]})]}),"\n",(0,o.jsx)(n.h3,{id:"patch_operations",children:(0,o.jsx)(n.code,{children:"patch_operations"})}),"\n",(0,o.jsxs)(n.p,{children:["Patch operations to be performed when ",(0,o.jsx)(n.code,{children:"operation: Patch"})," ."]}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"array"})]}),"\n",(0,o.jsx)(n.h3,{id:"patch_operationsoperation",children:(0,o.jsx)(n.code,{children:"patch_operations[].operation"})}),"\n",(0,o.jsx)(n.p,{children:"Operation."}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"string"}),(0,o.jsx)(n.br,{}),"\n","Default: ",(0,o.jsx)(n.code,{children:'"Add"'})]}),"\n",(0,o.jsxs)(n.table,{children:[(0,o.jsx)(n.thead,{children:(0,o.jsxs)(n.tr,{children:[(0,o.jsx)(n.th,{children:"Option"}),(0,o.jsx)(n.th,{children:"Summary"})]})}),(0,o.jsxs)(n.tbody,{children:[(0,o.jsxs)(n.tr,{children:[(0,o.jsx)(n.td,{children:(0,o.jsx)(n.code,{children:"Add"})}),(0,o.jsx)(n.td,{children:"Add patch operation."})]}),(0,o.jsxs)(n.tr,{children:[(0,o.jsx)(n.td,{children:(0,o.jsx)(n.code,{children:"Increment"})}),(0,o.jsx)(n.td,{children:"Increment patch operation."})]}),(0,o.jsxs)(n.tr,{children:[(0,o.jsx)(n.td,{children:(0,o.jsx)(n.code,{children:"Remove"})}),(0,o.jsx)(n.td,{children:"Remove patch operation."})]}),(0,o.jsxs)(n.tr,{children:[(0,o.jsx)(n.td,{children:(0,o.jsx)(n.code,{children:"Replace"})}),(0,o.jsx)(n.td,{children:"Replace patch operation."})]}),(0,o.jsxs)(n.tr,{children:[(0,o.jsx)(n.td,{children:(0,o.jsx)(n.code,{children:"Set"})}),(0,o.jsx)(n.td,{children:"Set patch operation."})]})]})]}),"\n",(0,o.jsx)(n.h3,{id:"patch_operationspath",children:(0,o.jsx)(n.code,{children:"patch_operations[].path"})}),"\n",(0,o.jsx)(n.p,{children:"Path."}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"string"})]}),"\n",(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-yml",children:"# Examples\n\npath: /foo/bar/baz\n"})}),"\n",(0,o.jsx)(n.h3,{id:"patch_operationsvalue_map",children:(0,o.jsx)(n.code,{children:"patch_operations[].value_map"})}),"\n",(0,o.jsxs)(n.p,{children:["A ",(0,o.jsx)(n.a,{href:"/docs/guides/bloblang/about",children:"Bloblang mapping"})," which should evaluate to a value of any type that is supported by CosmosDB."]}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"string"})]}),"\n",(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-yml",children:'# Examples\n\nvalue_map: root = "blobfish"\n\nvalue_map: root = 41\n\nvalue_map: root = true\n\nvalue_map: root = json("blobfish").depth\n\nvalue_map: root = [1, 2, 3]\n'})}),"\n",(0,o.jsx)(n.h3,{id:"patch_condition",children:(0,o.jsx)(n.code,{children:"patch_condition"})}),"\n",(0,o.jsxs)(n.p,{children:["Patch operation condition.\nThis field supports ",(0,o.jsx)(n.a,{href:"/docs/configuration/interpolation#bloblang-queries",children:"interpolation functions"}),"."]}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"string"})]}),"\n",(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-yml",children:"# Examples\n\npatch_condition: from c where not is_defined(c.blobfish)\n"})}),"\n",(0,o.jsx)(n.h3,{id:"auto_id",children:(0,o.jsx)(n.code,{children:"auto_id"})}),"\n",(0,o.jsxs)(n.p,{children:["Automatically set the item ",(0,o.jsx)(n.code,{children:"id"})," field to a random UUID v4. If the ",(0,o.jsx)(n.code,{children:"id"})," field is already set, then it will not be overwritten. Setting this to ",(0,o.jsx)(n.code,{children:"false"})," can improve performance, since the messages will not have to be parsed."]}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"bool"}),(0,o.jsx)(n.br,{}),"\n","Default: ",(0,o.jsx)(n.code,{children:"true"})]}),"\n",(0,o.jsx)(n.h3,{id:"item_id",children:(0,o.jsx)(n.code,{children:"item_id"})}),"\n",(0,o.jsxs)(n.p,{children:["ID of item to replace or delete. Only used by the Replace and Delete operations\nThis field supports ",(0,o.jsx)(n.a,{href:"/docs/configuration/interpolation#bloblang-queries",children:"interpolation functions"}),"."]}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"string"})]}),"\n",(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-yml",children:'# Examples\n\nitem_id: ${! json("id") }\n'})}),"\n",(0,o.jsx)(n.h3,{id:"batching-1",children:(0,o.jsx)(n.code,{children:"batching"})}),"\n",(0,o.jsxs)(n.p,{children:["Allows you to configure a ",(0,o.jsx)(n.a,{href:"/docs/configuration/batching",children:"batching policy"}),"."]}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"object"})]}),"\n",(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-yml",children:'# Examples\n\nbatching:\n  byte_size: 5000\n  count: 0\n  period: 1s\n\nbatching:\n  count: 10\n  period: 1s\n\nbatching:\n  check: this.contains("END BATCH")\n  count: 0\n  period: 1m\n'})}),"\n",(0,o.jsx)(n.h3,{id:"batchingcount",children:(0,o.jsx)(n.code,{children:"batching.count"})}),"\n",(0,o.jsxs)(n.p,{children:["A number of messages at which the batch should be flushed. If ",(0,o.jsx)(n.code,{children:"0"})," disables count based batching."]}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"int"}),(0,o.jsx)(n.br,{}),"\n","Default: ",(0,o.jsx)(n.code,{children:"0"})]}),"\n",(0,o.jsx)(n.h3,{id:"batchingbyte_size",children:(0,o.jsx)(n.code,{children:"batching.byte_size"})}),"\n",(0,o.jsxs)(n.p,{children:["An amount of bytes at which the batch should be flushed. If ",(0,o.jsx)(n.code,{children:"0"})," disables size based batching."]}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"int"}),(0,o.jsx)(n.br,{}),"\n","Default: ",(0,o.jsx)(n.code,{children:"0"})]}),"\n",(0,o.jsx)(n.h3,{id:"batchingperiod",children:(0,o.jsx)(n.code,{children:"batching.period"})}),"\n",(0,o.jsx)(n.p,{children:"A period in which an incomplete batch should be flushed regardless of its size."}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"string"}),(0,o.jsx)(n.br,{}),"\n","Default: ",(0,o.jsx)(n.code,{children:'""'})]}),"\n",(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-yml",children:"# Examples\n\nperiod: 1s\n\nperiod: 1m\n\nperiod: 500ms\n"})}),"\n",(0,o.jsx)(n.h3,{id:"batchingcheck",children:(0,o.jsx)(n.code,{children:"batching.check"})}),"\n",(0,o.jsxs)(n.p,{children:["A ",(0,o.jsx)(n.a,{href:"/docs/guides/bloblang/about/",children:"Bloblang query"})," that should return a boolean value indicating whether a message should end a batch."]}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"string"}),(0,o.jsx)(n.br,{}),"\n","Default: ",(0,o.jsx)(n.code,{children:'""'})]}),"\n",(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-yml",children:'# Examples\n\ncheck: this.type == "end_of_transaction"\n'})}),"\n",(0,o.jsx)(n.h3,{id:"batchingprocessors",children:(0,o.jsx)(n.code,{children:"batching.processors"})}),"\n",(0,o.jsxs)(n.p,{children:["A list of ",(0,o.jsx)(n.a,{href:"/docs/components/processors/about",children:"processors"})," to apply to a batch as it is flushed. This allows you to aggregate and archive the batch however you see fit. Please note that all resulting messages are flushed as a single batch, therefore splitting the batch into smaller batches using these processors is a no-op."]}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"array"})]}),"\n",(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-yml",children:"# Examples\n\nprocessors:\n  - archive:\n      format: concatenate\n\nprocessors:\n  - archive:\n      format: lines\n\nprocessors:\n  - archive:\n      format: json_array\n"})}),"\n",(0,o.jsx)(n.h3,{id:"max_in_flight",children:(0,o.jsx)(n.code,{children:"max_in_flight"})}),"\n",(0,o.jsx)(n.p,{children:"The maximum number of messages to have in flight at a given time. Increase this to improve throughput."}),"\n",(0,o.jsxs)(n.p,{children:["Type: ",(0,o.jsx)(n.code,{children:"int"}),(0,o.jsx)(n.br,{}),"\n","Default: ",(0,o.jsx)(n.code,{children:"64"})]}),"\n",(0,o.jsx)(n.h2,{id:"cosmosdb-emulator",children:"CosmosDB Emulator"}),"\n",(0,o.jsxs)(n.p,{children:["If you wish to run the CosmosDB emulator that is referenced in the documentation ",(0,o.jsx)(n.a,{href:"https://learn.microsoft.com/en-us/azure/cosmos-db/linux-emulator",children:"here"}),", the following Docker command should do the trick:"]}),"\n",(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-shell",children:"> docker run --rm -it -p 8081:8081 --name=cosmosdb -e AZURE_COSMOS_EMULATOR_PARTITION_COUNT=10 -e AZURE_COSMOS_EMULATOR_ENABLE_DATA_PERSISTENCE=false mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator\n"})}),"\n",(0,o.jsxs)(n.p,{children:["Note: ",(0,o.jsx)(n.code,{children:"AZURE_COSMOS_EMULATOR_PARTITION_COUNT"})," controls the number of partitions that will be supported by the emulator. The bigger the value, the longer it takes for the container to start up."]}),"\n",(0,o.jsxs)(n.p,{children:["Additionally, instead of installing the container self-signed certificate which is exposed via ",(0,o.jsx)(n.code,{children:"https://localhost:8081/_explorer/emulator.pem"}),", you can run ",(0,o.jsx)(n.a,{href:"https://mitmproxy.org/",children:"mitmproxy"})," like so:"]}),"\n",(0,o.jsx)(n.pre,{children:(0,o.jsx)(n.code,{className:"language-shell",children:'> mitmproxy -k --mode "reverse:https://localhost:8081"\n'})}),"\n",(0,o.jsxs)(n.p,{children:["Then you can access the CosmosDB UI via ",(0,o.jsx)(n.code,{children:"http://localhost:8080/_explorer/index.html"})," and use ",(0,o.jsx)(n.code,{children:"http://localhost:8080"})," as the CosmosDB endpoint."]})]})}function p(e={}){const{wrapper:n}={...(0,s.a)(),...e.components};return n?(0,o.jsx)(n,{...e,children:(0,o.jsx)(u,{...e})}):u(e)}},5162:function(e,n,t){t.d(n,{Z:function(){return r}});t(7294);var o=t(6010),s={tabItem:"tabItem_Ymn6"},i=t(5893);function r(e){let{children:n,hidden:t,className:r}=e;return(0,i.jsx)("div",{role:"tabpanel",className:(0,o.Z)(s.tabItem,r),hidden:t,children:n})}},4866:function(e,n,t){t.d(n,{Z:function(){return _}});var o=t(7294),s=t(6010),i=t(2466),r=t(6550),a=t(469),c=t(1980),l=t(7392),d=t(12);function h(e){var n,t;return null!=(n=null==(t=o.Children.toArray(e).filter((e=>"\n"!==e)).map((e=>{if(!e||(0,o.isValidElement)(e)&&function(e){const{props:n}=e;return!!n&&"object"==typeof n&&"value"in n}(e))return e;throw new Error("Docusaurus error: Bad <Tabs> child <"+("string"==typeof e.type?e.type:e.type.name)+'>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.')})))?void 0:t.filter(Boolean))?n:[]}function u(e){const{values:n,children:t}=e;return(0,o.useMemo)((()=>{const e=null!=n?n:function(e){return h(e).map((e=>{let{props:{value:n,label:t,attributes:o,default:s}}=e;return{value:n,label:t,attributes:o,default:s}}))}(t);return function(e){const n=(0,l.l)(e,((e,n)=>e.value===n.value));if(n.length>0)throw new Error('Docusaurus error: Duplicate values "'+n.map((e=>e.value)).join(", ")+'" found in <Tabs>. Every value needs to be unique.')}(e),e}),[n,t])}function p(e){let{value:n,tabValues:t}=e;return t.some((e=>e.value===n))}function m(e){let{queryString:n=!1,groupId:t}=e;const s=(0,r.k6)(),i=function(e){let{queryString:n=!1,groupId:t}=e;if("string"==typeof n)return n;if(!1===n)return null;if(!0===n&&!t)throw new Error('Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".');return null!=t?t:null}({queryString:n,groupId:t});return[(0,c._X)(i),(0,o.useCallback)((e=>{if(!i)return;const n=new URLSearchParams(s.location.search);n.set(i,e),s.replace({...s.location,search:n.toString()})}),[i,s])]}function x(e){const{defaultValue:n,queryString:t=!1,groupId:s}=e,i=u(e),[r,c]=(0,o.useState)((()=>function(e){var n;let{defaultValue:t,tabValues:o}=e;if(0===o.length)throw new Error("Docusaurus error: the <Tabs> component requires at least one <TabItem> children component");if(t){if(!p({value:t,tabValues:o}))throw new Error('Docusaurus error: The <Tabs> has a defaultValue "'+t+'" but none of its children has the corresponding value. Available values are: '+o.map((e=>e.value)).join(", ")+". If you intend to show no default tab, use defaultValue={null} instead.");return t}const s=null!=(n=o.find((e=>e.default)))?n:o[0];if(!s)throw new Error("Unexpected error: 0 tabValues");return s.value}({defaultValue:n,tabValues:i}))),[l,h]=m({queryString:t,groupId:s}),[x,j]=function(e){let{groupId:n}=e;const t=function(e){return e?"docusaurus.tab."+e:null}(n),[s,i]=(0,d.Nk)(t);return[s,(0,o.useCallback)((e=>{t&&i.set(e)}),[t,i])]}({groupId:s}),b=(()=>{const e=null!=l?l:x;return p({value:e,tabValues:i})?e:null})();(0,a.Z)((()=>{b&&c(b)}),[b]);return{selectedValue:r,selectValue:(0,o.useCallback)((e=>{if(!p({value:e,tabValues:i}))throw new Error("Can't select invalid tab value="+e);c(e),h(e),j(e)}),[h,j,i]),tabValues:i}}var j=t(2389),b={tabList:"tabList__CuJ",tabItem:"tabItem_LNqP"},f=t(5893);function g(e){let{className:n,block:t,selectedValue:o,selectValue:r,tabValues:a}=e;const c=[],{blockElementScrollPositionUntilNextRender:l}=(0,i.o5)(),d=e=>{const n=e.currentTarget,t=c.indexOf(n),s=a[t].value;s!==o&&(l(n),r(s))},h=e=>{var n;let t=null;switch(e.key){case"Enter":d(e);break;case"ArrowRight":{var o;const n=c.indexOf(e.currentTarget)+1;t=null!=(o=c[n])?o:c[0];break}case"ArrowLeft":{var s;const n=c.indexOf(e.currentTarget)-1;t=null!=(s=c[n])?s:c[c.length-1];break}}null==(n=t)||n.focus()};return(0,f.jsx)("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,s.Z)("tabs",{"tabs--block":t},n),children:a.map((e=>{let{value:n,label:t,attributes:i}=e;return(0,f.jsx)("li",{role:"tab",tabIndex:o===n?0:-1,"aria-selected":o===n,ref:e=>c.push(e),onKeyDown:h,onClick:d,...i,className:(0,s.Z)("tabs__item",b.tabItem,null==i?void 0:i.className,{"tabs__item--active":o===n}),children:null!=t?t:n},n)}))})}function v(e){let{lazy:n,children:t,selectedValue:s}=e;const i=(Array.isArray(t)?t:[t]).filter(Boolean);if(n){const e=i.find((e=>e.props.value===s));return e?(0,o.cloneElement)(e,{className:"margin-top--md"}):null}return(0,f.jsx)("div",{className:"margin-top--md",children:i.map(((e,n)=>(0,o.cloneElement)(e,{key:n,hidden:e.props.value!==s})))})}function y(e){const n=x(e);return(0,f.jsxs)("div",{className:(0,s.Z)("tabs-container",b.tabList),children:[(0,f.jsx)(g,{...e,...n}),(0,f.jsx)(v,{...e,...n})]})}function _(e){const n=(0,j.Z)();return(0,f.jsx)(y,{...e,children:h(e.children)},String(n))}},1151:function(e,n,t){t.d(n,{Z:function(){return a},a:function(){return r}});var o=t(7294);const s={},i=o.createContext(s);function r(e){const n=o.useContext(i);return o.useMemo((function(){return"function"==typeof e?e(n):{...n,...e}}),[n,e])}function a(e){let n;return n=e.disableParentContext?"function"==typeof e.components?e.components(s):e.components||s:r(e.components),o.createElement(i.Provider,{value:n},e.children)}}}]);