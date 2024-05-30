"use strict";(self.webpackChunkbento=self.webpackChunkbento||[]).push([[3140],{2661:function(e,n,s){s.r(n),s.d(n,{assets:function(){return d},contentTitle:function(){return l},default:function(){return p},frontMatter:function(){return r},metadata:function(){return a},toc:function(){return h}});var t=s(5893),i=s(1151),c=s(4866),o=s(5162);const r={title:"aws_s3",slug:"aws_s3",type:"output",status:"stable",categories:["Services","AWS"],name:"aws_s3"},l=void 0,a={id:"components/outputs/aws_s3",title:"aws_s3",description:"\x3c!--",source:"@site/docs/components/outputs/aws_s3.md",sourceDirName:"components/outputs",slug:"/components/outputs/aws_s3",permalink:"/bento/docs/components/outputs/aws_s3",draft:!1,unlisted:!1,editUrl:"https://github.com/warpstreamlabs/bento/edit/main/website/docs/components/outputs/aws_s3.md",tags:[],version:"current",frontMatter:{title:"aws_s3",slug:"aws_s3",type:"output",status:"stable",categories:["Services","AWS"],name:"aws_s3"},sidebar:"docs",previous:{title:"aws_kinesis_firehose",permalink:"/bento/docs/components/outputs/aws_kinesis_firehose"},next:{title:"aws_sns",permalink:"/bento/docs/components/outputs/aws_sns"}},d={},h=[{value:"Metadata",id:"metadata",level:3},{value:"Tags",id:"tags",level:3},{value:"Credentials",id:"credentials",level:3},{value:"Batching",id:"batching",level:3},{value:"Performance",id:"performance",level:2},{value:"Fields",id:"fields",level:2},{value:"<code>bucket</code>",id:"bucket",level:3},{value:"<code>path</code>",id:"path",level:3},{value:"<code>tags</code>",id:"tags-1",level:3},{value:"<code>content_type</code>",id:"content_type",level:3},{value:"<code>content_encoding</code>",id:"content_encoding",level:3},{value:"<code>cache_control</code>",id:"cache_control",level:3},{value:"<code>content_disposition</code>",id:"content_disposition",level:3},{value:"<code>content_language</code>",id:"content_language",level:3},{value:"<code>website_redirect_location</code>",id:"website_redirect_location",level:3},{value:"<code>metadata</code>",id:"metadata-1",level:3},{value:"<code>metadata.exclude_prefixes</code>",id:"metadataexclude_prefixes",level:3},{value:"<code>storage_class</code>",id:"storage_class",level:3},{value:"<code>kms_key_id</code>",id:"kms_key_id",level:3},{value:"<code>server_side_encryption</code>",id:"server_side_encryption",level:3},{value:"<code>force_path_style_urls</code>",id:"force_path_style_urls",level:3},{value:"<code>max_in_flight</code>",id:"max_in_flight",level:3},{value:"<code>timeout</code>",id:"timeout",level:3},{value:"<code>batching</code>",id:"batching-1",level:3},{value:"<code>batching.count</code>",id:"batchingcount",level:3},{value:"<code>batching.byte_size</code>",id:"batchingbyte_size",level:3},{value:"<code>batching.period</code>",id:"batchingperiod",level:3},{value:"<code>batching.check</code>",id:"batchingcheck",level:3},{value:"<code>batching.processors</code>",id:"batchingprocessors",level:3},{value:"<code>region</code>",id:"region",level:3},{value:"<code>endpoint</code>",id:"endpoint",level:3},{value:"<code>credentials</code>",id:"credentials-1",level:3},{value:"<code>credentials.profile</code>",id:"credentialsprofile",level:3},{value:"<code>credentials.id</code>",id:"credentialsid",level:3},{value:"<code>credentials.secret</code>",id:"credentialssecret",level:3},{value:"<code>credentials.token</code>",id:"credentialstoken",level:3},{value:"<code>credentials.from_ec2_role</code>",id:"credentialsfrom_ec2_role",level:3},{value:"<code>credentials.role</code>",id:"credentialsrole",level:3},{value:"<code>credentials.role_external_id</code>",id:"credentialsrole_external_id",level:3}];function u(e){const n={a:"a",admonition:"admonition",br:"br",code:"code",h2:"h2",h3:"h3",p:"p",pre:"pre",...(0,i.a)(),...e.components};return(0,t.jsxs)(t.Fragment,{children:[(0,t.jsxs)(n.p,{children:["Sends message parts as objects to an Amazon S3 bucket. Each object is uploaded with the path specified with the ",(0,t.jsx)(n.code,{children:"path"})," field."]}),"\n",(0,t.jsx)(n.p,{children:"Introduced in version 3.36.0."}),"\n",(0,t.jsxs)(c.Z,{defaultValue:"common",values:[{label:"Common",value:"common"},{label:"Advanced",value:"advanced"}],children:[(0,t.jsx)(o.Z,{value:"common",children:(0,t.jsx)(n.pre,{children:(0,t.jsx)(n.code,{className:"language-yml",children:'# Common config fields, showing default values\noutput:\n  label: ""\n  aws_s3:\n    bucket: "" # No default (required)\n    path: ${!count("files")}-${!timestamp_unix_nano()}.txt\n    tags: {}\n    content_type: application/octet-stream\n    metadata:\n      exclude_prefixes: []\n    max_in_flight: 64\n    batching:\n      count: 0\n      byte_size: 0\n      period: ""\n      check: ""\n'})})}),(0,t.jsx)(o.Z,{value:"advanced",children:(0,t.jsx)(n.pre,{children:(0,t.jsx)(n.code,{className:"language-yml",children:'# All config fields, showing default values\noutput:\n  label: ""\n  aws_s3:\n    bucket: "" # No default (required)\n    path: ${!count("files")}-${!timestamp_unix_nano()}.txt\n    tags: {}\n    content_type: application/octet-stream\n    content_encoding: ""\n    cache_control: ""\n    content_disposition: ""\n    content_language: ""\n    website_redirect_location: ""\n    metadata:\n      exclude_prefixes: []\n    storage_class: STANDARD\n    kms_key_id: ""\n    server_side_encryption: ""\n    force_path_style_urls: false\n    max_in_flight: 64\n    timeout: 5s\n    batching:\n      count: 0\n      byte_size: 0\n      period: ""\n      check: ""\n      processors: [] # No default (optional)\n    region: ""\n    endpoint: ""\n    credentials:\n      profile: ""\n      id: ""\n      secret: ""\n      token: ""\n      from_ec2_role: false\n      role: ""\n      role_external_id: ""\n'})})})]}),"\n",(0,t.jsxs)(n.p,{children:["In order to have a different path for each object you should use function interpolations described ",(0,t.jsx)(n.a,{href:"/docs/configuration/interpolation#bloblang-queries",children:"here"}),", which are calculated per message of a batch."]}),"\n",(0,t.jsx)(n.h3,{id:"metadata",children:"Metadata"}),"\n",(0,t.jsxs)(n.p,{children:["Metadata fields on messages will be sent as headers, in order to mutate these values (or remove them) check out the ",(0,t.jsx)(n.a,{href:"/docs/configuration/metadata",children:"metadata docs"}),"."]}),"\n",(0,t.jsx)(n.h3,{id:"tags",children:"Tags"}),"\n",(0,t.jsxs)(n.p,{children:["The tags field allows you to specify key/value pairs to attach to objects as tags, where the values support ",(0,t.jsx)(n.a,{href:"/docs/configuration/interpolation#bloblang-queries",children:"interpolation functions"}),":"]}),"\n",(0,t.jsx)(n.pre,{children:(0,t.jsx)(n.code,{className:"language-yaml",children:'output:\n  aws_s3:\n    bucket: TODO\n    path: ${!count("files")}-${!timestamp_unix_nano()}.tar.gz\n    tags:\n      Key1: Value1\n      Timestamp: ${!meta("Timestamp")}\n'})}),"\n",(0,t.jsx)(n.h3,{id:"credentials",children:"Credentials"}),"\n",(0,t.jsxs)(n.p,{children:["By default Bento will use a shared credentials file when connecting to AWS services. It's also possible to set them explicitly at the component level, allowing you to transfer data across accounts. You can find out more ",(0,t.jsx)(n.a,{href:"/docs/guides/cloud/aws",children:"in this document"}),"."]}),"\n",(0,t.jsx)(n.h3,{id:"batching",children:"Batching"}),"\n",(0,t.jsxs)(n.p,{children:["It's common to want to upload messages to S3 as batched archives, the easiest way to do this is to batch your messages at the output level and join the batch of messages with an ",(0,t.jsx)(n.a,{href:"/docs/components/processors/archive",children:(0,t.jsx)(n.code,{children:"archive"})})," and/or ",(0,t.jsx)(n.a,{href:"/docs/components/processors/compress",children:(0,t.jsx)(n.code,{children:"compress"})})," processor."]}),"\n",(0,t.jsx)(n.p,{children:"For example, if we wished to upload messages as a .tar.gz archive of documents we could achieve that with the following config:"}),"\n",(0,t.jsx)(n.pre,{children:(0,t.jsx)(n.code,{className:"language-yaml",children:'output:\n  aws_s3:\n    bucket: TODO\n    path: ${!count("files")}-${!timestamp_unix_nano()}.tar.gz\n    batching:\n      count: 100\n      period: 10s\n      processors:\n        - archive:\n            format: tar\n        - compress:\n            algorithm: gzip\n'})}),"\n",(0,t.jsx)(n.p,{children:"Alternatively, if we wished to upload JSON documents as a single large document containing an array of objects we can do that with:"}),"\n",(0,t.jsx)(n.pre,{children:(0,t.jsx)(n.code,{className:"language-yaml",children:'output:\n  aws_s3:\n    bucket: TODO\n    path: ${!count("files")}-${!timestamp_unix_nano()}.json\n    batching:\n      count: 100\n      processors:\n        - archive:\n            format: json_array\n'})}),"\n",(0,t.jsx)(n.h2,{id:"performance",children:"Performance"}),"\n",(0,t.jsxs)(n.p,{children:["This output benefits from sending multiple messages in flight in parallel for improved performance. You can tune the max number of in flight messages (or message batches) with the field ",(0,t.jsx)(n.code,{children:"max_in_flight"}),"."]}),"\n",(0,t.jsx)(n.h2,{id:"fields",children:"Fields"}),"\n",(0,t.jsx)(n.h3,{id:"bucket",children:(0,t.jsx)(n.code,{children:"bucket"})}),"\n",(0,t.jsx)(n.p,{children:"The bucket to upload messages to."}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"string"})]}),"\n",(0,t.jsx)(n.h3,{id:"path",children:(0,t.jsx)(n.code,{children:"path"})}),"\n",(0,t.jsxs)(n.p,{children:["The path of each message to upload.\nThis field supports ",(0,t.jsx)(n.a,{href:"/docs/configuration/interpolation#bloblang-queries",children:"interpolation functions"}),"."]}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"string"}),(0,t.jsx)(n.br,{}),"\n","Default: ",(0,t.jsx)(n.code,{children:'"${!count(\\"files\\")}-${!timestamp_unix_nano()}.txt"'})]}),"\n",(0,t.jsx)(n.pre,{children:(0,t.jsx)(n.code,{className:"language-yml",children:'# Examples\n\npath: ${!count("files")}-${!timestamp_unix_nano()}.txt\n\npath: ${!meta("kafka_key")}.json\n\npath: ${!json("doc.namespace")}/${!json("doc.id")}.json\n'})}),"\n",(0,t.jsx)(n.h3,{id:"tags-1",children:(0,t.jsx)(n.code,{children:"tags"})}),"\n",(0,t.jsxs)(n.p,{children:["Key/value pairs to store with the object as tags.\nThis field supports ",(0,t.jsx)(n.a,{href:"/docs/configuration/interpolation#bloblang-queries",children:"interpolation functions"}),"."]}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"object"}),(0,t.jsx)(n.br,{}),"\n","Default: ",(0,t.jsx)(n.code,{children:"{}"})]}),"\n",(0,t.jsx)(n.pre,{children:(0,t.jsx)(n.code,{className:"language-yml",children:'# Examples\n\ntags:\n  Key1: Value1\n  Timestamp: ${!meta("Timestamp")}\n'})}),"\n",(0,t.jsx)(n.h3,{id:"content_type",children:(0,t.jsx)(n.code,{children:"content_type"})}),"\n",(0,t.jsxs)(n.p,{children:["The content type to set for each object.\nThis field supports ",(0,t.jsx)(n.a,{href:"/docs/configuration/interpolation#bloblang-queries",children:"interpolation functions"}),"."]}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"string"}),(0,t.jsx)(n.br,{}),"\n","Default: ",(0,t.jsx)(n.code,{children:'"application/octet-stream"'})]}),"\n",(0,t.jsx)(n.h3,{id:"content_encoding",children:(0,t.jsx)(n.code,{children:"content_encoding"})}),"\n",(0,t.jsxs)(n.p,{children:["An optional content encoding to set for each object.\nThis field supports ",(0,t.jsx)(n.a,{href:"/docs/configuration/interpolation#bloblang-queries",children:"interpolation functions"}),"."]}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"string"}),(0,t.jsx)(n.br,{}),"\n","Default: ",(0,t.jsx)(n.code,{children:'""'})]}),"\n",(0,t.jsx)(n.h3,{id:"cache_control",children:(0,t.jsx)(n.code,{children:"cache_control"})}),"\n",(0,t.jsxs)(n.p,{children:["The cache control to set for each object.\nThis field supports ",(0,t.jsx)(n.a,{href:"/docs/configuration/interpolation#bloblang-queries",children:"interpolation functions"}),"."]}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"string"}),(0,t.jsx)(n.br,{}),"\n","Default: ",(0,t.jsx)(n.code,{children:'""'})]}),"\n",(0,t.jsx)(n.h3,{id:"content_disposition",children:(0,t.jsx)(n.code,{children:"content_disposition"})}),"\n",(0,t.jsxs)(n.p,{children:["The content disposition to set for each object.\nThis field supports ",(0,t.jsx)(n.a,{href:"/docs/configuration/interpolation#bloblang-queries",children:"interpolation functions"}),"."]}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"string"}),(0,t.jsx)(n.br,{}),"\n","Default: ",(0,t.jsx)(n.code,{children:'""'})]}),"\n",(0,t.jsx)(n.h3,{id:"content_language",children:(0,t.jsx)(n.code,{children:"content_language"})}),"\n",(0,t.jsxs)(n.p,{children:["The content language to set for each object.\nThis field supports ",(0,t.jsx)(n.a,{href:"/docs/configuration/interpolation#bloblang-queries",children:"interpolation functions"}),"."]}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"string"}),(0,t.jsx)(n.br,{}),"\n","Default: ",(0,t.jsx)(n.code,{children:'""'})]}),"\n",(0,t.jsx)(n.h3,{id:"website_redirect_location",children:(0,t.jsx)(n.code,{children:"website_redirect_location"})}),"\n",(0,t.jsxs)(n.p,{children:["The website redirect location to set for each object.\nThis field supports ",(0,t.jsx)(n.a,{href:"/docs/configuration/interpolation#bloblang-queries",children:"interpolation functions"}),"."]}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"string"}),(0,t.jsx)(n.br,{}),"\n","Default: ",(0,t.jsx)(n.code,{children:'""'})]}),"\n",(0,t.jsx)(n.h3,{id:"metadata-1",children:(0,t.jsx)(n.code,{children:"metadata"})}),"\n",(0,t.jsx)(n.p,{children:"Specify criteria for which metadata values are attached to objects as headers."}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"object"})]}),"\n",(0,t.jsx)(n.h3,{id:"metadataexclude_prefixes",children:(0,t.jsx)(n.code,{children:"metadata.exclude_prefixes"})}),"\n",(0,t.jsx)(n.p,{children:"Provide a list of explicit metadata key prefixes to be excluded when adding metadata to sent messages."}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"array"}),(0,t.jsx)(n.br,{}),"\n","Default: ",(0,t.jsx)(n.code,{children:"[]"})]}),"\n",(0,t.jsx)(n.h3,{id:"storage_class",children:(0,t.jsx)(n.code,{children:"storage_class"})}),"\n",(0,t.jsxs)(n.p,{children:["The storage class to set for each object.\nThis field supports ",(0,t.jsx)(n.a,{href:"/docs/configuration/interpolation#bloblang-queries",children:"interpolation functions"}),"."]}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"string"}),(0,t.jsx)(n.br,{}),"\n","Default: ",(0,t.jsx)(n.code,{children:'"STANDARD"'}),(0,t.jsx)(n.br,{}),"\n","Options: ",(0,t.jsx)(n.code,{children:"STANDARD"}),", ",(0,t.jsx)(n.code,{children:"REDUCED_REDUNDANCY"}),", ",(0,t.jsx)(n.code,{children:"GLACIER"}),", ",(0,t.jsx)(n.code,{children:"STANDARD_IA"}),", ",(0,t.jsx)(n.code,{children:"ONEZONE_IA"}),", ",(0,t.jsx)(n.code,{children:"INTELLIGENT_TIERING"}),", ",(0,t.jsx)(n.code,{children:"DEEP_ARCHIVE"}),"."]}),"\n",(0,t.jsx)(n.h3,{id:"kms_key_id",children:(0,t.jsx)(n.code,{children:"kms_key_id"})}),"\n",(0,t.jsx)(n.p,{children:"An optional server side encryption key."}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"string"}),(0,t.jsx)(n.br,{}),"\n","Default: ",(0,t.jsx)(n.code,{children:'""'})]}),"\n",(0,t.jsx)(n.h3,{id:"server_side_encryption",children:(0,t.jsx)(n.code,{children:"server_side_encryption"})}),"\n",(0,t.jsx)(n.p,{children:"An optional server side encryption algorithm."}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"string"}),(0,t.jsx)(n.br,{}),"\n","Default: ",(0,t.jsx)(n.code,{children:'""'}),(0,t.jsx)(n.br,{}),"\n","Requires version 3.63.0 or newer"]}),"\n",(0,t.jsx)(n.h3,{id:"force_path_style_urls",children:(0,t.jsx)(n.code,{children:"force_path_style_urls"})}),"\n",(0,t.jsx)(n.p,{children:"Forces the client API to use path style URLs, which helps when connecting to custom endpoints."}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"bool"}),(0,t.jsx)(n.br,{}),"\n","Default: ",(0,t.jsx)(n.code,{children:"false"})]}),"\n",(0,t.jsx)(n.h3,{id:"max_in_flight",children:(0,t.jsx)(n.code,{children:"max_in_flight"})}),"\n",(0,t.jsx)(n.p,{children:"The maximum number of messages to have in flight at a given time. Increase this to improve throughput."}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"int"}),(0,t.jsx)(n.br,{}),"\n","Default: ",(0,t.jsx)(n.code,{children:"64"})]}),"\n",(0,t.jsx)(n.h3,{id:"timeout",children:(0,t.jsx)(n.code,{children:"timeout"})}),"\n",(0,t.jsx)(n.p,{children:"The maximum period to wait on an upload before abandoning it and reattempting."}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"string"}),(0,t.jsx)(n.br,{}),"\n","Default: ",(0,t.jsx)(n.code,{children:'"5s"'})]}),"\n",(0,t.jsx)(n.h3,{id:"batching-1",children:(0,t.jsx)(n.code,{children:"batching"})}),"\n",(0,t.jsxs)(n.p,{children:["Allows you to configure a ",(0,t.jsx)(n.a,{href:"/docs/configuration/batching",children:"batching policy"}),"."]}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"object"})]}),"\n",(0,t.jsx)(n.pre,{children:(0,t.jsx)(n.code,{className:"language-yml",children:'# Examples\n\nbatching:\n  byte_size: 5000\n  count: 0\n  period: 1s\n\nbatching:\n  count: 10\n  period: 1s\n\nbatching:\n  check: this.contains("END BATCH")\n  count: 0\n  period: 1m\n'})}),"\n",(0,t.jsx)(n.h3,{id:"batchingcount",children:(0,t.jsx)(n.code,{children:"batching.count"})}),"\n",(0,t.jsxs)(n.p,{children:["A number of messages at which the batch should be flushed. If ",(0,t.jsx)(n.code,{children:"0"})," disables count based batching."]}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"int"}),(0,t.jsx)(n.br,{}),"\n","Default: ",(0,t.jsx)(n.code,{children:"0"})]}),"\n",(0,t.jsx)(n.h3,{id:"batchingbyte_size",children:(0,t.jsx)(n.code,{children:"batching.byte_size"})}),"\n",(0,t.jsxs)(n.p,{children:["An amount of bytes at which the batch should be flushed. If ",(0,t.jsx)(n.code,{children:"0"})," disables size based batching."]}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"int"}),(0,t.jsx)(n.br,{}),"\n","Default: ",(0,t.jsx)(n.code,{children:"0"})]}),"\n",(0,t.jsx)(n.h3,{id:"batchingperiod",children:(0,t.jsx)(n.code,{children:"batching.period"})}),"\n",(0,t.jsx)(n.p,{children:"A period in which an incomplete batch should be flushed regardless of its size."}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"string"}),(0,t.jsx)(n.br,{}),"\n","Default: ",(0,t.jsx)(n.code,{children:'""'})]}),"\n",(0,t.jsx)(n.pre,{children:(0,t.jsx)(n.code,{className:"language-yml",children:"# Examples\n\nperiod: 1s\n\nperiod: 1m\n\nperiod: 500ms\n"})}),"\n",(0,t.jsx)(n.h3,{id:"batchingcheck",children:(0,t.jsx)(n.code,{children:"batching.check"})}),"\n",(0,t.jsxs)(n.p,{children:["A ",(0,t.jsx)(n.a,{href:"/docs/guides/bloblang/about/",children:"Bloblang query"})," that should return a boolean value indicating whether a message should end a batch."]}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"string"}),(0,t.jsx)(n.br,{}),"\n","Default: ",(0,t.jsx)(n.code,{children:'""'})]}),"\n",(0,t.jsx)(n.pre,{children:(0,t.jsx)(n.code,{className:"language-yml",children:'# Examples\n\ncheck: this.type == "end_of_transaction"\n'})}),"\n",(0,t.jsx)(n.h3,{id:"batchingprocessors",children:(0,t.jsx)(n.code,{children:"batching.processors"})}),"\n",(0,t.jsxs)(n.p,{children:["A list of ",(0,t.jsx)(n.a,{href:"/docs/components/processors/about",children:"processors"})," to apply to a batch as it is flushed. This allows you to aggregate and archive the batch however you see fit. Please note that all resulting messages are flushed as a single batch, therefore splitting the batch into smaller batches using these processors is a no-op."]}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"array"})]}),"\n",(0,t.jsx)(n.pre,{children:(0,t.jsx)(n.code,{className:"language-yml",children:"# Examples\n\nprocessors:\n  - archive:\n      format: concatenate\n\nprocessors:\n  - archive:\n      format: lines\n\nprocessors:\n  - archive:\n      format: json_array\n"})}),"\n",(0,t.jsx)(n.h3,{id:"region",children:(0,t.jsx)(n.code,{children:"region"})}),"\n",(0,t.jsx)(n.p,{children:"The AWS region to target."}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"string"}),(0,t.jsx)(n.br,{}),"\n","Default: ",(0,t.jsx)(n.code,{children:'""'})]}),"\n",(0,t.jsx)(n.h3,{id:"endpoint",children:(0,t.jsx)(n.code,{children:"endpoint"})}),"\n",(0,t.jsx)(n.p,{children:"Allows you to specify a custom endpoint for the AWS API."}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"string"}),(0,t.jsx)(n.br,{}),"\n","Default: ",(0,t.jsx)(n.code,{children:'""'})]}),"\n",(0,t.jsx)(n.h3,{id:"credentials-1",children:(0,t.jsx)(n.code,{children:"credentials"})}),"\n",(0,t.jsxs)(n.p,{children:["Optional manual configuration of AWS credentials to use. More information can be found ",(0,t.jsx)(n.a,{href:"/docs/guides/cloud/aws",children:"in this document"}),"."]}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"object"})]}),"\n",(0,t.jsx)(n.h3,{id:"credentialsprofile",children:(0,t.jsx)(n.code,{children:"credentials.profile"})}),"\n",(0,t.jsxs)(n.p,{children:["A profile from ",(0,t.jsx)(n.code,{children:"~/.aws/credentials"})," to use."]}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"string"}),(0,t.jsx)(n.br,{}),"\n","Default: ",(0,t.jsx)(n.code,{children:'""'})]}),"\n",(0,t.jsx)(n.h3,{id:"credentialsid",children:(0,t.jsx)(n.code,{children:"credentials.id"})}),"\n",(0,t.jsx)(n.p,{children:"The ID of credentials to use."}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"string"}),(0,t.jsx)(n.br,{}),"\n","Default: ",(0,t.jsx)(n.code,{children:'""'})]}),"\n",(0,t.jsx)(n.h3,{id:"credentialssecret",children:(0,t.jsx)(n.code,{children:"credentials.secret"})}),"\n",(0,t.jsx)(n.p,{children:"The secret for the credentials being used."}),"\n",(0,t.jsx)(n.admonition,{title:"Secret",type:"warning",children:(0,t.jsxs)(n.p,{children:["This field contains sensitive information that usually shouldn't be added to a config directly, read our ",(0,t.jsx)(n.a,{href:"/docs/configuration/secrets",children:"secrets page for more info"}),"."]})}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"string"}),(0,t.jsx)(n.br,{}),"\n","Default: ",(0,t.jsx)(n.code,{children:'""'})]}),"\n",(0,t.jsx)(n.h3,{id:"credentialstoken",children:(0,t.jsx)(n.code,{children:"credentials.token"})}),"\n",(0,t.jsx)(n.p,{children:"The token for the credentials being used, required when using short term credentials."}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"string"}),(0,t.jsx)(n.br,{}),"\n","Default: ",(0,t.jsx)(n.code,{children:'""'})]}),"\n",(0,t.jsx)(n.h3,{id:"credentialsfrom_ec2_role",children:(0,t.jsx)(n.code,{children:"credentials.from_ec2_role"})}),"\n",(0,t.jsxs)(n.p,{children:["Use the credentials of a host EC2 machine configured to assume ",(0,t.jsx)(n.a,{href:"https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-ec2.html",children:"an IAM role associated with the instance"}),"."]}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"bool"}),(0,t.jsx)(n.br,{}),"\n","Default: ",(0,t.jsx)(n.code,{children:"false"}),(0,t.jsx)(n.br,{}),"\n","Requires version 4.2.0 or newer"]}),"\n",(0,t.jsx)(n.h3,{id:"credentialsrole",children:(0,t.jsx)(n.code,{children:"credentials.role"})}),"\n",(0,t.jsx)(n.p,{children:"A role ARN to assume."}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"string"}),(0,t.jsx)(n.br,{}),"\n","Default: ",(0,t.jsx)(n.code,{children:'""'})]}),"\n",(0,t.jsx)(n.h3,{id:"credentialsrole_external_id",children:(0,t.jsx)(n.code,{children:"credentials.role_external_id"})}),"\n",(0,t.jsx)(n.p,{children:"An external ID to provide when assuming a role."}),"\n",(0,t.jsxs)(n.p,{children:["Type: ",(0,t.jsx)(n.code,{children:"string"}),(0,t.jsx)(n.br,{}),"\n","Default: ",(0,t.jsx)(n.code,{children:'""'})]})]})}function p(e={}){const{wrapper:n}={...(0,i.a)(),...e.components};return n?(0,t.jsx)(n,{...e,children:(0,t.jsx)(u,{...e})}):u(e)}},5162:function(e,n,s){s.d(n,{Z:function(){return o}});s(7294);var t=s(6010),i={tabItem:"tabItem_Ymn6"},c=s(5893);function o(e){let{children:n,hidden:s,className:o}=e;return(0,c.jsx)("div",{role:"tabpanel",className:(0,t.Z)(i.tabItem,o),hidden:s,children:n})}},4866:function(e,n,s){s.d(n,{Z:function(){return y}});var t=s(7294),i=s(6010),c=s(2466),o=s(6550),r=s(469),l=s(1980),a=s(7392),d=s(12);function h(e){var n,s;return null!=(n=null==(s=t.Children.toArray(e).filter((e=>"\n"!==e)).map((e=>{if(!e||(0,t.isValidElement)(e)&&function(e){const{props:n}=e;return!!n&&"object"==typeof n&&"value"in n}(e))return e;throw new Error("Docusaurus error: Bad <Tabs> child <"+("string"==typeof e.type?e.type:e.type.name)+'>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.')})))?void 0:s.filter(Boolean))?n:[]}function u(e){const{values:n,children:s}=e;return(0,t.useMemo)((()=>{const e=null!=n?n:function(e){return h(e).map((e=>{let{props:{value:n,label:s,attributes:t,default:i}}=e;return{value:n,label:s,attributes:t,default:i}}))}(s);return function(e){const n=(0,a.l)(e,((e,n)=>e.value===n.value));if(n.length>0)throw new Error('Docusaurus error: Duplicate values "'+n.map((e=>e.value)).join(", ")+'" found in <Tabs>. Every value needs to be unique.')}(e),e}),[n,s])}function p(e){let{value:n,tabValues:s}=e;return s.some((e=>e.value===n))}function x(e){let{queryString:n=!1,groupId:s}=e;const i=(0,o.k6)(),c=function(e){let{queryString:n=!1,groupId:s}=e;if("string"==typeof n)return n;if(!1===n)return null;if(!0===n&&!s)throw new Error('Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".');return null!=s?s:null}({queryString:n,groupId:s});return[(0,l._X)(c),(0,t.useCallback)((e=>{if(!c)return;const n=new URLSearchParams(i.location.search);n.set(c,e),i.replace({...i.location,search:n.toString()})}),[c,i])]}function j(e){const{defaultValue:n,queryString:s=!1,groupId:i}=e,c=u(e),[o,l]=(0,t.useState)((()=>function(e){var n;let{defaultValue:s,tabValues:t}=e;if(0===t.length)throw new Error("Docusaurus error: the <Tabs> component requires at least one <TabItem> children component");if(s){if(!p({value:s,tabValues:t}))throw new Error('Docusaurus error: The <Tabs> has a defaultValue "'+s+'" but none of its children has the corresponding value. Available values are: '+t.map((e=>e.value)).join(", ")+". If you intend to show no default tab, use defaultValue={null} instead.");return s}const i=null!=(n=t.find((e=>e.default)))?n:t[0];if(!i)throw new Error("Unexpected error: 0 tabValues");return i.value}({defaultValue:n,tabValues:c}))),[a,h]=x({queryString:s,groupId:i}),[j,f]=function(e){let{groupId:n}=e;const s=function(e){return e?"docusaurus.tab."+e:null}(n),[i,c]=(0,d.Nk)(s);return[i,(0,t.useCallback)((e=>{s&&c.set(e)}),[s,c])]}({groupId:i}),m=(()=>{const e=null!=a?a:j;return p({value:e,tabValues:c})?e:null})();(0,r.Z)((()=>{m&&l(m)}),[m]);return{selectedValue:o,selectValue:(0,t.useCallback)((e=>{if(!p({value:e,tabValues:c}))throw new Error("Can't select invalid tab value="+e);l(e),h(e),f(e)}),[h,f,c]),tabValues:c}}var f=s(2389),m={tabList:"tabList__CuJ",tabItem:"tabItem_LNqP"},g=s(5893);function b(e){let{className:n,block:s,selectedValue:t,selectValue:o,tabValues:r}=e;const l=[],{blockElementScrollPositionUntilNextRender:a}=(0,c.o5)(),d=e=>{const n=e.currentTarget,s=l.indexOf(n),i=r[s].value;i!==t&&(a(n),o(i))},h=e=>{var n;let s=null;switch(e.key){case"Enter":d(e);break;case"ArrowRight":{var t;const n=l.indexOf(e.currentTarget)+1;s=null!=(t=l[n])?t:l[0];break}case"ArrowLeft":{var i;const n=l.indexOf(e.currentTarget)-1;s=null!=(i=l[n])?i:l[l.length-1];break}}null==(n=s)||n.focus()};return(0,g.jsx)("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,i.Z)("tabs",{"tabs--block":s},n),children:r.map((e=>{let{value:n,label:s,attributes:c}=e;return(0,g.jsx)("li",{role:"tab",tabIndex:t===n?0:-1,"aria-selected":t===n,ref:e=>l.push(e),onKeyDown:h,onClick:d,...c,className:(0,i.Z)("tabs__item",m.tabItem,null==c?void 0:c.className,{"tabs__item--active":t===n}),children:null!=s?s:n},n)}))})}function v(e){let{lazy:n,children:s,selectedValue:i}=e;const c=(Array.isArray(s)?s:[s]).filter(Boolean);if(n){const e=c.find((e=>e.props.value===i));return e?(0,t.cloneElement)(e,{className:"margin-top--md"}):null}return(0,g.jsx)("div",{className:"margin-top--md",children:c.map(((e,n)=>(0,t.cloneElement)(e,{key:n,hidden:e.props.value!==i})))})}function _(e){const n=j(e);return(0,g.jsxs)("div",{className:(0,i.Z)("tabs-container",m.tabList),children:[(0,g.jsx)(b,{...e,...n}),(0,g.jsx)(v,{...e,...n})]})}function y(e){const n=(0,f.Z)();return(0,g.jsx)(_,{...e,children:h(e.children)},String(n))}},1151:function(e,n,s){s.d(n,{Z:function(){return r},a:function(){return o}});var t=s(7294);const i={},c=t.createContext(i);function o(e){const n=t.useContext(c);return t.useMemo((function(){return"function"==typeof e?e(n):{...n,...e}}),[n,e])}function r(e){let n;return n=e.disableParentContext?"function"==typeof e.components?e.components(i):e.components||i:o(e.components),t.createElement(c.Provider,{value:n},e.children)}}}]);