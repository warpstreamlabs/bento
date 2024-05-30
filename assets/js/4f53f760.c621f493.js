"use strict";(self.webpackChunkbento=self.webpackChunkbento||[]).push([[2875],{8444:function(e,n,t){t.r(n),t.d(n,{assets:function(){return a},contentTitle:function(){return o},default:function(){return p},frontMatter:function(){return c},metadata:function(){return d},toc:function(){return u}});var i=t(5893),l=t(1151),s=t(4866),r=t(5162);const c={title:"mqtt",slug:"mqtt",type:"output",status:"stable",categories:["Services"],name:"mqtt"},o=void 0,d={id:"components/outputs/mqtt",title:"mqtt",description:"\x3c!--",source:"@site/docs/components/outputs/mqtt.md",sourceDirName:"components/outputs",slug:"/components/outputs/mqtt",permalink:"/bento/docs/components/outputs/mqtt",draft:!1,unlisted:!1,editUrl:"https://github.com/warpstreamlabs/bento/edit/main/website/docs/components/outputs/mqtt.md",tags:[],version:"current",frontMatter:{title:"mqtt",slug:"mqtt",type:"output",status:"stable",categories:["Services"],name:"mqtt"},sidebar:"docs",previous:{title:"mongodb",permalink:"/bento/docs/components/outputs/mongodb"},next:{title:"nanomsg",permalink:"/bento/docs/components/outputs/nanomsg"}},a={},u=[{value:"Performance",id:"performance",level:2},{value:"Fields",id:"fields",level:2},{value:"<code>urls</code>",id:"urls",level:3},{value:"<code>client_id</code>",id:"client_id",level:3},{value:"<code>dynamic_client_id_suffix</code>",id:"dynamic_client_id_suffix",level:3},{value:"<code>connect_timeout</code>",id:"connect_timeout",level:3},{value:"<code>will</code>",id:"will",level:3},{value:"<code>will.enabled</code>",id:"willenabled",level:3},{value:"<code>will.qos</code>",id:"willqos",level:3},{value:"<code>will.retained</code>",id:"willretained",level:3},{value:"<code>will.topic</code>",id:"willtopic",level:3},{value:"<code>will.payload</code>",id:"willpayload",level:3},{value:"<code>user</code>",id:"user",level:3},{value:"<code>password</code>",id:"password",level:3},{value:"<code>keepalive</code>",id:"keepalive",level:3},{value:"<code>tls</code>",id:"tls",level:3},{value:"<code>tls.enabled</code>",id:"tlsenabled",level:3},{value:"<code>tls.skip_cert_verify</code>",id:"tlsskip_cert_verify",level:3},{value:"<code>tls.enable_renegotiation</code>",id:"tlsenable_renegotiation",level:3},{value:"<code>tls.root_cas</code>",id:"tlsroot_cas",level:3},{value:"<code>tls.root_cas_file</code>",id:"tlsroot_cas_file",level:3},{value:"<code>tls.client_certs</code>",id:"tlsclient_certs",level:3},{value:"<code>tls.client_certs[].cert</code>",id:"tlsclient_certscert",level:3},{value:"<code>tls.client_certs[].key</code>",id:"tlsclient_certskey",level:3},{value:"<code>tls.client_certs[].cert_file</code>",id:"tlsclient_certscert_file",level:3},{value:"<code>tls.client_certs[].key_file</code>",id:"tlsclient_certskey_file",level:3},{value:"<code>tls.client_certs[].password</code>",id:"tlsclient_certspassword",level:3},{value:"<code>topic</code>",id:"topic",level:3},{value:"<code>qos</code>",id:"qos",level:3},{value:"<code>write_timeout</code>",id:"write_timeout",level:3},{value:"<code>retained</code>",id:"retained",level:3},{value:"<code>retained_interpolated</code>",id:"retained_interpolated",level:3},{value:"<code>max_in_flight</code>",id:"max_in_flight",level:3}];function h(e){const n={a:"a",admonition:"admonition",br:"br",code:"code",h2:"h2",h3:"h3",p:"p",pre:"pre",table:"table",tbody:"tbody",td:"td",th:"th",thead:"thead",tr:"tr",...(0,l.a)(),...e.components};return(0,i.jsxs)(i.Fragment,{children:[(0,i.jsx)(n.p,{children:"Pushes messages to an MQTT broker."}),"\n",(0,i.jsxs)(s.Z,{defaultValue:"common",values:[{label:"Common",value:"common"},{label:"Advanced",value:"advanced"}],children:[(0,i.jsx)(r.Z,{value:"common",children:(0,i.jsx)(n.pre,{children:(0,i.jsx)(n.code,{className:"language-yml",children:'# Common config fields, showing default values\noutput:\n  label: ""\n  mqtt:\n    urls: [] # No default (required)\n    client_id: ""\n    connect_timeout: 30s\n    topic: "" # No default (required)\n    qos: 1\n    write_timeout: 3s\n    retained: false\n    max_in_flight: 64\n'})})}),(0,i.jsx)(r.Z,{value:"advanced",children:(0,i.jsx)(n.pre,{children:(0,i.jsx)(n.code,{className:"language-yml",children:'# All config fields, showing default values\noutput:\n  label: ""\n  mqtt:\n    urls: [] # No default (required)\n    client_id: ""\n    dynamic_client_id_suffix: "" # No default (optional)\n    connect_timeout: 30s\n    will:\n      enabled: false\n      qos: 0\n      retained: false\n      topic: ""\n      payload: ""\n    user: ""\n    password: ""\n    keepalive: 30\n    tls:\n      enabled: false\n      skip_cert_verify: false\n      enable_renegotiation: false\n      root_cas: ""\n      root_cas_file: ""\n      client_certs: []\n    topic: "" # No default (required)\n    qos: 1\n    write_timeout: 3s\n    retained: false\n    retained_interpolated: "" # No default (optional)\n    max_in_flight: 64\n'})})})]}),"\n",(0,i.jsxs)(n.p,{children:["The ",(0,i.jsx)(n.code,{children:"topic"})," field can be dynamically set using function interpolations described ",(0,i.jsx)(n.a,{href:"/docs/configuration/interpolation#bloblang-queries",children:"here"}),". When sending batched messages these interpolations are performed per message part."]}),"\n",(0,i.jsx)(n.h2,{id:"performance",children:"Performance"}),"\n",(0,i.jsxs)(n.p,{children:["This output benefits from sending multiple messages in flight in parallel for improved performance. You can tune the max number of in flight messages (or message batches) with the field ",(0,i.jsx)(n.code,{children:"max_in_flight"}),"."]}),"\n",(0,i.jsx)(n.h2,{id:"fields",children:"Fields"}),"\n",(0,i.jsx)(n.h3,{id:"urls",children:(0,i.jsx)(n.code,{children:"urls"})}),"\n",(0,i.jsx)(n.p,{children:"A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs."}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"array"})]}),"\n",(0,i.jsx)(n.pre,{children:(0,i.jsx)(n.code,{className:"language-yml",children:"# Examples\n\nurls:\n  - tcp://localhost:1883\n"})}),"\n",(0,i.jsx)(n.h3,{id:"client_id",children:(0,i.jsx)(n.code,{children:"client_id"})}),"\n",(0,i.jsx)(n.p,{children:"An identifier for the client connection."}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"string"}),(0,i.jsx)(n.br,{}),"\n","Default: ",(0,i.jsx)(n.code,{children:'""'})]}),"\n",(0,i.jsx)(n.h3,{id:"dynamic_client_id_suffix",children:(0,i.jsx)(n.code,{children:"dynamic_client_id_suffix"})}),"\n",(0,i.jsxs)(n.p,{children:["Append a dynamically generated suffix to the specified ",(0,i.jsx)(n.code,{children:"client_id"})," on each run of the pipeline. This can be useful when clustering Bento producers."]}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"string"})]}),"\n",(0,i.jsxs)(n.table,{children:[(0,i.jsx)(n.thead,{children:(0,i.jsxs)(n.tr,{children:[(0,i.jsx)(n.th,{children:"Option"}),(0,i.jsx)(n.th,{children:"Summary"})]})}),(0,i.jsx)(n.tbody,{children:(0,i.jsxs)(n.tr,{children:[(0,i.jsx)(n.td,{children:(0,i.jsx)(n.code,{children:"nanoid"})}),(0,i.jsx)(n.td,{children:"append a nanoid of length 21 characters"})]})})]}),"\n",(0,i.jsx)(n.h3,{id:"connect_timeout",children:(0,i.jsx)(n.code,{children:"connect_timeout"})}),"\n",(0,i.jsx)(n.p,{children:"The maximum amount of time to wait in order to establish a connection before the attempt is abandoned."}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"string"}),(0,i.jsx)(n.br,{}),"\n","Default: ",(0,i.jsx)(n.code,{children:'"30s"'}),(0,i.jsx)(n.br,{}),"\n","Requires version 3.58.0 or newer"]}),"\n",(0,i.jsx)(n.pre,{children:(0,i.jsx)(n.code,{className:"language-yml",children:"# Examples\n\nconnect_timeout: 1s\n\nconnect_timeout: 500ms\n"})}),"\n",(0,i.jsx)(n.h3,{id:"will",children:(0,i.jsx)(n.code,{children:"will"})}),"\n",(0,i.jsx)(n.p,{children:"Set last will message in case of Bento failure"}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"object"})]}),"\n",(0,i.jsx)(n.h3,{id:"willenabled",children:(0,i.jsx)(n.code,{children:"will.enabled"})}),"\n",(0,i.jsx)(n.p,{children:"Whether to enable last will messages."}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"bool"}),(0,i.jsx)(n.br,{}),"\n","Default: ",(0,i.jsx)(n.code,{children:"false"})]}),"\n",(0,i.jsx)(n.h3,{id:"willqos",children:(0,i.jsx)(n.code,{children:"will.qos"})}),"\n",(0,i.jsx)(n.p,{children:"Set QoS for last will message. Valid values are: 0, 1, 2."}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"int"}),(0,i.jsx)(n.br,{}),"\n","Default: ",(0,i.jsx)(n.code,{children:"0"})]}),"\n",(0,i.jsx)(n.h3,{id:"willretained",children:(0,i.jsx)(n.code,{children:"will.retained"})}),"\n",(0,i.jsx)(n.p,{children:"Set retained for last will message."}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"bool"}),(0,i.jsx)(n.br,{}),"\n","Default: ",(0,i.jsx)(n.code,{children:"false"})]}),"\n",(0,i.jsx)(n.h3,{id:"willtopic",children:(0,i.jsx)(n.code,{children:"will.topic"})}),"\n",(0,i.jsx)(n.p,{children:"Set topic for last will message."}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"string"}),(0,i.jsx)(n.br,{}),"\n","Default: ",(0,i.jsx)(n.code,{children:'""'})]}),"\n",(0,i.jsx)(n.h3,{id:"willpayload",children:(0,i.jsx)(n.code,{children:"will.payload"})}),"\n",(0,i.jsx)(n.p,{children:"Set payload for last will message."}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"string"}),(0,i.jsx)(n.br,{}),"\n","Default: ",(0,i.jsx)(n.code,{children:'""'})]}),"\n",(0,i.jsx)(n.h3,{id:"user",children:(0,i.jsx)(n.code,{children:"user"})}),"\n",(0,i.jsx)(n.p,{children:"A username to connect with."}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"string"}),(0,i.jsx)(n.br,{}),"\n","Default: ",(0,i.jsx)(n.code,{children:'""'})]}),"\n",(0,i.jsx)(n.h3,{id:"password",children:(0,i.jsx)(n.code,{children:"password"})}),"\n",(0,i.jsx)(n.p,{children:"A password to connect with."}),"\n",(0,i.jsx)(n.admonition,{title:"Secret",type:"warning",children:(0,i.jsxs)(n.p,{children:["This field contains sensitive information that usually shouldn't be added to a config directly, read our ",(0,i.jsx)(n.a,{href:"/docs/configuration/secrets",children:"secrets page for more info"}),"."]})}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"string"}),(0,i.jsx)(n.br,{}),"\n","Default: ",(0,i.jsx)(n.code,{children:'""'})]}),"\n",(0,i.jsx)(n.h3,{id:"keepalive",children:(0,i.jsx)(n.code,{children:"keepalive"})}),"\n",(0,i.jsx)(n.p,{children:"Max seconds of inactivity before a keepalive message is sent."}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"int"}),(0,i.jsx)(n.br,{}),"\n","Default: ",(0,i.jsx)(n.code,{children:"30"})]}),"\n",(0,i.jsx)(n.h3,{id:"tls",children:(0,i.jsx)(n.code,{children:"tls"})}),"\n",(0,i.jsx)(n.p,{children:"Custom TLS settings can be used to override system defaults."}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"object"})]}),"\n",(0,i.jsx)(n.h3,{id:"tlsenabled",children:(0,i.jsx)(n.code,{children:"tls.enabled"})}),"\n",(0,i.jsx)(n.p,{children:"Whether custom TLS settings are enabled."}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"bool"}),(0,i.jsx)(n.br,{}),"\n","Default: ",(0,i.jsx)(n.code,{children:"false"})]}),"\n",(0,i.jsx)(n.h3,{id:"tlsskip_cert_verify",children:(0,i.jsx)(n.code,{children:"tls.skip_cert_verify"})}),"\n",(0,i.jsx)(n.p,{children:"Whether to skip server side certificate verification."}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"bool"}),(0,i.jsx)(n.br,{}),"\n","Default: ",(0,i.jsx)(n.code,{children:"false"})]}),"\n",(0,i.jsx)(n.h3,{id:"tlsenable_renegotiation",children:(0,i.jsx)(n.code,{children:"tls.enable_renegotiation"})}),"\n",(0,i.jsxs)(n.p,{children:["Whether to allow the remote server to repeatedly request renegotiation. Enable this option if you're seeing the error message ",(0,i.jsx)(n.code,{children:"local error: tls: no renegotiation"}),"."]}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"bool"}),(0,i.jsx)(n.br,{}),"\n","Default: ",(0,i.jsx)(n.code,{children:"false"}),(0,i.jsx)(n.br,{}),"\n","Requires version 3.45.0 or newer"]}),"\n",(0,i.jsx)(n.h3,{id:"tlsroot_cas",children:(0,i.jsx)(n.code,{children:"tls.root_cas"})}),"\n",(0,i.jsx)(n.p,{children:"An optional root certificate authority to use. This is a string, representing a certificate chain from the parent trusted root certificate, to possible intermediate signing certificates, to the host certificate."}),"\n",(0,i.jsx)(n.admonition,{title:"Secret",type:"warning",children:(0,i.jsxs)(n.p,{children:["This field contains sensitive information that usually shouldn't be added to a config directly, read our ",(0,i.jsx)(n.a,{href:"/docs/configuration/secrets",children:"secrets page for more info"}),"."]})}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"string"}),(0,i.jsx)(n.br,{}),"\n","Default: ",(0,i.jsx)(n.code,{children:'""'})]}),"\n",(0,i.jsx)(n.pre,{children:(0,i.jsx)(n.code,{className:"language-yml",children:"# Examples\n\nroot_cas: |-\n  -----BEGIN CERTIFICATE-----\n  ...\n  -----END CERTIFICATE-----\n"})}),"\n",(0,i.jsx)(n.h3,{id:"tlsroot_cas_file",children:(0,i.jsx)(n.code,{children:"tls.root_cas_file"})}),"\n",(0,i.jsx)(n.p,{children:"An optional path of a root certificate authority file to use. This is a file, often with a .pem extension, containing a certificate chain from the parent trusted root certificate, to possible intermediate signing certificates, to the host certificate."}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"string"}),(0,i.jsx)(n.br,{}),"\n","Default: ",(0,i.jsx)(n.code,{children:'""'})]}),"\n",(0,i.jsx)(n.pre,{children:(0,i.jsx)(n.code,{className:"language-yml",children:"# Examples\n\nroot_cas_file: ./root_cas.pem\n"})}),"\n",(0,i.jsx)(n.h3,{id:"tlsclient_certs",children:(0,i.jsx)(n.code,{children:"tls.client_certs"})}),"\n",(0,i.jsxs)(n.p,{children:["A list of client certificates to use. For each certificate either the fields ",(0,i.jsx)(n.code,{children:"cert"})," and ",(0,i.jsx)(n.code,{children:"key"}),", or ",(0,i.jsx)(n.code,{children:"cert_file"})," and ",(0,i.jsx)(n.code,{children:"key_file"})," should be specified, but not both."]}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"array"}),(0,i.jsx)(n.br,{}),"\n","Default: ",(0,i.jsx)(n.code,{children:"[]"})]}),"\n",(0,i.jsx)(n.pre,{children:(0,i.jsx)(n.code,{className:"language-yml",children:"# Examples\n\nclient_certs:\n  - cert: foo\n    key: bar\n\nclient_certs:\n  - cert_file: ./example.pem\n    key_file: ./example.key\n"})}),"\n",(0,i.jsx)(n.h3,{id:"tlsclient_certscert",children:(0,i.jsx)(n.code,{children:"tls.client_certs[].cert"})}),"\n",(0,i.jsx)(n.p,{children:"A plain text certificate to use."}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"string"}),(0,i.jsx)(n.br,{}),"\n","Default: ",(0,i.jsx)(n.code,{children:'""'})]}),"\n",(0,i.jsx)(n.h3,{id:"tlsclient_certskey",children:(0,i.jsx)(n.code,{children:"tls.client_certs[].key"})}),"\n",(0,i.jsx)(n.p,{children:"A plain text certificate key to use."}),"\n",(0,i.jsx)(n.admonition,{title:"Secret",type:"warning",children:(0,i.jsxs)(n.p,{children:["This field contains sensitive information that usually shouldn't be added to a config directly, read our ",(0,i.jsx)(n.a,{href:"/docs/configuration/secrets",children:"secrets page for more info"}),"."]})}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"string"}),(0,i.jsx)(n.br,{}),"\n","Default: ",(0,i.jsx)(n.code,{children:'""'})]}),"\n",(0,i.jsx)(n.h3,{id:"tlsclient_certscert_file",children:(0,i.jsx)(n.code,{children:"tls.client_certs[].cert_file"})}),"\n",(0,i.jsx)(n.p,{children:"The path of a certificate to use."}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"string"}),(0,i.jsx)(n.br,{}),"\n","Default: ",(0,i.jsx)(n.code,{children:'""'})]}),"\n",(0,i.jsx)(n.h3,{id:"tlsclient_certskey_file",children:(0,i.jsx)(n.code,{children:"tls.client_certs[].key_file"})}),"\n",(0,i.jsx)(n.p,{children:"The path of a certificate key to use."}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"string"}),(0,i.jsx)(n.br,{}),"\n","Default: ",(0,i.jsx)(n.code,{children:'""'})]}),"\n",(0,i.jsx)(n.h3,{id:"tlsclient_certspassword",children:(0,i.jsx)(n.code,{children:"tls.client_certs[].password"})}),"\n",(0,i.jsxs)(n.p,{children:["A plain text password for when the private key is password encrypted in PKCS#1 or PKCS#8 format. The obsolete ",(0,i.jsx)(n.code,{children:"pbeWithMD5AndDES-CBC"})," algorithm is not supported for the PKCS#8 format. Warning: Since it does not authenticate the ciphertext, it is vulnerable to padding oracle attacks that can let an attacker recover the plaintext."]}),"\n",(0,i.jsx)(n.admonition,{title:"Secret",type:"warning",children:(0,i.jsxs)(n.p,{children:["This field contains sensitive information that usually shouldn't be added to a config directly, read our ",(0,i.jsx)(n.a,{href:"/docs/configuration/secrets",children:"secrets page for more info"}),"."]})}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"string"}),(0,i.jsx)(n.br,{}),"\n","Default: ",(0,i.jsx)(n.code,{children:'""'})]}),"\n",(0,i.jsx)(n.pre,{children:(0,i.jsx)(n.code,{className:"language-yml",children:"# Examples\n\npassword: foo\n\npassword: ${KEY_PASSWORD}\n"})}),"\n",(0,i.jsx)(n.h3,{id:"topic",children:(0,i.jsx)(n.code,{children:"topic"})}),"\n",(0,i.jsxs)(n.p,{children:["The topic to publish messages to.\nThis field supports ",(0,i.jsx)(n.a,{href:"/docs/configuration/interpolation#bloblang-queries",children:"interpolation functions"}),"."]}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"string"})]}),"\n",(0,i.jsx)(n.h3,{id:"qos",children:(0,i.jsx)(n.code,{children:"qos"})}),"\n",(0,i.jsx)(n.p,{children:"The QoS value to set for each message. Has options 0, 1, 2."}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"int"}),(0,i.jsx)(n.br,{}),"\n","Default: ",(0,i.jsx)(n.code,{children:"1"})]}),"\n",(0,i.jsx)(n.h3,{id:"write_timeout",children:(0,i.jsx)(n.code,{children:"write_timeout"})}),"\n",(0,i.jsx)(n.p,{children:"The maximum amount of time to wait to write data before the attempt is abandoned."}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"string"}),(0,i.jsx)(n.br,{}),"\n","Default: ",(0,i.jsx)(n.code,{children:'"3s"'}),(0,i.jsx)(n.br,{}),"\n","Requires version 3.58.0 or newer"]}),"\n",(0,i.jsx)(n.pre,{children:(0,i.jsx)(n.code,{className:"language-yml",children:"# Examples\n\nwrite_timeout: 1s\n\nwrite_timeout: 500ms\n"})}),"\n",(0,i.jsx)(n.h3,{id:"retained",children:(0,i.jsx)(n.code,{children:"retained"})}),"\n",(0,i.jsx)(n.p,{children:"Set message as retained on the topic."}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"bool"}),(0,i.jsx)(n.br,{}),"\n","Default: ",(0,i.jsx)(n.code,{children:"false"})]}),"\n",(0,i.jsx)(n.h3,{id:"retained_interpolated",children:(0,i.jsx)(n.code,{children:"retained_interpolated"})}),"\n",(0,i.jsxs)(n.p,{children:["Override the value of ",(0,i.jsx)(n.code,{children:"retained"})," with an interpolable value, this allows it to be dynamically set based on message contents. The value must resolve to either ",(0,i.jsx)(n.code,{children:"true"})," or ",(0,i.jsx)(n.code,{children:"false"}),".\nThis field supports ",(0,i.jsx)(n.a,{href:"/docs/configuration/interpolation#bloblang-queries",children:"interpolation functions"}),"."]}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"string"}),(0,i.jsx)(n.br,{}),"\n","Requires version 3.59.0 or newer"]}),"\n",(0,i.jsx)(n.h3,{id:"max_in_flight",children:(0,i.jsx)(n.code,{children:"max_in_flight"})}),"\n",(0,i.jsx)(n.p,{children:"The maximum number of messages to have in flight at a given time. Increase this to improve throughput."}),"\n",(0,i.jsxs)(n.p,{children:["Type: ",(0,i.jsx)(n.code,{children:"int"}),(0,i.jsx)(n.br,{}),"\n","Default: ",(0,i.jsx)(n.code,{children:"64"})]})]})}function p(e={}){const{wrapper:n}={...(0,l.a)(),...e.components};return n?(0,i.jsx)(n,{...e,children:(0,i.jsx)(h,{...e})}):h(e)}},5162:function(e,n,t){t.d(n,{Z:function(){return r}});t(7294);var i=t(6010),l={tabItem:"tabItem_Ymn6"},s=t(5893);function r(e){let{children:n,hidden:t,className:r}=e;return(0,s.jsx)("div",{role:"tabpanel",className:(0,i.Z)(l.tabItem,r),hidden:t,children:n})}},4866:function(e,n,t){t.d(n,{Z:function(){return y}});var i=t(7294),l=t(6010),s=t(2466),r=t(6550),c=t(469),o=t(1980),d=t(7392),a=t(12);function u(e){var n,t;return null!=(n=null==(t=i.Children.toArray(e).filter((e=>"\n"!==e)).map((e=>{if(!e||(0,i.isValidElement)(e)&&function(e){const{props:n}=e;return!!n&&"object"==typeof n&&"value"in n}(e))return e;throw new Error("Docusaurus error: Bad <Tabs> child <"+("string"==typeof e.type?e.type:e.type.name)+'>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.')})))?void 0:t.filter(Boolean))?n:[]}function h(e){const{values:n,children:t}=e;return(0,i.useMemo)((()=>{const e=null!=n?n:function(e){return u(e).map((e=>{let{props:{value:n,label:t,attributes:i,default:l}}=e;return{value:n,label:t,attributes:i,default:l}}))}(t);return function(e){const n=(0,d.l)(e,((e,n)=>e.value===n.value));if(n.length>0)throw new Error('Docusaurus error: Duplicate values "'+n.map((e=>e.value)).join(", ")+'" found in <Tabs>. Every value needs to be unique.')}(e),e}),[n,t])}function p(e){let{value:n,tabValues:t}=e;return t.some((e=>e.value===n))}function x(e){let{queryString:n=!1,groupId:t}=e;const l=(0,r.k6)(),s=function(e){let{queryString:n=!1,groupId:t}=e;if("string"==typeof n)return n;if(!1===n)return null;if(!0===n&&!t)throw new Error('Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".');return null!=t?t:null}({queryString:n,groupId:t});return[(0,o._X)(s),(0,i.useCallback)((e=>{if(!s)return;const n=new URLSearchParams(l.location.search);n.set(s,e),l.replace({...l.location,search:n.toString()})}),[s,l])]}function f(e){const{defaultValue:n,queryString:t=!1,groupId:l}=e,s=h(e),[r,o]=(0,i.useState)((()=>function(e){var n;let{defaultValue:t,tabValues:i}=e;if(0===i.length)throw new Error("Docusaurus error: the <Tabs> component requires at least one <TabItem> children component");if(t){if(!p({value:t,tabValues:i}))throw new Error('Docusaurus error: The <Tabs> has a defaultValue "'+t+'" but none of its children has the corresponding value. Available values are: '+i.map((e=>e.value)).join(", ")+". If you intend to show no default tab, use defaultValue={null} instead.");return t}const l=null!=(n=i.find((e=>e.default)))?n:i[0];if(!l)throw new Error("Unexpected error: 0 tabValues");return l.value}({defaultValue:n,tabValues:s}))),[d,u]=x({queryString:t,groupId:l}),[f,j]=function(e){let{groupId:n}=e;const t=function(e){return e?"docusaurus.tab."+e:null}(n),[l,s]=(0,a.Nk)(t);return[l,(0,i.useCallback)((e=>{t&&s.set(e)}),[t,s])]}({groupId:l}),m=(()=>{const e=null!=d?d:f;return p({value:e,tabValues:s})?e:null})();(0,c.Z)((()=>{m&&o(m)}),[m]);return{selectedValue:r,selectValue:(0,i.useCallback)((e=>{if(!p({value:e,tabValues:s}))throw new Error("Can't select invalid tab value="+e);o(e),u(e),j(e)}),[u,j,s]),tabValues:s}}var j=t(2389),m={tabList:"tabList__CuJ",tabItem:"tabItem_LNqP"},b=t(5893);function v(e){let{className:n,block:t,selectedValue:i,selectValue:r,tabValues:c}=e;const o=[],{blockElementScrollPositionUntilNextRender:d}=(0,s.o5)(),a=e=>{const n=e.currentTarget,t=o.indexOf(n),l=c[t].value;l!==i&&(d(n),r(l))},u=e=>{var n;let t=null;switch(e.key){case"Enter":a(e);break;case"ArrowRight":{var i;const n=o.indexOf(e.currentTarget)+1;t=null!=(i=o[n])?i:o[0];break}case"ArrowLeft":{var l;const n=o.indexOf(e.currentTarget)-1;t=null!=(l=o[n])?l:o[o.length-1];break}}null==(n=t)||n.focus()};return(0,b.jsx)("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,l.Z)("tabs",{"tabs--block":t},n),children:c.map((e=>{let{value:n,label:t,attributes:s}=e;return(0,b.jsx)("li",{role:"tab",tabIndex:i===n?0:-1,"aria-selected":i===n,ref:e=>o.push(e),onKeyDown:u,onClick:a,...s,className:(0,l.Z)("tabs__item",m.tabItem,null==s?void 0:s.className,{"tabs__item--active":i===n}),children:null!=t?t:n},n)}))})}function g(e){let{lazy:n,children:t,selectedValue:l}=e;const s=(Array.isArray(t)?t:[t]).filter(Boolean);if(n){const e=s.find((e=>e.props.value===l));return e?(0,i.cloneElement)(e,{className:"margin-top--md"}):null}return(0,b.jsx)("div",{className:"margin-top--md",children:s.map(((e,n)=>(0,i.cloneElement)(e,{key:n,hidden:e.props.value!==l})))})}function _(e){const n=f(e);return(0,b.jsxs)("div",{className:(0,l.Z)("tabs-container",m.tabList),children:[(0,b.jsx)(v,{...e,...n}),(0,b.jsx)(g,{...e,...n})]})}function y(e){const n=(0,j.Z)();return(0,b.jsx)(_,{...e,children:u(e.children)},String(n))}},1151:function(e,n,t){t.d(n,{Z:function(){return c},a:function(){return r}});var i=t(7294);const l={},s=i.createContext(l);function r(e){const n=i.useContext(s);return i.useMemo((function(){return"function"==typeof e?e(n):{...n,...e}}),[n,e])}function c(e){let n;return n=e.disableParentContext?"function"==typeof e.components?e.components(l):e.components||l:r(e.components),i.createElement(s.Provider,{value:n},e.children)}}}]);