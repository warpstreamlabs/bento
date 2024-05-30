"use strict";(self.webpackChunkbento=self.webpackChunkbento||[]).push([[8140],{419:function(t,e,n){n.r(e),n.d(e,{assets:function(){return d},contentTitle:function(){return a},default:function(){return r},frontMatter:function(){return o},metadata:function(){return u},toc:function(){return c}});var i=n(5893),s=n(1151);const o={title:"Dynamic Inputs and Outputs"},a=void 0,u={id:"configuration/dynamic_inputs_and_outputs",title:"Dynamic Inputs and Outputs",description:"It is possible to have sets of inputs and outputs in Bento that can be added,",source:"@site/docs/configuration/dynamic_inputs_and_outputs.md",sourceDirName:"configuration",slug:"/configuration/dynamic_inputs_and_outputs",permalink:"/bento/docs/configuration/dynamic_inputs_and_outputs",draft:!1,unlisted:!1,editUrl:"https://github.com/warpstreamlabs/bento/edit/main/website/docs/configuration/dynamic_inputs_and_outputs.md",tags:[],version:"current",frontMatter:{title:"Dynamic Inputs and Outputs"},sidebar:"docs",previous:{title:"Templating",permalink:"/bento/docs/configuration/templating"},next:{title:"Using CUE",permalink:"/bento/docs/configuration/using_cue"}},d={},c=[{value:"API",id:"api",level:2},{value:"<code>/inputs</code>",id:"inputs",level:3},{value:"<code>/inputs/{input_label}</code>",id:"inputsinput_label",level:3},{value:"<code>/outputs</code>",id:"outputs",level:3},{value:"<code>/outputs/{output_label}</code>",id:"outputsoutput_label",level:3},{value:"Applications",id:"applications",level:2}];function l(t){const e={a:"a",code:"code",h2:"h2",h3:"h3",p:"p",pre:"pre",...(0,s.a)(),...t.components};return(0,i.jsxs)(i.Fragment,{children:[(0,i.jsxs)(e.p,{children:["It is possible to have sets of inputs and outputs in Bento that can be added,\nupdated and removed during runtime with the ",(0,i.jsx)(e.a,{href:"/docs/components/inputs/dynamic",children:"dynamic fan in"})," and\n",(0,i.jsx)(e.a,{href:"/docs/components/outputs/dynamic",children:"dynamic fan out"})," types."]}),"\n",(0,i.jsx)(e.p,{children:"Dynamic inputs and outputs are each identified by unique string labels, which\nare specified when adding them either in configuration or via the HTTP API. The\nlabels are useful when querying which types are active."}),"\n",(0,i.jsx)(e.h2,{id:"api",children:"API"}),"\n",(0,i.jsx)(e.p,{children:"The API for dynamic types (both inputs and outputs) is a collection of HTTP REST\nendpoints:"}),"\n",(0,i.jsx)(e.h3,{id:"inputs",children:(0,i.jsx)(e.code,{children:"/inputs"})}),"\n",(0,i.jsxs)(e.p,{children:["Returns a JSON object that maps input labels to an object containing details\nabout the input, including uptime and configuration. If the input has terminated\nnaturally the uptime will be set to ",(0,i.jsx)(e.code,{children:"stopped"}),"."]}),"\n",(0,i.jsx)(e.pre,{children:(0,i.jsx)(e.code,{className:"language-json",children:'{\n  "<string, input_label>": {\n    "uptime": "<string>",\n    "config": <object>\n  },\n  ...\n}\n'})}),"\n",(0,i.jsx)(e.h3,{id:"inputsinput_label",children:(0,i.jsx)(e.code,{children:"/inputs/{input_label}"})}),"\n",(0,i.jsxs)(e.p,{children:["GET returns the configuration of the input idenfified by ",(0,i.jsx)(e.code,{children:"input_label"}),"."]}),"\n",(0,i.jsxs)(e.p,{children:["POST sets the input ",(0,i.jsx)(e.code,{children:"input_label"})," to the body of the request parsed as a JSON\nconfiguration. If the input label already exists the previous input is first\nstopped and removed."]}),"\n",(0,i.jsxs)(e.p,{children:["DELETE stops and removes the input identified by ",(0,i.jsx)(e.code,{children:"input_label"}),"."]}),"\n",(0,i.jsx)(e.h3,{id:"outputs",children:(0,i.jsx)(e.code,{children:"/outputs"})}),"\n",(0,i.jsxs)(e.p,{children:["Returns a JSON object that maps output labels to an object containing details\nabout the output, including uptime and configuration. If the output has\nterminated naturally the uptime will be set to ",(0,i.jsx)(e.code,{children:"stopped"}),"."]}),"\n",(0,i.jsx)(e.pre,{children:(0,i.jsx)(e.code,{className:"language-json",children:'{\n  "<string, output_label>": {\n    "uptime": "<string>",\n    "config": <object>\n  },\n  ...\n}\n'})}),"\n",(0,i.jsx)(e.h3,{id:"outputsoutput_label",children:(0,i.jsx)(e.code,{children:"/outputs/{output_label}"})}),"\n",(0,i.jsxs)(e.p,{children:["GET returns the configuration of the output idenfified by ",(0,i.jsx)(e.code,{children:"output_label"}),"."]}),"\n",(0,i.jsxs)(e.p,{children:["POST sets the output ",(0,i.jsx)(e.code,{children:"output_label"})," to the body of the request parsed as a JSON\nconfiguration. If the output label already exists the previous output is first\nstopped and removed."]}),"\n",(0,i.jsxs)(e.p,{children:["DELETE stops and removes the output identified by ",(0,i.jsx)(e.code,{children:"output_label"}),"."]}),"\n",(0,i.jsx)(e.p,{children:"A custom prefix can be set for these endpoints in configuration."}),"\n",(0,i.jsx)(e.h2,{id:"applications",children:"Applications"}),"\n",(0,i.jsx)(e.p,{children:"Dynamic types are useful when a platforms data streams might need to change\nregularly and automatically. It is also useful for triggering batches of\nplatform data, e.g. a cron job can be created to send hourly curl requests that\nadds a dynamic input to read a file of sample data:"}),"\n",(0,i.jsx)(e.pre,{children:(0,i.jsx)(e.code,{className:"language-sh",children:'curl http://localhost:4195/inputs/read_sample -d @- << EOF\n{\n\t"file": {\n\t\t"path": "/tmp/line_delim_sample_data.txt"\n\t}\n}\nEOF\n'})}),"\n",(0,i.jsxs)(e.p,{children:["Some inputs have a finite lifetime, e.g. ",(0,i.jsx)(e.code,{children:"s3"})," without an SQS queue configured\nwill close once the whole bucket has been read. When a dynamic types lifetime\nends the ",(0,i.jsx)(e.code,{children:"uptime"})," field of an input listing will be set to ",(0,i.jsx)(e.code,{children:"stopped"}),". You can\nuse this to write tools that trigger new inputs (to move onto the next bucket,\nfor example)."]})]})}function r(t={}){const{wrapper:e}={...(0,s.a)(),...t.components};return e?(0,i.jsx)(e,{...t,children:(0,i.jsx)(l,{...t})}):l(t)}},1151:function(t,e,n){n.d(e,{Z:function(){return u},a:function(){return a}});var i=n(7294);const s={},o=i.createContext(s);function a(t){const e=i.useContext(o);return i.useMemo((function(){return"function"==typeof t?t(e):{...e,...t}}),[e,t])}function u(t){let e;return e=t.disableParentContext?"function"==typeof t.components?t.components(s):t.components||s:a(t.components),i.createElement(o.Provider,{value:e},t.children)}}}]);