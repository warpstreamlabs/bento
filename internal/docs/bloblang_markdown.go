package docs

import (
	"bytes"
	"strings"
	"text/template"

	"github.com/warpstreamlabs/bento/internal/bloblang/query"
)

type functionCategory struct {
	Name  string
	Specs []query.FunctionSpec
}

type functionsContext struct {
	Categories []functionCategory
}

var bloblangParamsTemplate = `{{define "parameters" -}}
{{if gt (len .Definitions) 0}}
#### Parameters

{{range $i, $param := .Definitions -}}
` + "**`{{$param.Name}}`**" + ` &lt;{{if $param.IsOptional}}(optional) {{end}}{{$param.ValueType}}{{if $param.DefaultValue}}, default ` + "`{{$param.PrettyDefault}}`" + `{{end}}&gt; {{$param.Description}}  
{{end -}}
{{end -}}
{{end -}}
`

var bloblangFunctionsTemplate = bloblangParamsTemplate + `{{define "function_example" -}}
{{if gt (len .Summary) 0 -}}
{{.Summary}}

{{end -}}

` + "```coffee" + `
{{.Mapping}}
{{range $i, $result := .Results}}
# In:  {{index $result 0}}
# Out: {{index $result 1}}
{{end -}}
` + "```" + `
{{end -}}

{{define "function_spec" -}}
### ` + "`{{.Name}}`" + `

{{if eq .Status "beta" -}}
:::caution BETA
This function is mostly stable but breaking changes could still be made outside of major version releases if a fundamental problem with it is found.
:::
{{end -}}
{{if eq .Status "experimental" -}}
:::caution EXPERIMENTAL
This function is experimental and therefore breaking changes could be made to it outside of major version releases.
:::
{{end -}}
{{.Description}}{{if gt (len .Version) 0}}

Introduced in version {{.Version}}.
{{end}}
{{template "parameters" .Params -}}
{{if gt (len .Examples) 0}}
#### Examples

{{range $i, $example := .Examples}}
{{template "function_example" $example -}}
{{end -}}
{{end -}}

{{end -}}

---
title: Bloblang Functions
sidebar_label: Functions
description: A list of Bloblang functions
---

<!--
     THIS FILE IS AUTOGENERATED!

     To make changes please edit the contents of:
     internal/bloblang/query/functions.go
     internal/docs/bloblang.go
-->

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Functions can be placed anywhere and allow you to extract information from your environment, generate values, or access data from the underlying message being mapped:

` + "```coffee" + `
root.doc.id = uuid_v4()
root.doc.received_at = now()
root.doc.host = hostname()
` + "```" + `

Functions support both named and nameless style arguments:

` + "```coffee" + `
root.values_one = range(start: 0, stop: this.max, step: 2)
root.values_two = range(0, this.max, 2)
` + "```" + `

{{range $i, $cat := .Categories -}}
## {{$cat.Name}}

{{range $i, $spec := $cat.Specs -}}
{{template "function_spec" $spec}}
{{end -}}
{{end -}}

[error_handling]: /docs/configuration/error_handling
[field_paths]: /docs/configuration/field_paths
[meta_proc]: /docs/components/processors/metadata
[methods.encode]: /docs/guides/bloblang/methods#encode
[methods.string]: /docs/guides/bloblang/methods#string
`

func prefixExamples(s []query.ExampleSpec) {
	for _, spec := range s {
		for i := range spec.Results {
			spec.Results[i][0] = strings.ReplaceAll(
				strings.TrimSuffix(spec.Results[i][0], "\n"),
				"\n", "\n#      ",
			)
			spec.Results[i][1] = strings.ReplaceAll(
				strings.TrimSuffix(spec.Results[i][1], "\n"),
				"\n", "\n#      ",
			)
		}
	}
}

// BloblangFunctionsMarkdown returns a markdown document for all Bloblang
// functions.
func BloblangFunctionsMarkdown() ([]byte, error) {
	ctx := functionsContext{}

	specs := query.FunctionDocs()
	for _, s := range specs {
		prefixExamples(s.Examples)
	}

	for _, cat := range []string{
		query.FunctionCategoryGeneral,
		query.FunctionCategoryMessage,
		query.FunctionCategoryEnvironment,
		query.FunctionCategoryFakeData,
		query.FunctionCategoryDeprecated,
	} {
		functions := functionCategory{
			Name: cat,
		}
		for _, spec := range specs {
			if spec.Category == cat {
				functions.Specs = append(functions.Specs, spec)
			}
		}
		if len(functions.Specs) > 0 {
			ctx.Categories = append(ctx.Categories, functions)
		}
	}

	var buf bytes.Buffer
	err := template.Must(template.New("functions").Parse(bloblangFunctionsTemplate)).Execute(&buf, ctx)

	return buf.Bytes(), err
}

//------------------------------------------------------------------------------

type methodCategory struct {
	Name  string
	Specs []query.MethodSpec
}

type methodsContext struct {
	Categories []methodCategory
	General    []query.MethodSpec
}

var bloblangMethodsTemplate = bloblangParamsTemplate + `{{define "method_example" -}}
{{if gt (len .Summary) 0 -}}
{{.Summary}}

{{end -}}

` + "```coffee" + `
{{.Mapping}}
{{range $i, $result := .Results}}
# In:  {{index $result 0}}
# Out: {{index $result 1}}
{{end -}}
` + "```" + `
{{end -}}

{{define "method_spec" -}}
### ` + "`{{.Name}}`" + `

{{if eq .Status "beta" -}}
:::caution BETA
This method is mostly stable but breaking changes could still be made outside of major version releases if a fundamental problem with it is found.
:::
{{end -}}
{{if eq .Status "experimental" -}}
:::caution EXPERIMENTAL
This method is experimental and therefore breaking changes could be made to it outside of major version releases.
:::
{{end -}}
{{.Description}}{{if gt (len .Version) 0}}

Introduced in version {{.Version}}.
{{end}}
{{template "parameters" .Params -}}
{{if gt (len .Examples) 0}}
#### Examples

{{range $i, $example := .Examples}}
{{template "method_example" $example -}}
{{end -}}
{{end -}}

{{end -}}

---
title: Bloblang Methods
sidebar_label: Methods
description: A list of Bloblang methods
---

<!--
     THIS FILE IS AUTOGENERATED!

     To make changes please edit the contents of:
     internal/bloblang/query/methods.go
     internal/bloblang/query/methods_strings.go
     internal/docs/bloblang.go
-->

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Methods provide most of the power in Bloblang as they allow you to augment values and can be added to any expression (including other methods):

` + "```coffee" + `
root.doc.id = this.thing.id.string().catch(uuid_v4())
root.doc.reduced_nums = this.thing.nums.map_each(num -> if num < 10 {
  deleted()
} else {
  num - 10
})
root.has_good_taste = ["pikachu","mewtwo","magmar"].contains(this.user.fav_pokemon)
` + "```" + `

Methods support both named and nameless style arguments:

` + "```coffee" + `
root.foo_one = this.(bar | baz).trim().replace_all(old: "dog", new: "cat")
root.foo_two = this.(bar | baz).trim().replace_all("dog", "cat")
` + "```" + `

{{if gt (len .General) 0 -}}
## General

{{range $i, $spec := .General -}}
{{template "method_spec" $spec}}
{{end -}}
{{end -}}

{{range $i, $cat := .Categories -}}
## {{$cat.Name}}

{{range $i, $spec := $cat.Specs -}}
{{template "method_spec" $spec}}
{{end -}}
{{end -}}

[field_paths]: /docs/configuration/field_paths
[methods.encode]: #encode
[methods.string]: #string
`

func methodForCat(s query.MethodSpec, cat string) (query.MethodSpec, bool) {
	for _, c := range s.Categories {
		if c.Category == cat {
			spec := s
			if c.Description != "" {
				spec.Description = strings.TrimSpace(c.Description)
			}
			if len(c.Examples) > 0 {
				spec.Examples = c.Examples
			}
			return spec, true
		}
	}
	return s, false
}

// BloblangMethodsMarkdown returns a markdown document for all Bloblang methods.
func BloblangMethodsMarkdown() ([]byte, error) {
	ctx := methodsContext{}

	specs := query.MethodDocs()
	for _, s := range specs {
		prefixExamples(s.Examples)
		for _, cat := range s.Categories {
			prefixExamples(cat.Examples)
		}
	}

	for _, cat := range []string{
		query.MethodCategoryStrings,
		query.MethodCategoryRegexp,
		query.MethodCategoryNumbers,
		query.MethodCategoryTime,
		query.MethodCategoryCoercion,
		query.MethodCategoryObjectAndArray,
		query.MethodCategoryParsing,
		query.MethodCategoryEncoding,
		query.MethodCategoryJWT,
		query.MethodCategoryGeoIP,
		query.MethodCategoryDeprecated,
	} {
		methods := methodCategory{
			Name: cat,
		}
		for _, spec := range specs {
			var ok bool
			if spec, ok = methodForCat(spec, cat); ok {
				methods.Specs = append(methods.Specs, spec)
			}
		}
		if len(methods.Specs) > 0 {
			ctx.Categories = append(ctx.Categories, methods)
		}
	}

	for _, spec := range specs {
		if len(spec.Categories) == 0 && spec.Status != query.StatusHidden {
			spec.Description = strings.TrimSpace(spec.Description)
			ctx.General = append(ctx.General, spec)
		}
	}

	var buf bytes.Buffer
	err := template.Must(template.New("methods").Parse(bloblangMethodsTemplate)).Execute(&buf, ctx)

	return buf.Bytes(), err
}
