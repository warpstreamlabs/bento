package editor

import (
	"fmt"
	"html"
	"html/template"
	"regexp"
	"strings"

	"github.com/warpstreamlabs/bento/internal/bloblang/query"
)

const docsBaseURL = "https://warpstreamlabs.github.io/bento/docs/guides/bloblang"

type specDoc struct {
	Name        string
	IsMethod    bool
	Status      query.Status
	Version     string
	Description string
	Params      query.Params
}

func functionDoc(name string, spec query.FunctionSpec) specDoc {
	return specDoc{
		Name:        name,
		Status:      spec.Status,
		Version:     spec.Version,
		Description: spec.Description,
		Params:      spec.Params,
	}
}

func methodDoc(name string, spec query.MethodSpec) specDoc {
	return specDoc{
		Name:        name,
		IsMethod:    true,
		Status:      spec.Status,
		Version:     spec.Version,
		Description: spec.Description,
		Params:      spec.Params,
	}
}

// renderer renders docs as the HTML shown in editor tooltips.
type renderer struct {
	docsURL string
}

func newRenderer(docsURL string) *renderer {
	return &renderer{docsURL: strings.TrimRight(docsURL, "/")}
}

func (r *renderer) Spec(spec specDoc) string {
	page := r.docsURL + "/functions"
	if spec.IsMethod {
		page = r.docsURL + "/methods"
	}
	return render("spec", struct {
		specDoc
		DocsURL string
	}{spec, page + "#" + spec.Name})
}

func (r *renderer) Keyword(name, description string) string {
	return render("keyword", struct{ Name, Description string }{name, description})
}

var docTemplates = template.Must(template.New("docs").Funcs(template.FuncMap{
	"markdown": markdownToHTML,
}).Parse(`
{{- define "keyword" -}}
<div class="ace-doc">
	<div class="ace-doc-header">
		<div class="ace-doc-signature">
			<strong>{{.Name}}</strong>
		</div>
	</div>
	<div class="ace-doc-description">{{.Description}}</div>
</div>
{{- end -}}

{{- define "signature" -}}
{{if .IsMethod}}.{{end}}{{.Name}}(
	{{- range $i, $p := .Params.Definitions}}{{if $i}}, {{end}}{{$p.Name}}: {{$p.ValueType}}{{end}}
	{{- if .Params.Variadic}}{{if .Params.Definitions}}, {{end}}...{{end -}}
)
{{- end -}}

{{- define "spec" -}}
<div class="ace-doc" data-docs-url="{{.DocsURL}}" data-function-name="{{.Name}}">
	<div class="ace-doc-header clickable-header" title="Click to view documentation">
		<div class="ace-doc-signature">
			<strong>{{template "signature" .}}</strong>
			<span class="ace-status-{{.Status}}">{{.Status}}</span>
		</div>
	</div>
	{{- with .Version}}
	<div class="ace-doc-version">Since: v{{.}}</div>
	{{- end}}
	{{- with .Description}}
	<div class="ace-doc-description">{{markdown .}}</div>
	{{- end}}
	{{- with .Params.Definitions}}
	<div class="ace-doc-parameters">
		<strong>Parameters:</strong>
		{{- range .}}
		<div class="ace-doc-param">
			<code>{{.Name}} [{{.ValueType}}]</code><br/>
			{{- with .Description}}
			<span class="ace-doc-param-desc">{{markdown .}}</span>
			{{- end}}
		</div>
		{{- end}}
	</div>
	{{- end}}
</div>
{{- end -}}
`))

// The templates are static and their data is typed, so a failure here is a
// programming error.
func render(name string, data any) string {
	var b strings.Builder
	if err := docTemplates.ExecuteTemplate(&b, name, data); err != nil {
		panic(fmt.Sprintf("failed to render %q doc template: %v", name, err))
	}
	return b.String()
}

var (
	admonitionRegex = regexp.MustCompile(`:::([a-zA-Z]+)[\s\S]*?:::`)
	inlineCodeRegex = regexp.MustCompile("`([^`]+)`")
	linkRegex       = regexp.MustCompile(`\[([^\]]+)\](?:\([^)]*\)|\[[^\]]*\])`) // [text](href) or [text][ref]
)

// Links are reduced to their text, as most are relative to the docs site or
// name reference definitions that only it has. The header links to the docs.
func markdownToHTML(markdown string) template.HTML {
	// Escape, so that only the tags added below are HTML.
	s := admonitionRegex.ReplaceAllString(markdown, "")
	s = html.EscapeString(strings.TrimSpace(s))
	s = linkRegex.ReplaceAllString(s, "$1")
	s = inlineCodeRegex.ReplaceAllString(s, "<code>$1</code>")
	return template.HTML(s)
}
