package plugin

import (
	"bytes"
	"text/template"

	_ "embed"

	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/plugin/runtime"
)

//go:embed docs_reference.md
var pluginDocsReference string

type pluginContext struct {
	Fields []docs.FieldSpecCtx
}

// DocsFieldsMarkdown returns the manifest fields reference documentation.
func DocsFieldsMarkdown() ([]byte, error) {
	pluginDocsTemplate := docs.FieldsTemplate(false) + pluginDocsReference

	var buf bytes.Buffer
	err := template.Must(template.New("plugin").Parse(pluginDocsTemplate)).Execute(&buf, pluginContext{
		Fields: docs.FieldObject("", "").WithChildren(runtime.ManifestConfigSpec()...).FlattenChildrenForDocs(),
	})
	return buf.Bytes(), err
}
