package plugin

import (
	"bytes"
	"text/template"

	_ "embed"

	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/plugin/runtime"
)

//go:embed docs.md
var pluginDocs string

type pluginContext struct {
	Fields []docs.FieldSpecCtx
}

// DocsMarkdown returns a markdown document for the runtime plugin documentation.
func DocsMarkdown() ([]byte, error) {
	pluginDocsTemplate := docs.FieldsTemplate(false) + pluginDocs

	var buf bytes.Buffer
	err := template.Must(template.New("plugin").Parse(pluginDocsTemplate)).Execute(&buf, pluginContext{
		Fields: docs.FieldObject("", "").WithChildren(runtime.ManifestConfigSpec()...).FlattenChildrenForDocs(),
	})
	return buf.Bytes(), err
}
