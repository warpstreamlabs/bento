package pure

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	"github.com/warpstreamlabs/bento/public/bloblang"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	tpFieldText      = "text"
	tpFieldFunctions = "functions"
)

func TemplateProcessorSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Mapping").
		Beta().
		Summary("Transforms messages using Go template syntax.").
		Description(`Transforms messages using Go template syntax.
Supports built-in Go template functions,
Sprig template functions (including string manipulation, math, date, and encoding utilities)
and a custom `+"`"+`meta`+"`"+` function to access message metadata (e.g., `+"`"+`{{ meta \"key\" }}`+"`"+`).
Additionally, users can define custom Bloblang-based functions via the `+"`"+`functions`+"`"+` field, which are available during template execution.`).
		Fields(
			service.NewStringField(tpFieldText).
				Description("The Go template to apply to messages.").
				Example("{{ .name }} - {{ meta \"source\" }}").
				Example("{{ range .items }}{{ .name }}: {{ .value }}{{ end }}").
				Default("{{ . }}"),
			service.NewStringMapField(tpFieldFunctions).
				Description("A map of Bloblang functions to make available to the template.").
				Optional(),
		)
}

func init() {
	err := service.RegisterProcessor("template", TemplateProcessorSpec(), NewTemplateProcessorFromConfig)
	if err != nil {
		panic(err)
	}
}

type templateProc struct {
	tmpl *template.Template
}

func NewTemplateProcessorFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	templateStr, err := conf.FieldString(tpFieldText)
	if err != nil {
		return nil, err
	}

	var functions template.FuncMap

	if conf.Contains(tpFieldFunctions) {
		functionsMap, err := conf.FieldStringMap(tpFieldFunctions)
		if err != nil {
			return nil, err
		}

		functions = make(template.FuncMap, len(functionsMap)+1)
		for name, fn := range functionsMap {
			blob, err := bloblang.Parse(fn)
			if err != nil {
				return nil, err
			}
			if name == "meta" {
				return nil, errors.New("can not define a function named meta")
			}
			functions[name] = func(msg any) (any, error) {
				return blob.Query(msg)
			}
		}
		functions["meta"] = func(name string) any {
			return nil
		}
	} else {
		functions = template.FuncMap{
			"meta": func(name string) any {
				return nil
			},
		}
	}

	tmpl, err := template.New("template").Funcs(functions).Funcs(sprig.TxtFuncMap()).Parse(templateStr)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse template: %w", err)
	}

	return &templateProc{
		tmpl: tmpl,
	}, nil
}

func (t *templateProc) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	obj, err := msg.AsStructured()
	if err != nil {
		return nil, fmt.Errorf("Failed to parse part into json: %v", err)
	}

	var buf bytes.Buffer
	if err := t.tmpl.Funcs(template.FuncMap{
		"meta": func(name string) any {
			if val, exists := msg.MetaGetMut(name); exists {
				return val
			}
			return nil
		},
	}).Execute(&buf, obj); err != nil {
		return nil, fmt.Errorf("Failed to execute template: %v", err)
	}

	msg.SetBytes(buf.Bytes())
	return service.MessageBatch{msg}, nil
}

func (t *templateProc) Close(context.Context) error {
	return nil
}
