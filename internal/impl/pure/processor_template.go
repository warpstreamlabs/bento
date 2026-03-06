package pure

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"text/template"

	"github.com/warpstreamlabs/bento/public/bloblang"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	tpFieldText      = "text"
	tpFieldFunctions = "functions"
)

func templateProcSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Mapping").
		Beta().
		Summary("Transforms messages using Go template syntax.").
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
	err := service.RegisterProcessor(
		"template", templateProcSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newTemplateProcessor(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

type templateProc struct {
	tmpl *template.Template
	log  *service.Logger
}

func newTemplateProcessor(conf *service.ParsedConfig, mgr *service.Resources) (*templateProc, error) {
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

	tmpl, err := template.New("template").Funcs(functions).Parse(templateStr)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse template: %w", err)
	}

	return &templateProc{
		tmpl: tmpl,
		log:  mgr.Logger(),
	}, nil
}

func (t *templateProc) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	obj, err := msg.AsStructured()
	if err != nil {
		t.log.Errorf("Failed to parse part into json: %v\n", err)
		return nil, err
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
		t.log.Errorf("Failed to execute template: %v", err)
		return nil, err
	}

	msg.SetBytes(buf.Bytes())
	return service.MessageBatch{msg}, nil
}

func (t *templateProc) Close(context.Context) error {
	return nil
}
