package pure

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	"github.com/warpstreamlabs/bento/public/bloblang"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	tpFieldText               = "text"
	tpFieldAsFile             = "as_file"
	tpFieldFunctions          = "functions"
	tpFieldSubTemplates       = "sub_templates"
	tpFieldSubTemplatesText   = "text"
	tpFieldSubTemplatesAsFile = "as_file"
)

func TemplateProcessorSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Mapping").
		Summary("Transforms messages using Go template syntax.").
		Description(`Transforms messages using Go template syntax.
Supports built-in Go template functions,
Sprig template functions (including string manipulation, math, date, and encoding utilities)
and a custom `+"`"+`meta`+"`"+` function to access message metadata (e.g., `+"`"+`{{ meta \"key\" }}`+"`"+`).
Additionally, users can define custom Bloblang-based functions via the `+"`"+`functions`+"`"+` field, which are available during template execution.`).
		Fields(
			service.NewStringField(tpFieldText).
				Description("The Go template to apply to messages, if `as_file` is enabled, it will be used as file path and the template gets readed from file.").
				Example("{{ .name }} - {{ meta \"source\" }}").
				Example("{{ range .items }}{{ .name }}: {{ .value }}{{ end }}").
				Default(""),
			service.NewBoolField(tpFieldAsFile).
				Description("Read the tempalte as a file.").
				Default(false),
			service.NewStringMapField(tpFieldFunctions).
				Description("A map of Bloblang functions to make available to the template.").
				Optional(),
			service.NewObjectMapField(tpFieldSubTemplates,
				service.NewStringField(tpFieldSubTemplatesText).
					Description("The Go template, if `as_file` is enabled, it will be used as file path and the template gets readed from file."),
				service.NewBoolField(tpFieldSubTemplatesAsFile).
					Description("Read the tempalte as a file.").
					Default(false),
			).
				Description("A map of other templates which will defined into the `main` template.").
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

var templatePredefinedFunctions = []string{
	// https://pkg.go.dev/text/template#hdr-Functions

	// global functions
	"and",
	"call",
	"html",
	"index",
	"slice",
	"js",
	"len",
	"not",
	"or",
	"print",
	"printf",
	"println",
	"urlquery",

	// binary comparison operators
	"eq",
	"ne",
	"lt",
	"le",
	"gt",
	"ge",

	// https://pkg.go.dev/text/template#hdr-Actions
	"-",
	"pipeline",
	"if",
	"end",
	"else",
	"range",
	"break",
	"continue",
	"template",
	"block",
	"define",
	"with",
}

func NewTemplateProcessorFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	templateStr, err := conf.FieldString(tpFieldText)
	if err != nil {
		return nil, err
	}

	var functions template.FuncMap
	sprigFunctions := sprig.TxtFuncMap()

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
			if slices.Contains(templatePredefinedFunctions, name) {
				return nil, fmt.Errorf("can not redefine a predefined function or named %s", name)
			}
			_, sprigContains := sprigFunctions[name]
			if sprigContains {
				return nil, fmt.Errorf("can not redefine a sprig function named %s", name)
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

	templateAsFile, _ := conf.FieldBool(tpFieldText)

	tmpl := template.New("main").Funcs(functions).Funcs(sprigFunctions)

	if templateAsFile {
		file, err := mgr.FS().Open(templateStr)
		if err != nil {
			return nil, fmt.Errorf("Failed to open template from file path %s: %w", templateStr, err)
		}
		fileData, err := io.ReadAll(file)
		if err != nil {
			return nil, fmt.Errorf("Failed to read template from file path %s: %w", templateStr, err)
		}
		file.Close()
		tmpl, err = tmpl.Parse(string(fileData))
		if err != nil {
			return nil, fmt.Errorf("Failed to parse template from file path %s: %w", templateStr, err)
		}
	} else {
		tmpl, err = tmpl.Parse(templateStr)
		if err != nil {
			return nil, fmt.Errorf("Failed to parse template: %w", err)
		}
	}

	if conf.Contains(tpFieldSubTemplates) {
		subTemplates, err := conf.FieldObjectMap(tpFieldSubTemplates)
		if err != nil {
			return nil, err
		}

		for name, subTemplate := range subTemplates {
			subTemplateStr, err := subTemplate.FieldString(tpFieldSubTemplatesText)
			if err != nil {
				return nil, err
			}

			subTemplateAsFile, _ := conf.FieldBool(tpFieldSubTemplatesAsFile)

			if subTemplateAsFile {
				file, err := mgr.FS().Open(subTemplateStr)
				if err != nil {
					return nil, fmt.Errorf("Failed to open sub template %s from file path %s: %w", name, subTemplateStr, err)
				}
				fileData, err := io.ReadAll(file)
				if err != nil {
					return nil, fmt.Errorf("Failed to read sub template %s from file path %s: %w", name, subTemplateStr, err)
				}
				file.Close()
				_, err = tmpl.New(name).Parse(string(fileData))
				if err != nil {
					return nil, fmt.Errorf("Failed to parse sub template %s from file path %s: %w", name, subTemplateStr, err)
				}
			} else {
				_, err = tmpl.New(name).Parse(subTemplateStr)
				if err != nil {
					return nil, fmt.Errorf("Failed to parse sub template %s: %w", name, err)
				}
			}

		}
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
