package bloblang

import (
	"encoding/json"

	"github.com/Jeffail/gabs/v2"

	"github.com/warpstreamlabs/bento/internal/bloblang/query"
)

// FunctionView describes a particular function belonging to a Bloblang
// environment.
type FunctionView struct {
	spec query.FunctionSpec
}

// Description provides an overview of the function.
func (v *FunctionView) Description() string {
	return v.spec.Description
}

// FormatJSON returns a byte slice of the function configuration formatted as a
// JSON object. The schema of this method is undocumented and is not intended
// for general use.
//
// Experimental: This method is not intended for general use and could have its
// signature and/or behaviour changed outside of major version bumps.
func (v *FunctionView) FormatJSON() ([]byte, error) {
	return json.Marshal(v.spec)
}

// TemplateData returns an exported struct containing information ready to
// inject into a template for generating documentation.
func (v *FunctionView) TemplateData() TemplateFunctionData {
	return TemplateFunctionData{
		Status:      string(v.spec.Status),
		Name:        v.spec.Name,
		Category:    v.spec.Category,
		Description: v.spec.Description,
		Params:      templateParams(v.spec.Params),
		Examples:    templateExamples(v.spec.Examples),
		Version:     v.spec.Version,
	}
}

// MethodView describes a particular method belonging to a Bloblang environment.
type MethodView struct {
	spec query.MethodSpec
}

// Description provides an overview of the method.
func (v *MethodView) Description() string {
	return v.spec.Description
}

// FormatJSON returns a byte slice of the method configuration formatted as a
// JSON object. The schema of this method is undocumented and is not intended
// for general use.
//
// Experimental: This method is not intended for general use and could have its
// signature and/or behaviour changed outside of major version bumps.
func (v *MethodView) FormatJSON() ([]byte, error) {
	return json.Marshal(v.spec)
}

// TemplateData returns an exported struct containing information ready to
// inject into a template for generating documentation.
func (v *MethodView) TemplateData() TemplateMethodData {
	return TemplateMethodData{
		Status:      string(v.spec.Status),
		Name:        v.spec.Name,
		Description: v.spec.Description,
		Params:      templateParams(v.spec.Params),
		Examples:    templateExamples(v.spec.Examples),
		Categories:  templateCategories(v.spec.Categories),
		Version:     v.spec.Version,
	}
}

//------------------------------------------------------------------------------

func templateParams(p query.Params) TemplateParamsData {
	var tDefs []TemplateParamData
	for _, d := range p.Definitions {
		var jDefault string
		if d.DefaultValue != nil {
			jDefault = gabs.Wrap(*d.DefaultValue).String()
		}
		tDefs = append(tDefs, TemplateParamData{
			Name:              d.Name,
			Description:       d.Description,
			ValueType:         string(d.ValueType),
			IsOptional:        d.IsOptional,
			DefaultMarshalled: jDefault,
		})
	}
	return TemplateParamsData{
		Variadic:    p.Variadic,
		Definitions: tDefs,
	}
}

// TemplateParamData describes a single parameter defined for a bloblang plugin.
type TemplateParamData struct {
	Name        string
	Description string
	ValueType   string

	// IsOptional is implicit when there's a DefaultMarshalled. However, there
	// are times when a parameter is used to change behaviour without having a
	// default.
	IsOptional        bool
	DefaultMarshalled string
}

// TemplateParamsData describes the overall set of parameters for a bloblang
// plugin.
type TemplateParamsData struct {
	Variadic    bool
	Definitions []TemplateParamData
}

func templateExamples(esa []query.ExampleSpec) (eda []TemplateExampleData) {
	for _, es := range esa {
		eda = append(eda, TemplateExampleData{
			Mapping:     es.Mapping,
			Summary:     es.Summary,
			Results:     es.Results,
			SkipTesting: es.SkipTesting,
		})
	}
	return
}

// TemplateExampleData describes a single example for a given bloblang plugin.
type TemplateExampleData struct {
	Mapping string
	Summary string
	Results [][2]string

	// True if this example will not function as shown when tested
	SkipTesting bool
}

func templateCategories(csa []query.MethodCatSpec) (cda []TemplateMethodCategoryData) {
	for _, es := range csa {
		cda = append(cda, TemplateMethodCategoryData{
			Category:    es.Category,
			Description: es.Description,
			Examples:    templateExamples(es.Examples),
		})
	}
	return
}

// TemplateMethodCategoryData describes a behaviour, along with examples, of a
// method plugin for a given category. Separating documentation for a method
// into categories is sometimes appropriate in cases where the method behaves
// differently based on the target value.
type TemplateMethodCategoryData struct {
	Category    string
	Description string
	Examples    []TemplateExampleData
}

// TemplateMethodData describes a bloblang method.
type TemplateMethodData struct {
	// The release status of the function.
	Status string

	// Name of the method (as it appears in config).
	Name string

	// Description of the method purpose (in markdown).
	Description string

	// Params defines the expected arguments of the method.
	Params TemplateParamsData

	// Examples shows general usage for the method.
	Examples []TemplateExampleData

	// Categories further describe the method for a given category.
	Categories []TemplateMethodCategoryData

	// Version is the Bento version this component was introduced.
	Version string
}

// TemplateFunctionData describes a bloblang function.
type TemplateFunctionData struct {
	// The release status of the function.
	Status string

	// Name of the function (as it appears in config).
	Name string

	// Category is a rough category for the function.
	Category string

	// Description of the functions purpose (in markdown).
	Description string

	// Params defines the expected arguments of the function.
	Params TemplateParamsData

	// Examples shows general usage for the function.
	Examples []TemplateExampleData

	// Version is the Bento version this component was introduced.
	Version string
}
