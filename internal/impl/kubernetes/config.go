package kubernetes

import (
	"github.com/warpstreamlabs/bento/public/service"
)

// CommonFields returns config fields shared across all Kubernetes inputs.
func CommonFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringListField("namespaces").
			Description("Namespaces to watch. Empty list means all namespaces.").
			Default([]any{}).
			Example([]string{"default"}).
			Example([]string{"production", "staging"}),
		service.NewStringField("label_selector").
			Description("Kubernetes label selector to filter resources (e.g., 'app=myapp,env=prod').").
			Default("").
			Example("app=myapp").
			Example("app in (frontend,backend)"),
		service.NewStringField("field_selector").
			Description("Kubernetes field selector to filter resources (e.g., 'status.phase=Running').").
			Default("").
			Optional().
			Advanced(),
		service.NewStringField("request_timeout").
			Description("Timeout for Kubernetes API requests such as list calls. Use \"0s\" to disable.").
			Default("30s").
			Advanced(),
	}
}

// MetadataDescription returns the standard metadata documentation block.
func MetadataDescription(fields []string) string {
	result := `

### Metadata

This input adds the following metadata fields to each message:

` + "```text" + `
`
	for _, f := range fields {
		result += "- " + f + "\n"
	}
	result += "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#bloblang-queries).
`
	return result
}
