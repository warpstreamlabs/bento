package kubernetes

import (
	"strings"

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
		service.NewStringMapField("label_selector").
			Description("Kubernetes label selector to filter resources.").
			Default(map[string]any{}).
			Example(map[string]any{"app": "myapp"}).
			Example(map[string]any{"app": "myapp", "env": "prod"}),
		service.NewStringMapField("field_selector").
			Description("Kubernetes field selector to filter resources.").
			Default(map[string]any{}).
			Example(map[string]any{"status.phase": "Running"}).
			Example(map[string]any{"metadata.name": "my-pod"}).
			Optional().
			Advanced(),
		service.NewStringField("request_timeout").
			Description("Timeout for Kubernetes API requests such as list calls. Use \"0s\" to disable.").
			Default("30s").
			Advanced(),
	}
}

// LabelSelectorFromMap converts a map of key-value pairs to a Kubernetes
// label selector string (e.g., "app=myapp,env=prod").
func LabelSelectorFromMap(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	pairs := make([]string, 0, len(labels))
	for k, v := range labels {
		pairs = append(pairs, k+"="+v)
	}
	return strings.Join(pairs, ",")
}

// metadataDescription returns the standard metadata documentation block.
func metadataDescription(fields ...string) string {
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
