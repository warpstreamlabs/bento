package cache

import (
	"fmt"

	"gopkg.in/yaml.v3"

	"github.com/warpstreamlabs/bento/internal/docs"
)

// Config is the all encompassing configuration struct for all cache types.
// Deprecated: Do not add new components here. Instead, use the public plugin
// APIs. Examples can be found in: ./internal/impl.
type Config struct {
	Label  string `json:"label" yaml:"label"`
	Type   string `json:"type" yaml:"type"`
	Plugin any    `json:"plugin,omitempty" yaml:"plugin,omitempty"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Label:  "",
		Type:   "memory",
		Plugin: nil,
	}
}

func FromAny(prov docs.Provider, value any) (conf Config, err error) {
	switch t := value.(type) {
	case Config:
		return t, nil
	case *yaml.Node:
		return fromYAML(prov, t)
	case map[string]any:
		return fromMap(prov, t)
	}
	err = fmt.Errorf("unexpected value, expected object, got %T", value)
	return
}

func fromMap(prov docs.Provider, value map[string]any) (conf Config, err error) {
	if conf.Type, _, err = docs.GetInferenceCandidateFromMap(prov, docs.TypeCache, value); err != nil {
		err = docs.NewLintError(0, docs.LintComponentNotFound, err)
		return
	}

	conf.Label, _ = value["label"].(string)

	if p, exists := value[conf.Type]; exists {
		conf.Plugin = p
	} else if p, exists := value["plugin"]; exists {
		conf.Plugin = p
	}
	return
}

func fromYAML(prov docs.Provider, value *yaml.Node) (conf Config, err error) {
	if conf.Type, _, err = docs.GetInferenceCandidateFromYAML(prov, docs.TypeCache, value); err != nil {
		err = docs.NewLintError(value.Line, docs.LintComponentNotFound, err)
		return
	}

	for i := 0; i < len(value.Content)-1; i += 2 {
		if value.Content[i].Value == "label" {
			conf.Label = value.Content[i+1].Value
			break
		}
	}

	pluginNode, err := docs.GetPluginConfigYAML(conf.Type, value)
	if err != nil {
		err = docs.NewLintError(value.Line, docs.LintFailedRead, err)
		return
	}

	conf.Plugin = &pluginNode
	return
}
