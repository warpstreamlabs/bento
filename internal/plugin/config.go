package plugin

import (
	"bytes"
	"fmt"

	"gopkg.in/yaml.v3"

	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/plugin/runtime"
)

// FieldPlugins is the field of the main config that lists plugins to load.
const FieldPlugins = "plugins"

// ConfigSpec describes the plugins section of the main config.
func ConfigSpec() docs.FieldSpec {
	return docs.FieldObject(FieldPlugins, "EXPERIMENTAL: WASM plugins to load, and the mounts and memory each one is allowed. Plugins are loaded before the rest of the config is read, so changes to this section need a restart.").
		Array().
		WithChildren(append(docs.FieldSpecs{
			docs.FieldString("source", "The path to a plugin's directory.", "./plugins/uppercase"),
		}, runtime.GrantSpec()...)...).
		Optional().
		Advanced()
}

type configEntry struct {
	Source        string `yaml:"source"`
	runtime.Grant `yaml:",inline"`
}

// EntriesFromConfig returns the plugins listed in the plugins section of a
// parsed main config, if it has one.
func EntriesFromConfig(root *yaml.Node) ([]Entry, error) {
	section := pluginsNode(root)
	if section == nil {
		return nil, nil
	}

	var confs []configEntry
	if err := section.Decode(&confs); err != nil {
		return nil, fmt.Errorf("%v: %w", FieldPlugins, err)
	}

	entries := make([]Entry, len(confs))
	for i, c := range confs {
		entries[i] = Entry{Source: c.Source, Grant: c.Grant}
	}
	return entries, nil
}

// ConfigSectionYAML returns the plugins section of a parsed main config as
// YAML, for detecting changes to it.
func ConfigSectionYAML(root *yaml.Node) string {
	section := pluginsNode(root)
	if section == nil {
		return ""
	}
	var buf bytes.Buffer
	_ = yaml.NewEncoder(&buf).Encode(section)
	return buf.String()
}

func pluginsNode(root *yaml.Node) *yaml.Node {
	if root == nil {
		return nil
	}
	if root.Kind == yaml.DocumentNode && len(root.Content) > 0 {
		root = root.Content[0]
	}
	if root.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(root.Content); i += 2 {
		if root.Content[i].Value == FieldPlugins {
			return root.Content[i+1]
		}
	}
	return nil
}
