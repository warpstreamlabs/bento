package runtime

import (
	"fmt"
	"strconv"

	"github.com/dustin/go-humanize"
	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/template"
	"gopkg.in/yaml.v3"
)

type MemorySize uint64

func (m *MemorySize) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.ScalarNode {
		if i, err := strconv.ParseInt(value.Value, 10, 64); err == nil {
			*m = MemorySize(i)
			return nil
		}
		b, err := humanize.ParseBytes(value.Value)
		if err != nil {
			return err
		}
		*m = MemorySize(b)
		return nil
	}
	return fmt.Errorf("expected a memory string or integer, got %v", value.Tag)
}

// Manifest is the configuration for a single plugin.
type Manifest struct {
	Name        string                 `yaml:"name"`
	Type        string                 `yaml:"type"`
	Status      string                 `yaml:"status"`
	Summary     string                 `yaml:"summary"`
	Description string                 `yaml:"description"`
	Fields      []template.FieldConfig `yaml:"fields"`
	Runtime     RuntimeManifest        `yaml:"runtime"`
}

// RuntimeManifest specifies the runtime configuration for a plugin.
type RuntimeManifest struct {
	Wasm WasmRuntimeConfig `yaml:"wasm"`
}

// WasmRuntimeConfig is the configuration for the WASM runtime.
type WasmRuntimeConfig struct {
	Path         string            `yaml:"path"`
	Env          map[string]string `yaml:"env"`
	Mounts       []MountConfig     `yaml:"mounts"`
	AllowedHosts []string          `yaml:"allowed_hosts"`
	Config       map[string]string `yaml:"config"`
	Memory       MemoryConfig      `yaml:"memory"`
}

// MemoryConfig contains memory-related configuration for the WASM runtime.
type MemoryConfig struct {
	MaxPages             uint32     `yaml:"max_pages"`
	MaxHttpResponseBytes MemorySize `yaml:"max_http_response_bytes"`
	MaxVarBytes          MemorySize `yaml:"max_var_bytes"`
}

// MountConfig describes a directory mount for a plugin.
type MountConfig struct {
	HostPath  string `yaml:"host_path"`
	GuestPath string `yaml:"guest_path"`
}

func (m Manifest) ComponentSpec() (docs.ComponentSpec, error) {
	fields := make([]docs.FieldSpec, len(m.Fields))
	for i, fieldConf := range m.Fields {
		var err error
		if fields[i], err = fieldConf.FieldSpec(); err != nil {
			return docs.ComponentSpec{}, fmt.Errorf("field %v: %w", i, err)
		}
	}
	config := docs.FieldComponent().WithChildren(fields...)

	status := docs.StatusStable
	if m.Status != "" {
		status = docs.Status(m.Status)
	}

	return docs.ComponentSpec{
		Name:        m.Name,
		Type:        docs.Type(m.Type),
		Status:      status,
		Plugin:      true,
		Summary:     m.Summary,
		Description: m.Description,
		Config:      config,
	}, nil
}

func ReadManifestYAML(data []byte) (*Manifest, []docs.Lint, error) {
	var m Manifest
	if err := yaml.Unmarshal(data, &m); err != nil {
		return nil, nil, fmt.Errorf("failed to parse plugin manifest: %w", err)
	}

	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		return nil, nil, err
	}

	lints := ManifestConfigSpec().LintYAML(docs.NewLintContext(docs.NewLintConfig(bundle.GlobalEnvironment)), &node)
	return &m, lints, nil
}

func ManifestConfigSpec() docs.FieldSpecs {
	return componentMetadataSpec().Add(
		docs.FieldObject("runtime", "The runtime configuration for the plugin.").WithChildren(wasmRuntimeSpec()),
	)
}

func componentMetadataSpec() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldString("name", "The name of the plugin."),
		docs.FieldString("type", "The type of component this plugin creates.").HasOptions(
			"processor",
		),
		docs.FieldString("status", "The stability status of the plugin.").HasAnnotatedOptions(
			"stable", "This plugin is stable and will not change in breaking ways outside of major version releases.",
			"beta", "This plugin is beta and will not change in breaking ways unless a major problem is found.",
			"experimental", "This plugin is experimental and subject to breaking changes outside of major version releases.",
		).HasDefault("stable").Advanced(),
		docs.FieldString("summary", "A short summary of the plugin.").HasDefault(""),
		docs.FieldString("description", "A detailed description of the plugin and how to use it.").HasDefault(""),
		docs.FieldObject("fields", "The configuration fields of the plugin.").Array().WithChildren(template.FieldConfigSpec()...),
	}
}

func wasmRuntimeSpec() docs.FieldSpec {
	return docs.FieldObject("wasm", "The WASM runtime configuration.").WithChildren(
		docs.FieldString("path", "The path to the WASM binary relative to the plugin directory.").
			HasDefault("plugin.wasm").
			Optional(),
		docs.FieldString("env", "Environment variables to pass to the plugin.").Map().Optional(),
		docs.FieldObject("mounts", "A list of directory mounts.").Array().Optional().WithChildren(
			docs.FieldString("host_path", "The path on the host machine to mount."),
			docs.FieldString("guest_path", "The path inside the WASM container."),
		),
		docs.FieldString("allowed_hosts", "A list of hosts that the plugin is allowed to connect to.").Array().Optional(),
		docs.FieldString("config", "Arbitrary key-value configuration for the plugin.").Map().Optional(),
		docs.FieldObject("memory", "Describes the limits on the memory the plugin may be allocated.").WithChildren(
			docs.FieldInt("max_pages", "The max amount of pages the plugin can allocate. One page is 64KiB.").Optional(),
			docs.FieldString("max_http_response_bytes", "The max size of an Extism HTTP response (e.g., '10MB').").Optional(),
			docs.FieldString("max_var_bytes", "The max size of all Extism vars (e.g., '1MB').").Optional(),
		).Optional(),
	)
}
