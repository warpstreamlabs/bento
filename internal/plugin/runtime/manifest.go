package runtime

import (
	"errors"
	"fmt"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/template"
	"gopkg.in/yaml.v3"
)

// Manifest describes a plugin: what it is, its config fields and the
// capabilities it requires. Everything in a manifest is fixed when the plugin
// is built, settings that depend on where the plugin runs are given separately
// as a Grant.
type Manifest struct {
	Name        string                 `yaml:"name"`
	Type        string                 `yaml:"type"`
	Status      string                 `yaml:"status"`
	Summary     string                 `yaml:"summary"`
	Description string                 `yaml:"description"`
	Fields      []template.FieldConfig `yaml:"fields"`
	Requires    Requirements           `yaml:"requires"`

	// Runtime is the deprecated runtime block, see legacy_manifest.go.
	Runtime *LegacyRuntimeManifest `yaml:"runtime,omitempty"`
}

// Requirements are the capabilities a plugin asks for.
type Requirements struct {
	Memory MemoryRequirement  `yaml:"memory,omitempty"`
	Mounts []MountRequirement `yaml:"mounts,omitempty"`
}

// MemoryRequirement is the memory a plugin needs to run.
type MemoryRequirement struct {
	MinPages uint32 `yaml:"min_pages,omitempty"`
}

// MountRequirement is a directory a plugin expects to be mounted.
type MountRequirement struct {
	Name        string `yaml:"name"`
	GuestPath   string `yaml:"guest_path"`
	Writable    bool   `yaml:"writable,omitempty"`
	Optional    bool   `yaml:"optional,omitempty"`
	Description string `yaml:"description,omitempty"`
}

// Grant is what the operator running a plugin allows it.
type Grant struct {
	Memory MemoryConfig          `yaml:"memory,omitempty"`
	Mounts map[string]MountGrant `yaml:"mounts,omitempty"`
}

// IsZero returns true if the grant allows nothing beyond the defaults.
func (g Grant) IsZero() bool {
	return g.Memory.MaxPages == 0 && len(g.Mounts) == 0
}

// MountGrant binds a requested mount to a directory on the host.
type MountGrant struct {
	HostPath string `yaml:"host_path"`
	Writable bool   `yaml:"writable,omitempty"`
}

// WasmRuntimeConfig is the effective configuration a plugin is run with, the
// result of resolving its requirements against a grant.
type WasmRuntimeConfig struct {
	Path   string        `yaml:"path"`
	Mounts []MountConfig `yaml:"mounts"`
	Memory MemoryConfig  `yaml:"memory"`
}

// MemoryConfig contains memory-related configuration for the WASM runtime.
type MemoryConfig struct {
	MaxPages uint32 `yaml:"max_pages,omitempty"`
}

// MountConfig describes a directory mount for a plugin.
type MountConfig struct {
	HostPath  string `yaml:"host_path"`
	GuestPath string `yaml:"guest_path"`
	ReadOnly  *bool  `yaml:"read_only,omitempty"`
}

// DefaultWasmPath is the path of the WASM binary within a plugin directory.
const DefaultWasmPath = "plugin.wasm"

// ResolveOptions controls how a manifest is resolved.
type ResolveOptions struct {
	// AllowLegacy permits the deprecated runtime block, which lets a plugin
	// choose its own mounts. Only plugins from trusted local sources should
	// set this.
	AllowLegacy bool
}

// Resolve matches the requirements of a manifest against a grant and returns
// the configuration to run the plugin with, along with any warnings.
func Resolve(m *Manifest, g Grant, opts ResolveOptions) (WasmRuntimeConfig, []string, error) {
	if m.Runtime != nil {
		return resolveLegacy(m, g, opts)
	}

	conf := WasmRuntimeConfig{
		Path:   DefaultWasmPath,
		Memory: g.Memory,
	}

	if minPages := m.Requires.Memory.MinPages; minPages > 0 && g.Memory.MaxPages > 0 && g.Memory.MaxPages < minPages {
		return conf, nil, fmt.Errorf("memory.max_pages %v is below the %v pages the plugin requires", g.Memory.MaxPages, minPages)
	}

	var errs []error
	requested := map[string]struct{}{}
	for _, req := range m.Requires.Mounts {
		if req.Name == "" || req.GuestPath == "" {
			errs = append(errs, errors.New("requires.mounts entries need both a name and guest_path"))
			continue
		}
		if _, exists := requested[req.Name]; exists {
			errs = append(errs, fmt.Errorf("mount %q is requested more than once", req.Name))
			continue
		}
		requested[req.Name] = struct{}{}

		grant, granted := g.Mounts[req.Name]
		if !granted {
			if !req.Optional {
				errs = append(errs, fmt.Errorf("mount %q (%v) is required but has not been granted a host_path", req.Name, req.GuestPath))
			}
			continue
		}
		if grant.HostPath == "" {
			errs = append(errs, fmt.Errorf("mount %q: host_path must be set", req.Name))
			continue
		}
		if grant.Writable && !req.Writable {
			errs = append(errs, fmt.Errorf("mount %q: granted as writable but the plugin only requests read access", req.Name))
			continue
		}

		readOnly := !grant.Writable
		conf.Mounts = append(conf.Mounts, MountConfig{
			HostPath:  grant.HostPath,
			GuestPath: req.GuestPath,
			ReadOnly:  &readOnly,
		})
	}

	for name := range g.Mounts {
		if _, exists := requested[name]; !exists {
			errs = append(errs, fmt.Errorf("mount %q is granted but the plugin does not request it", name))
		}
	}

	if len(errs) > 0 {
		return conf, nil, errors.Join(errs...)
	}
	return conf, nil, nil
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
		requirementsSpec(),
		legacyRuntimeSpec(),
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

func requirementsSpec() docs.FieldSpec {
	return docs.FieldObject("requires", "The capabilities the plugin needs. Whoever runs the plugin grants them in the `plugins` section of their Bento config.").WithChildren(
		docs.FieldObject("memory", "The memory the plugin needs.").WithChildren(
			docs.FieldInt("min_pages", "The fewest pages of memory each instance of the plugin needs. One page is 64KiB. Bento refuses to run the plugin with a lower `max_pages` limit.").Optional(),
		).Optional(),
		docs.FieldObject("mounts", "Directories the plugin expects to be mounted.").Array().Optional().WithChildren(
			docs.FieldString("name", "The name the mount is granted by."),
			docs.FieldString("guest_path", "The path the plugin expects the directory at."),
			docs.FieldBool("writable", "Whether the plugin needs to write to the directory.").HasDefault(false),
			docs.FieldBool("optional", "Whether the plugin can run without the mount.").HasDefault(false),
			docs.FieldString("description", "What the plugin uses the directory for.").HasDefault(""),
		),
	).Optional()
}

// GrantSpec describes the fields of a Grant.
func GrantSpec() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldObject("memory", "Limits on the memory the plugin may be allocated.").WithChildren(
			docs.FieldInt("max_pages", "The max amount of pages each instance of the plugin can allocate. One page is 64KiB. When omitted or zero, an instance can allocate up to 4GiB.").Optional(),
		).Optional(),
		docs.FieldObject("mounts", "Host directories to give the plugin, keyed by the names of the mounts the plugin requests.").Map().Optional().WithChildren(
			docs.FieldString("host_path", "The path on the host machine to mount. A relative path is relative to the working directory of Bento."),
			docs.FieldBool("writable", "Whether the plugin may write to the directory. Only allowed if the plugin requests a writable mount.").HasDefault(false),
		),
	}
}
