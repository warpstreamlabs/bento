package runtime

// Support for the deprecated `runtime` block of plugin manifests, which mixed
// settings fixed by the plugin build with settings that depend on where the
// plugin runs.
//
// TODO: Remove this file, the Manifest.Runtime field and its uses two minor
// versions after the release that introduced `requires`.

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/warpstreamlabs/bento/internal/docs"
	"gopkg.in/yaml.v3"
)

// LegacyRuntimeManifest is the deprecated runtime block of a manifest.
type LegacyRuntimeManifest struct {
	Wasm WasmRuntimeConfig `yaml:"wasm"`
}

// LegacyMigrationURL is linked from the deprecation warning.
const LegacyMigrationURL = "https://warpstreamlabs.github.io/bento/docs/guides/plugins/about#migrating-from-the-runtime-block"

func resolveLegacy(m *Manifest, g Grant, opts ResolveOptions) (WasmRuntimeConfig, []string, error) {
	conf := m.Runtime.Wasm
	if conf.Path == "" {
		conf.Path = DefaultWasmPath
	}

	if !opts.AllowLegacy {
		return conf, nil, fmt.Errorf("the `runtime` block is only supported for plugins loaded from a local directory, migrate to `requires`: %v", LegacyMigrationURL)
	}
	if len(m.Requires.Mounts) > 0 || m.Requires.Memory.MinPages > 0 {
		return conf, nil, errors.New("a manifest cannot have both a `runtime` block and a `requires` block")
	}
	if !g.IsZero() {
		return conf, nil, errors.New("plugins with the deprecated `runtime` block cannot be given mounts or memory limits in the Bento config, migrate the plugin to `requires` first")
	}

	return conf, []string{legacyWarning(m)}, nil
}

func legacyWarning(m *Manifest) string {
	var manifest, config strings.Builder

	requires := Requirements{}
	grant := Grant{Memory: m.Runtime.Wasm.Memory}
	for i, mount := range m.Runtime.Wasm.Mounts {
		name := fmt.Sprintf("mount_%v", i)
		writable := mount.ReadOnly != nil && !*mount.ReadOnly
		requires.Mounts = append(requires.Mounts, MountRequirement{
			Name:      name,
			GuestPath: mount.GuestPath,
			Writable:  writable,
		})
		if grant.Mounts == nil {
			grant.Mounts = map[string]MountGrant{}
		}
		grant.Mounts[name] = MountGrant{HostPath: mount.HostPath, Writable: writable}
	}

	if len(requires.Mounts) > 0 {
		encodeYAML(&manifest, map[string]any{"requires": requires})
	}

	type entry struct {
		Source string `yaml:"source"`
		Grant  `yaml:",inline"`
	}
	encodeYAML(&config, map[string]any{"plugins": []entry{{Source: "<path to plugin>", Grant: grant}}})

	var b strings.Builder
	fmt.Fprintf(&b, "plugin %v uses the deprecated `runtime` block, which will stop working in a future release (%v).\n", m.Name, LegacyMigrationURL)
	if manifest.Len() > 0 {
		fmt.Fprintf(&b, "Replace it in plugin.yaml with:\n\n%v\n", indent(manifest.String()))
	} else {
		b.WriteString("Delete it from plugin.yaml.\n\n")
	}
	if p := m.Runtime.Wasm.Path; p != "" && p != DefaultWasmPath {
		fmt.Fprintf(&b, "Rename %v to %v.\n\n", p, DefaultWasmPath)
	}
	if grant.IsZero() {
		fmt.Fprintf(&b, "List the plugin in your Bento config with:\n\n%v", indent(config.String()))
	} else {
		fmt.Fprintf(&b, "Give the plugin its mounts and memory limit in your Bento config instead:\n\n%v", indent(config.String()))
	}
	return b.String()
}

func encodeYAML(w io.Writer, v any) {
	enc := yaml.NewEncoder(w)
	enc.SetIndent(2)
	_ = enc.Encode(v)
	_ = enc.Close()
}

func indent(s string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i, l := range lines {
		lines[i] = "    " + l
	}
	return strings.Join(lines, "\n") + "\n"
}

func legacyRuntimeSpec() docs.FieldSpec {
	return docs.FieldObject("runtime", "Deprecated: use `requires` in the manifest and grant mounts and memory in the `plugins` section of the Bento config instead.").WithChildren(
		docs.FieldObject("wasm", "The WASM runtime configuration.").WithChildren(
			docs.FieldString("path", "The path to the WASM binary relative to the plugin directory. Ignored when the binary is provided directly, such as when registering from Go.").
				HasDefault(DefaultWasmPath).
				Optional(),
			docs.FieldObject("mounts", "A list of directory mounts.").Array().Optional().WithChildren(
				docs.FieldString("host_path", "The path on the host machine to mount. A relative path is relative to the working directory of Bento."),
				docs.FieldString("guest_path", "The path inside the WASM container."),
				docs.FieldBool("read_only", "Whether to mount the path read-only.").Optional().HasDefault(true),
			),
			docs.FieldObject("memory", "Describes the limits on the memory the plugin may be allocated.").WithChildren(
				docs.FieldInt("max_pages", "The max amount of pages each instance of the plugin can allocate. One page is 64KiB. When omitted or zero, an instance can allocate up to 4GiB.").Optional(),
			).Optional(),
		),
	).Optional().Deprecated()
}
