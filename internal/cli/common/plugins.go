package common

import (
	"context"
	"fmt"
	"io"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/urfave/cli/v2"

	"github.com/warpstreamlabs/bento/internal/config"
	"github.com/warpstreamlabs/bento/internal/filepath"
	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
	"github.com/warpstreamlabs/bento/internal/plugin"
	"github.com/warpstreamlabs/bento/internal/plugin/oci"
)

// PluginStoreFlags are the flags that set where plugins from registries are
// stored and when they're pulled.
func PluginStoreFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:    "plugin-store",
			Usage:   "EXPERIMENTAL: the directory of the local plugin store (default: $BENTO_PLUGIN_STORE, or bento/plugins in the user cache directory)",
			EnvVars: []string{oci.EnvStoreDir},
		},
		&cli.StringFlag{
			Name:  "plugin-pull",
			Value: string(oci.PullMissing),
			Usage: "EXPERIMENTAL: when to pull oci:// plugins from their registry: missing, always or never",
		},
	}
}

// PluginStoreDir returns the directory of the local plugin store.
func PluginStoreDir(c *cli.Context) string {
	if dir := c.String("plugin-store"); dir != "" {
		return dir
	}
	return oci.DefaultStoreDir()
}

// OpenPluginStore opens the local plugin store, with access to registries
// using the credentials saved by bento plugin login or docker login.
func OpenPluginStore(c *cli.Context) (*oci.Store, error) {
	creds, err := oci.CredentialStore()
	if err != nil {
		return nil, err
	}
	return oci.OpenStore(c.Context, PluginStoreDir(c), oci.RegistryRemote(creds))
}

// EnableRegistryPlugins lets the global plugin loader load oci:// plugins,
// pulling them according to --plugin-pull. The store is only opened if such
// a plugin is used.
func EnableRegistryPlugins(c *cli.Context) error {
	policy, err := oci.ParsePullPolicy(c.String("plugin-pull"))
	if err != nil {
		return err
	}

	var store *oci.Store
	plugin.GlobalLoader().SetRemote(oci.SourcePrefix, func(ctx context.Context, source string) ([]byte, []byte, error) {
		if store == nil {
			var err error
			if store, err = OpenPluginStore(c); err != nil {
				return nil, nil, err
			}
		}
		return store.Fetch(ctx, source, policy)
	})
	return nil
}

// LoadFlagPlugins loads the plugins passed with --plugin (-p), which are given
// no mounts or memory limits. Warnings are written to stderr and lints are
// returned.
func LoadFlagPlugins(c *cli.Context, stderr io.Writer) (lints []string, err error) {
	var entries []plugin.Entry
	var globs []string
	for _, p := range c.StringSlice("plugin") {
		if strings.HasPrefix(p, oci.SourcePrefix) {
			entries = append(entries, plugin.Entry{Source: p})
		} else {
			globs = append(globs, p)
		}
	}

	paths, err := filepath.Globs(ifs.OS(), globs)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve plugin glob pattern: %w", err)
	}
	for _, p := range paths {
		entries = append(entries, plugin.Entry{Source: p})
	}

	lints, warnings, err := plugin.GlobalLoader().Load(entries...)
	for _, w := range warnings {
		fmt.Fprintf(stderr, "WARN: %v\n", w)
	}
	return lints, err
}

// LoadConfigPlugins loads the plugins listed in the plugins section of a
// parsed config.
func LoadConfigPlugins(root *yaml.Node) (lints, warnings []string, err error) {
	entries, err := plugin.EntriesFromConfig(root)
	if err != nil {
		return nil, nil, err
	}
	return plugin.GlobalLoader().Load(entries...)
}

// PluginsOnMainRead returns a function for config.OptOnMainRead that loads the
// plugins in the plugins section of the main config when it is first read.
// Plugins can't be unloaded, so later changes to the section only produce a
// warning.
func PluginsOnMainRead() config.MainReadFunc {
	var loaded bool
	var section string
	return func(root *yaml.Node) (lints, warnings []string, err error) {
		current := plugin.ConfigSectionYAML(root)
		if loaded {
			if current != section {
				return nil, []string{"the plugins section of the config has changed, restart Bento to apply the change"}, nil
			}
			return nil, nil, nil
		}
		loaded, section = true, current
		return LoadConfigPlugins(root)
	}
}
