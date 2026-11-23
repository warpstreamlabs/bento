package common

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"gopkg.in/yaml.v3"

	"github.com/urfave/cli/v2"

	"github.com/warpstreamlabs/bento/internal/config"
	"github.com/warpstreamlabs/bento/internal/docs"
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
		&cli.StringSliceFlag{
			Name:    "plugin-insecure-registry",
			Usage:   "EXPERIMENTAL: a registry, as host or host:port, to reach without verifying its TLS certificate, or over plain HTTP if it doesn't support HTTPS, as with Docker's insecure-registries. Registries on localhost always are.",
			EnvVars: []string{"BENTO_PLUGIN_INSECURE_REGISTRIES"},
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

// PluginRegistries returns access to registries, using the credentials saved
// by bento plugin login or docker login.
func PluginRegistries(c *cli.Context) (*oci.Registries, error) {
	creds, err := oci.CredentialStore()
	if err != nil {
		return nil, err
	}
	return oci.NewRegistries(creds, c.StringSlice("plugin-insecure-registry")), nil
}

// OpenPluginStore opens the local plugin store, with access to registries.
func OpenPluginStore(c *cli.Context) (*oci.Store, error) {
	registries, err := PluginRegistries(c)
	if err != nil {
		return nil, err
	}
	return oci.OpenStore(c.Context, PluginStoreDir(c), registries.Remote())
}

// EnableRegistryPlugins lets the global plugin loader load oci:// plugins,
// pulling them according to --plugin-pull. The store is only opened if such
// a plugin is used.
func EnableRegistryPlugins(c *cli.Context) error {
	policy, err := oci.ParsePullPolicy(c.String("plugin-pull"))
	if err != nil {
		return err
	}

	var (
		mu    sync.Mutex
		store *oci.Store
	)
	plugin.GlobalLoader().SetRemote(oci.SourcePrefix, func(ctx context.Context, source string) ([]byte, []byte, error) {
		mu.Lock()
		if store == nil {
			var err error
			if store, err = OpenPluginStore(c); err != nil {
				mu.Unlock()
				return nil, nil, err
			}
		}
		mu.Unlock()
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

	ctx, cancel := pluginContext(c.Context)
	defer cancel()
	lints, warnings, err := plugin.GlobalLoader().Load(ctx, entries...)
	for _, w := range warnings {
		fmt.Fprintf(stderr, "WARN: %v\n", w)
	}
	return lints, err
}

// LoadConfigPlugins loads the plugins listed in the plugins section of a
// parsed config.
func LoadConfigPlugins(ctx context.Context, root *yaml.Node) (lints, warnings []string, err error) {
	entries, err := plugin.EntriesFromConfig(root)
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := pluginContext(ctx)
	defer cancel()
	return plugin.GlobalLoader().Load(ctx, entries...)
}

// LoadConfigFilePlugins loads the plugins listed in the plugins section of a
// config file, for commands that read configs without a config.Reader, such
// as lint and test. A file that can't be read or parsed loads nothing, as the
// command reports that itself.
func LoadConfigFilePlugins(ctx context.Context, path string) (lints, warnings []string, err error) {
	confBytes, _, _, err := config.ReadFileEnvSwap(ifs.OS(), path, os.LookupEnv)
	if err != nil {
		return nil, nil, nil
	}
	root, err := docs.UnmarshalYAML(confBytes)
	if err != nil {
		return nil, nil, nil
	}
	return LoadConfigPlugins(ctx, root)
}

// pluginContext returns a context for loading plugins, which may pull them
// from a registry, that is cancelled by an interrupt.
func pluginContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
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
		return LoadConfigPlugins(context.Background(), root)
	}
}
