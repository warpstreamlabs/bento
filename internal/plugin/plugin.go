package plugin

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
	"github.com/warpstreamlabs/bento/internal/plugin/runtime"
)

var GlobalRuntime runtime.Runtime = &noopRuntime{}

type noopRuntime struct{}

var errNoPluginRuntime = errors.New("no plugin runtime enabled")

func (n *noopRuntime) Register(_ *runtime.Manifest, _ runtime.Source) (runtime.Plugin, error) {
	return nil, errNoPluginRuntime
}

func (n *noopRuntime) Close(_ context.Context) error {
	return nil
}

// Register registers a plugin with env using the global runtime.
func Register(env *bundle.Environment, manifest *runtime.Manifest, source runtime.Source) error {
	p, err := GlobalRuntime.Register(manifest, source)
	if err != nil {
		return fmt.Errorf("plugin %v failed to register: %w", manifest.Name, err)
	}

	if err := p.RegisterWith(env); err != nil {
		return fmt.Errorf("plugin %v: failed to register with environment: %w", manifest.Name, err)
	}

	return nil
}

func InitPlugins(ctx context.Context, env *bundle.Environment, rt runtime.Runtime, pluginPaths ...string) ([]string, error) {
	if len(pluginPaths) == 0 {
		return nil, nil
	}

	var lints []string
	for _, path := range pluginPaths {
		stat, err := ifs.OS().Stat(path)
		if err != nil {
			return nil, fmt.Errorf("plugin %v: error stat'ing path: %w", path, err)
		}

		var ppInfo *PortablePluginInfo

		if stat.IsDir() {
			ppInfo, err = loadDirPlugin(path)
			if err != nil {
				return nil, fmt.Errorf("dir plugin error: %w", err)
			}

		} else if strings.EqualFold(filepath.Ext(stat.Name()), ".bpz") {
			ppInfo, err = loadZipPlugin(path)
			if err != nil {
				return nil, fmt.Errorf("zip plugin error: %w", err)
			}
		} else {
			return nil, fmt.Errorf("plugin %v: must be a directory or .bpz", path)
		}

		manifestLints, err := registerPlugin(ppInfo, rt, env)
		if err != nil {
			return nil, fmt.Errorf("plugin %v: %w", path, err)
		}
		for _, l := range manifestLints {
			lints = append(lints, fmt.Sprintf("plugin %v: %v", path, l))
		}
	}

	return lints, nil
}

type PortablePluginInfo struct {
	manifest []byte
	rtSource runtime.Source
}

func registerPlugin(ppInfo *PortablePluginInfo, rt runtime.Runtime, env *bundle.Environment) ([]docs.Lint, error) {
	manifest, manifestLints, err := runtime.ReadManifestYAML(ppInfo.manifest)
	if err != nil {
		return nil, err
	}

	plugin, err := rt.Register(manifest, ppInfo.rtSource)
	if err != nil {
		return nil, fmt.Errorf("%v: failed to register: %w", manifest.Name, err)
	}

	if err := plugin.RegisterWith(env); err != nil {
		return nil, fmt.Errorf("%v: failed to register with environment: %w", manifest.Name, err)
	}

	return manifestLints, nil
}

// Load from a plugin zip file
func loadZipPlugin(path string) (*PortablePluginInfo, error) {
	reader, err := zip.OpenReader(path)
	if err != nil {
		return nil, fmt.Errorf("plugin %v: error parsing bpz: %w", path, err)
	}
	defer reader.Close()

	manifestBytes, err := fs.ReadFile(reader, "plugin.yaml")
	if err != nil {
		return nil, fmt.Errorf("plugin %v: error extracting manifest file: %w", path, err)
	}

	return &PortablePluginInfo{
		manifest: manifestBytes,
		rtSource: runtime.FSSource{FS: reader},
	}, nil
}

// Load from plugin directory
func loadDirPlugin(path string) (*PortablePluginInfo, error) {
	manifestPath := filepath.Join(path, "plugin.yaml")

	manifestBytes, err := ifs.ReadFile(ifs.OS(), manifestPath)
	if err != nil {
		return nil, fmt.Errorf("plugin %v: error reading manifest file: %w", path, err)
	}

	return &PortablePluginInfo{
		manifest: manifestBytes,
		rtSource: runtime.DirSource(path),
	}, nil
}
