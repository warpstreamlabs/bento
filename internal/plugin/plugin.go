package plugin

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/warpstreamlabs/bento/internal/bundle"
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
		var source runtime.Source
		var manifestBytes []byte
		var err error

		if strings.ToLower(filepath.Ext(path)) == ".bpz" {
			manifestBytes, source, err = getBPZPlugin(path)
			if err != nil {
				return nil, fmt.Errorf("error loading plugin %v: %w", path, err)
			}
		} else {
			manifestBytes, source, err = getDirectoryPlugin(path)
			if err != nil {
				return nil, fmt.Errorf("error loading plugin %v: %w", path, err)
			}
		}

		manifest, manifestLints, err := runtime.ReadManifestYAML(manifestBytes)
		if err != nil {
			return nil, fmt.Errorf("plugin %v: %w", path, err)
		}

		for _, l := range manifestLints {
			lints = append(lints, fmt.Sprintf("plugin %v: %v", path, l))
		}

		plugin, err := rt.Register(manifest, source)
		if err != nil {
			return nil, fmt.Errorf("plugin %v failed to register: %w", manifest.Name, err)
		}

		if err := plugin.RegisterWith(env); err != nil {
			return nil, fmt.Errorf("plugin %v: failed to register with environment: %w", manifest.Name, err)
		}

	}

	return lints, nil
}

func getDirectoryPlugin(path string) ([]byte, runtime.Source, error) {
	manifestPath := filepath.Join(path, "plugin.yaml")
	manifestBytes, err := ifs.ReadFile(ifs.OS(), manifestPath)
	if err != nil {
		return nil, nil, fmt.Errorf("plugin %v: failed to read manifest: %w", path, err)
	}
	source := runtime.DirSource(path)

	return manifestBytes, source, nil
}

func getBPZPlugin(path string) ([]byte, runtime.Source, error) {
	reader, err := zip.OpenReader(path)
	if err != nil {
		return nil, nil, fmt.Errorf("plugin %v: failed to open: %w", path, err)
	}
	defer reader.Close()

	manifestFile, err := reader.Open("plugin.yaml")

	if err != nil {
		return nil, nil, fmt.Errorf("plugin %v: failed to read manifest: %w", path, err)
	}

	manifestBytes, err := io.ReadAll(manifestFile)
	if err != nil {
		return nil, nil, fmt.Errorf("plugin %v: error while reading manifest: %w", path, err)
	}

	source := runtime.FSSource{FS: reader}

	return manifestBytes, source, nil
}
