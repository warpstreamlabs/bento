package plugin

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/warpstreamlabs/bento/internal/bundle"
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
		pluginFs, pluginFsCloser, err := getPluginFs(path)
		if err != nil {
			return nil, fmt.Errorf("plugin %v: %w", path, err)
		}
		if pluginFsCloser != nil {
			defer pluginFsCloser.Close()
		}

		manifestBytes, err := fs.ReadFile(pluginFs, "plugin.yaml")
		if err != nil {
			return nil, fmt.Errorf("plugin %v: error while reading manifest: %w", path, err)
		}

		manifest, manifestLints, err := runtime.ReadManifestYAML(manifestBytes)
		if err != nil {
			return nil, fmt.Errorf("plugin %v: %w", path, err)
		}

		for _, l := range manifestLints {
			lints = append(lints, fmt.Sprintf("plugin %v: %v", path, l))
		}

		plugin, err := rt.Register(manifest, runtime.FSSource{FS: pluginFs})
		if err != nil {
			return nil, fmt.Errorf("plugin %v failed to register: %w", manifest.Name, err)
		}

		if err := plugin.RegisterWith(env); err != nil {
			return nil, fmt.Errorf("plugin %v: failed to register with environment: %w", manifest.Name, err)
		}

		// Close plugin FS as Manifest / WASM are now both loaded
		if pluginFsCloser != nil {
			_ = pluginFsCloser.Close()
		}
	}

	return lints, nil
}

func getPluginFs(path string) (fs.FS, io.Closer, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return nil, nil, fmt.Errorf("error reading path: %w", err)
	}

	if stat.IsDir() {
		return os.DirFS(path), nil, nil
	}

	if strings.EqualFold(filepath.Ext(path), ".bpz") {
		reader, err := zip.OpenReader(path)
		if err != nil {
			return nil, nil, fmt.Errorf("error reading bpz: %w", err)
		}

		return reader, reader, nil
	}

	return nil, nil, errors.New("must be a directory or .bpz")
}
