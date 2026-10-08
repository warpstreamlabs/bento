package extismv1

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"

	extism "github.com/extism/go-sdk"
	"github.com/warpstreamlabs/bento/internal/plugin/runtime"
)

type ExtismRuntime struct {
	registered map[string]*extismPlugin
}

func NewPluginRuntime() *ExtismRuntime {
	return &ExtismRuntime{
		registered: make(map[string]*extismPlugin),
	}
}

func (rt *ExtismRuntime) Register(ctx context.Context, manifest *runtime.Manifest, source runtime.Source) (runtime.Plugin[*extism.CompiledPlugin], error) {
	paths := make(map[string]string, len(manifest.Runtime.Wasm.Mounts))
	for _, mountConf := range manifest.Runtime.Wasm.Mounts {
		paths[mountConf.HostPath] = mountConf.GuestPath
	}

	fileName := manifest.Runtime.Wasm.Path
	if fileName == "" {
		fileName = "plugin.wasm"
	}

	var data extism.Wasm
	switch s := source.(type) {
	case runtime.DirSource:
		wasmPath := filepath.Join(string(s), fileName)
		data = extism.WasmFile{
			Path: wasmPath,
		}
	case runtime.FSSource:
		contents, err := fs.ReadFile(s.FS, fileName)
		if err != nil {
			return nil, err
		}

		data = extism.WasmData{
			Data: contents,
		}
	case runtime.ByteSource:
		data = extism.WasmData{
			Data: s,
		}
	}

	extismManifest := extism.Manifest{
		Wasm: []extism.Wasm{data},
		Memory: &extism.ManifestMemory{
			MaxPages:             manifest.Runtime.Wasm.Memory.MaxPages,
			MaxHttpResponseBytes: int64(manifest.Runtime.Wasm.Memory.MaxHttpResponseBytes),
			MaxVarBytes:          int64(manifest.Runtime.Wasm.Memory.MaxVarBytes),
		},
		AllowedHosts: manifest.Runtime.Wasm.AllowedHosts,
		AllowedPaths: paths,
		Config:       manifest.Runtime.Wasm.Config,
	}

	config := extism.PluginConfig{
		EnableWasi: true,
	}

	compiled, err := extism.NewCompiledPlugin(ctx, extismManifest, config, []extism.HostFunction{})
	if err != nil {
		return nil, err
	}

	spec, err := manifest.ComponentSpec()
	if err != nil {
		return nil, err
	}

	return &extismPlugin{
		spec:     spec,
		compiled: compiled,
	}, nil
}

func (rt *ExtismRuntime) Close(ctx context.Context) error {
	var errs []error
	for name, plugin := range rt.registered {
		err := plugin.compiled.Close(ctx)
		if err == nil {
			continue
		}

		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			errs = append(errs, err)
		}
		delete(rt.registered, name)
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}
