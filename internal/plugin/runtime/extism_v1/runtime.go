package extismv1

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"

	extism "github.com/extism/go-sdk"
	"github.com/warpstreamlabs/bento/internal/plugin"
	"github.com/warpstreamlabs/bento/internal/plugin/runtime"
)

var _ runtime.Runtime = (*ExtismRuntime)(nil)

func init() {
	plugin.GlobalRuntime = NewPluginRuntime()
}

type ExtismRuntime struct {
	registered []*extismPlugin
}

func NewPluginRuntime() *ExtismRuntime {
	return &ExtismRuntime{}
}

func (rt *ExtismRuntime) Register(manifest *runtime.Manifest, source runtime.Source) (runtime.Plugin, error) {
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

	spec, err := manifest.ComponentSpec()
	if err != nil {
		return nil, err
	}

	plugin := newPlugin(spec, func() (*extism.CompiledPlugin, error) {
		// NOTE: Compilation is deferred until first use and its result (including
		// any error) is cached, so it must not depend on a caller's ctx.
		return extism.NewCompiledPlugin(context.Background(), extismManifest, config, []extism.HostFunction{})
	})
	rt.registered = append(rt.registered, plugin)

	return plugin, nil
}

func (rt *ExtismRuntime) Close(ctx context.Context) error {
	var errs []error
	for i, plugin := range rt.registered {
		if err := plugin.Close(ctx); err != nil {
			if ctx.Err() != nil {
				rt.registered = rt.registered[i:]
				return ctx.Err()
			}
			errs = append(errs, err)
		}
	}
	rt.registered = nil

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}
