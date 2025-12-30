//go:build !wasm

package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/experimental/sock"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
	"github.com/warpstreamlabs/bento/internal/plugin"
	"github.com/warpstreamlabs/bento/internal/plugin/model"
	"golang.org/x/sync/singleflight"
)

const wasmPageSize uint64 = 65_536 // 64KB

var errRuntimeNotInitialised = errors.New("runtime is not initialised")

var _ plugin.Runtime[api.Module] = (*WasmRuntime)(nil)

type WasmRuntime struct {
	runtime wazero.Runtime
	modules map[string]wazero.CompiledModule
	group   singleflight.Group

	loadMu sync.RWMutex
}

func NewPluginRuntime() *WasmRuntime {
	return &WasmRuntime{
		modules: make(map[string]wazero.CompiledModule),
	}
}

func (r *WasmRuntime) Load(ctx context.Context, conf plugin.RuntimeConfig) error {
	if r.runtime != nil {
		return nil
	}

	runtimeSettings := wazero.NewRuntimeConfig().WithCloseOnContextDone(true).WithDebugInfoEnabled(true)
	if conf.MaxMemory > 0 {
		limitPages := conf.MaxMemory / wasmPageSize
		runtimeSettings = runtimeSettings.WithMemoryLimitPages(uint32(limitPages))
	}

	rt := wazero.NewRuntimeWithConfig(ctx, runtimeSettings)
	if _, err := wasi_snapshot_preview1.Instantiate(ctx, rt); err != nil {
		return err
	}

	r.runtime = rt
	return nil
}

func (r *WasmRuntime) Register(ctx context.Context, conf plugin.ModuleConfig) (plugin.Plugin[api.Module], error) {
	if r.runtime == nil {
		return nil, errRuntimeNotInitialised
	}

	wasmBytes, err := ifs.ReadFile(ifs.OS(), conf.Path)
	if err != nil {
		return nil, err
	}

	cm, err := r.Compile(ctx, conf.Name, wasmBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to compile module: %w", err)
	}

	functionDefs := cm.ExportedFunctions()
	requiredExports := []string{"init_component", "get_plugin_spec", "malloc", "free"}
	if err := hasExportedFunctions(functionDefs, requiredExports...); err != nil {
		return nil, err
	}

	moduleConf, err := toModuleConfig(conf)
	if err != nil {
		return nil, err
	}

	socketConf, err := toSocketConfig(conf.Sockets)
	if err != nil {
		return nil, err
	}

	modCtx := context.Background()
	if socketConf != nil {
		modCtx = sock.WithConfig(ctx, socketConf)
	}

	modulePool := plugin.NewInstancePool(func(ctx context.Context) (api.Module, error) {
		module, err := r.runtime.InstantiateModule(modCtx, cm, moduleConf)
		if err != nil {
			return nil, fmt.Errorf("failed to instantiate module: %w", err)
		}

		return module, nil
	})

	module, err := modulePool.Get(modCtx)
	if err != nil {
		return nil, err
	}
	defer modulePool.Put(module)

	getPluginSpec := module.ExportedFunction("get_plugin_spec")
	specResults, err := getPluginSpec.Call(ctx)
	if err != nil {
		return nil, err
	}

	pluginSpecBytes := model.LoadBytes(module, model.Buffer(specResults[0]))

	spec, err := model.FromProtoPluginSpec(pluginSpecBytes)
	if err != nil {
		return nil, err
	}

	return NewWasmPlugin(spec, modulePool, functionDefs)
}

func (r *WasmRuntime) Compile(ctx context.Context, name string, wasmBytes []byte) (wazero.CompiledModule, error) {
	r.loadMu.RLock()
	if compiledModule, ok := r.modules[name]; ok {
		r.loadMu.RUnlock()
		return compiledModule, nil
	}
	r.loadMu.RUnlock()

	compiledModule, err, _ := r.group.Do(name, func() (interface{}, error) {
		r.loadMu.RLock()
		if m, ok := r.modules[name]; ok {
			r.loadMu.RUnlock()
			return m, nil
		}
		r.loadMu.RUnlock()

		compiledModule, err := r.runtime.CompileModule(ctx, wasmBytes)
		if err != nil {
			return nil, err
		}
		r.loadMu.Lock()
		r.modules[name] = compiledModule
		r.loadMu.Unlock()

		return compiledModule, nil
	})
	if err != nil {
		return nil, err
	}

	return compiledModule.(wazero.CompiledModule), err
}

func (r *WasmRuntime) Close(ctx context.Context) error {
	if r.runtime == nil {
		return nil
	}
	return r.runtime.Close(ctx)
}

func hasExportedFunctions(defs map[string]api.FunctionDefinition, required ...string) error {
	var missing []string
	for _, name := range required {
		if _, ok := defs[name]; !ok {
			missing = append(missing, name)
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf("wasm guest plugin missing required exports: [%s]", strings.Join(missing, ", "))
	}

	return nil
}
