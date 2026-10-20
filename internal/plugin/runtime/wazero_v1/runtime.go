package wazerov1

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"

	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
	"github.com/warpstreamlabs/bento/internal/plugin"
	"github.com/warpstreamlabs/bento/internal/plugin/runtime"
)

const (
	pluginProcessFn      = "process"
	pluginProcessBatchFn = "process_batch"
	pluginInitFn         = "init_plugin"

	pluginAllocFn   = "alloc"
	pluginReleaseFn = "release"
)

var _ runtime.Runtime = (*WazeroRuntime)(nil)

func init() {
	plugin.GlobalRuntime = NewPluginRuntime()
}

// WazeroRuntime runs plugins with wazero.
type WazeroRuntime struct {
	plugins []*wazeroPlugin
}

func NewPluginRuntime() *WazeroRuntime {
	return &WazeroRuntime{}
}

func (rt *WazeroRuntime) Register(manifest *runtime.Manifest, source runtime.Source) (runtime.Plugin, error) {
	spec, err := manifest.ComponentSpec()
	if err != nil {
		return nil, err
	}

	conf := manifest.Runtime.Wasm
	moduleConf, err := toModuleConfig(conf)
	if err != nil {
		return nil, err
	}

	path := conf.Path
	if path == "" {
		path = "plugin.wasm"
	}

	var wasm []byte
	switch s := source.(type) {
	case runtime.DirSource:
		wasm, err = ifs.ReadFile(ifs.OS(), filepath.Join(string(s), path))
	case runtime.FSSource:
		wasm, err = fs.ReadFile(s.FS, path)
	case runtime.ByteSource:
		wasm = s
	default:
		err = fmt.Errorf("unsupported plugin source: %T", source)
	}
	if err != nil {
		return nil, err
	}

	// Compilation is deferred until the plugin is first used, and its result
	// is cached, so it must not depend on the context of a caller.
	p := newPlugin(spec, func() (*compiledPlugin, error) {
		return compile(context.Background(), wasm, toRuntimeConfig(conf), moduleConf)
	})
	rt.plugins = append(rt.plugins, p)

	return p, nil
}

func (rt *WazeroRuntime) Close(ctx context.Context) error {
	var errs []error
	for _, p := range rt.plugins {
		errs = append(errs, p.Close(ctx))
	}
	rt.plugins = nil

	return errors.Join(errs...)
}

//------------------------------------------------------------------------------

type compiledPlugin struct {
	runtime  wazero.Runtime
	compiled wazero.CompiledModule
	config   wazero.ModuleConfig

	processFn string
}

func compile(ctx context.Context, wasm []byte, runtimeConf wazero.RuntimeConfig, moduleConf wazero.ModuleConfig) (*compiledPlugin, error) {
	var err error

	rt := wazero.NewRuntimeWithConfig(ctx, runtimeConf)
	defer func() {
		if err != nil {
			_ = rt.Close(ctx)
		}
	}()

	if _, err := wasi_snapshot_preview1.Instantiate(ctx, rt); err != nil {
		return nil, err
	}

	compiled, err := rt.CompileModule(ctx, wasm)
	if err != nil {
		return nil, fmt.Errorf("failed to compile module: %w", err)
	}

	defs := compiled.ExportedFunctions()

	processFn := pluginProcessFn
	if _, batched := defs[pluginProcessBatchFn]; batched {
		processFn = pluginProcessBatchFn
	}

	if err := hasExportedFunctions(defs, pluginAllocFn, pluginReleaseFn, processFn); err != nil {
		return nil, err
	}

	return &compiledPlugin{
		runtime:   rt,
		compiled:  compiled,
		config:    moduleConf,
		processFn: processFn,
	}, nil
}

func (m *compiledPlugin) Close(ctx context.Context) error {
	return m.runtime.Close(ctx)
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

//------------------------------------------------------------------------------

var errInvalidMount = errors.New("mounts require both a host_path and guest_path")

func toRuntimeConfig(conf runtime.WasmRuntimeConfig) wazero.RuntimeConfig {
	runtimeConf := wazero.NewRuntimeConfig().WithCloseOnContextDone(true)
	if conf.Memory.MaxPages > 0 {
		runtimeConf = runtimeConf.WithMemoryLimitPages(conf.Memory.MaxPages)
	}
	return runtimeConf
}

func toModuleConfig(conf runtime.WasmRuntimeConfig) (wazero.ModuleConfig, error) {
	moduleConf := wazero.NewModuleConfig().
		WithName(""). // Anonymous for multiple instances
		WithStartFunctions("_initialize").
		WithSysWalltime().
		WithSysNanotime().
		WithRandSource(rand.Reader)

	fsConf := wazero.NewFSConfig()
	for _, mount := range conf.Mounts {
		if mount.HostPath == "" || mount.GuestPath == "" {
			return nil, errInvalidMount
		}

		isReadOnly := mount.ReadOnly == nil || *mount.ReadOnly
		if isReadOnly {
			fsConf = fsConf.WithReadOnlyDirMount(mount.HostPath, mount.GuestPath)
		} else {
			fsConf = fsConf.WithDirMount(mount.HostPath, mount.GuestPath)
		}
	}

	return moduleConf.WithFSConfig(fsConf), nil
}
