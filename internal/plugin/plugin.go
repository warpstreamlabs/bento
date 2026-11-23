package plugin

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
	"github.com/warpstreamlabs/bento/internal/plugin/runtime"
)

var GlobalRuntime runtime.Runtime = &noopRuntime{}

type noopRuntime struct{}

var errNoPluginRuntime = errors.New("no plugin runtime enabled")

func (n *noopRuntime) Register(_ *runtime.Manifest, _ runtime.Source, _ runtime.WasmRuntimeConfig) (runtime.Plugin, error) {
	return nil, errNoPluginRuntime
}

func (n *noopRuntime) Close(_ context.Context) error {
	return nil
}

// Register registers a plugin from Go code with env using the global runtime.
// Plugins registered this way are trusted, so the deprecated runtime block of
// a manifest is still honoured.
func Register(env *bundle.Environment, manifest *runtime.Manifest, wasm []byte) error {
	conf, _, err := runtime.Resolve(manifest, runtime.Grant{}, runtime.ResolveOptions{AllowLegacy: true})
	if err != nil {
		return fmt.Errorf("plugin %v: %w", manifest.Name, err)
	}
	if _, _, err := runtime.CheckABI(wasm); err != nil {
		return fmt.Errorf("plugin %v: %w", manifest.Name, err)
	}
	return register(env, GlobalRuntime, manifest, wasm, conf)
}

func register(env *bundle.Environment, rt runtime.Runtime, manifest *runtime.Manifest, wasm []byte, conf runtime.WasmRuntimeConfig) error {
	p, err := rt.Register(manifest, runtime.ByteSource(wasm), conf)
	if err != nil {
		return fmt.Errorf("plugin %v failed to register: %w", manifest.Name, err)
	}

	if err := p.RegisterWith(env); err != nil {
		return fmt.Errorf("plugin %v: failed to register with environment: %w", manifest.Name, err)
	}
	return nil
}

//------------------------------------------------------------------------------

// Entry is a plugin to load, and what it is granted.
type Entry struct {
	Source string
	Grant  runtime.Grant
}

// Loader loads plugins into an environment. Loading the same source again
// with the same grant does nothing, so configs can be read more than once.
type Loader struct {
	env *bundle.Environment
	rt  runtime.Runtime

	mu      sync.Mutex
	sources map[string]loaded // by source
	names   map[string]string // plugin name to source
}

type loaded struct {
	name  string
	grant runtime.Grant
}

// NewLoader returns a loader that registers plugins with env using rt.
func NewLoader(env *bundle.Environment, rt runtime.Runtime) *Loader {
	return &Loader{
		env:     env,
		rt:      rt,
		sources: map[string]loaded{},
		names:   map[string]string{},
	}
}

var (
	globalLoader     *Loader
	globalLoaderOnce sync.Once
)

// GlobalLoader returns the loader for the global environment and runtime.
func GlobalLoader() *Loader {
	globalLoaderOnce.Do(func() {
		globalLoader = NewLoader(bundle.GlobalEnvironment, GlobalRuntime)
	})
	return globalLoader
}

// Load loads each plugin. Lints are problems with a plugin's manifest, and
// warnings are things the plugin's author or user should fix but which don't
// prevent it from running.
func (l *Loader) Load(entries ...Entry) (lints, warnings []string, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, e := range entries {
		eLints, eWarns, err := l.load(e)
		for _, lint := range eLints {
			lints = append(lints, fmt.Sprintf("plugin %v: %v", e.Source, lint))
		}
		for _, w := range eWarns {
			warnings = append(warnings, fmt.Sprintf("plugin %v: %v", e.Source, w))
		}
		if err != nil {
			return lints, warnings, fmt.Errorf("plugin %v: %w", e.Source, err)
		}
	}
	return lints, warnings, nil
}

func (l *Loader) load(e Entry) (lints, warnings []string, err error) {
	if e.Source == "" {
		return nil, nil, errors.New("source must be set")
	}
	key := sourceKey(e.Source)

	if prev, exists := l.sources[key]; exists {
		if reflect.DeepEqual(normaliseGrant(prev.grant), normaliseGrant(e.Grant)) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("plugin %v is loaded more than once with different permissions; if it's passed with -p, remove that and use only the plugins section of the config", prev.name)
	}

	if strings.Contains(e.Source, "://") {
		return nil, nil, errors.New("only plugin directories are supported")
	}

	manifestBytes, err := ifs.ReadFile(ifs.OS(), filepath.Join(e.Source, "plugin.yaml"))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read manifest: %w", err)
	}

	manifest, manifestLints, err := runtime.ReadManifestYAML(manifestBytes)
	if err != nil {
		return nil, nil, err
	}
	for _, lint := range manifestLints {
		lints = append(lints, lint.Error())
	}

	if other, exists := l.names[manifest.Name]; exists {
		return lints, nil, fmt.Errorf("a plugin named %v is already loaded from %v", manifest.Name, other)
	}

	// Plugins in local directories are trusted, so may still use the
	// deprecated runtime block.
	conf, resolveWarns, err := runtime.Resolve(manifest, e.Grant, runtime.ResolveOptions{AllowLegacy: true})
	if err != nil {
		return lints, nil, err
	}
	warnings = append(warnings, resolveWarns...)

	wasm, err := ifs.ReadFile(ifs.OS(), filepath.Join(e.Source, conf.Path))
	if err != nil {
		return lints, warnings, fmt.Errorf("failed to read wasm binary: %w", err)
	}

	_, abiWarns, err := runtime.CheckABI(wasm)
	if err != nil {
		return lints, warnings, err
	}
	warnings = append(warnings, abiWarns...)

	if err := register(l.env, l.rt, manifest, wasm, conf); err != nil {
		return lints, warnings, err
	}

	l.sources[key] = loaded{name: manifest.Name, grant: e.Grant}
	l.names[manifest.Name] = e.Source
	return lints, warnings, nil
}

func sourceKey(source string) string {
	if strings.Contains(source, "://") {
		return source
	}
	if abs, err := filepath.Abs(source); err == nil {
		return abs
	}
	return filepath.Clean(source)
}

func normaliseGrant(g runtime.Grant) runtime.Grant {
	if len(g.Mounts) == 0 {
		g.Mounts = nil
	}
	return g
}
