package plugin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"

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

	// Name, if set, is the name the plugin is registered as instead of the
	// name in its manifest, so the same plugin can be loaded more than once.
	Name string

	Grant runtime.Grant
}

// Loader loads plugins into an environment.
//
// Plugins are identified by their content, not their source, so the same
// plugin from different sources (such as a tag and a digest) is the same
// plugin. Loading a plugin under a name already in use does nothing if it's
// the same plugin with the same grant, so configs can be read more than once,
// and is an error otherwise.
type Loader struct {
	env *bundle.Environment
	rt  runtime.Runtime

	mu      sync.Mutex
	remotes map[string]RemoteFetchFunc // by source prefix
	loaded  map[string]loaded          // by registered name
}

// RemoteFetchFunc fetches the manifest and WASM binary of a plugin that isn't
// in a local directory, such as an OCI artifact.
type RemoteFetchFunc func(ctx context.Context, source string) (manifest, wasm []byte, err error)

// SetRemote sets how plugins with sources starting with prefix, such as
// oci://, are fetched. Plugins fetched this way aren't trusted, so may not use
// the deprecated runtime block of manifests.
func (l *Loader) SetRemote(prefix string, fetch RemoteFetchFunc) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.remotes[prefix] = fetch
}

type loaded struct {
	source   string
	identity string
	grant    runtime.Grant
}

// NewLoader returns a loader that registers plugins with env using rt.
func NewLoader(env *bundle.Environment, rt runtime.Runtime) *Loader {
	return &Loader{
		env:     env,
		rt:      rt,
		remotes: map[string]RemoteFetchFunc{},
		loaded:  map[string]loaded{},
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
// prevent it from running. Fetching plugins can be cancelled with ctx.
func (l *Loader) Load(ctx context.Context, entries ...Entry) (lints, warnings []string, err error) {
	for _, e := range entries {
		eLints, eWarns, err := l.load(ctx, e)
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

func (l *Loader) load(ctx context.Context, e Entry) (lints, warnings []string, err error) {
	if e.Source == "" {
		return nil, nil, errors.New("source must be set")
	}

	// Fetching may need the network, so happens without holding the lock.
	manifestBytes, wasm, trusted, err := l.fetch(ctx, e.Source)
	if err != nil {
		return nil, nil, err
	}

	manifest, manifestLints, err := runtime.ReadManifestYAML(manifestBytes)
	if err != nil {
		return nil, nil, err
	}
	for _, lint := range manifestLints {
		lints = append(lints, lint.Error())
	}
	if e.Name != "" {
		manifest.Name = e.Name
	}
	if manifest.Name == "" {
		return lints, nil, errors.New("plugin has no name")
	}

	// Plugins in local directories are trusted, so may still use the
	// deprecated runtime block.
	conf, resolveWarns, err := runtime.Resolve(manifest, e.Grant, runtime.ResolveOptions{AllowLegacy: trusted})
	if err != nil {
		return lints, nil, err
	}
	warnings = append(warnings, resolveWarns...)

	if wasm == nil {
		if wasm, err = ifs.ReadFile(ifs.OS(), filepath.Join(e.Source, conf.Path)); err != nil {
			return lints, warnings, fmt.Errorf("failed to read wasm binary: %w", err)
		}
	}

	_, abiWarns, err := runtime.CheckABI(wasm)
	if err != nil {
		return lints, warnings, err
	}
	warnings = append(warnings, abiWarns...)

	identity := contentIdentity(manifestBytes, wasm)

	l.mu.Lock()
	defer l.mu.Unlock()

	if prev, exists := l.loaded[manifest.Name]; exists {
		switch {
		case prev.identity != identity:
			return lints, warnings, fmt.Errorf("a different plugin named %v is already loaded from %v; set a different `name` for one of them in the plugins section", manifest.Name, prev.source)
		case !reflect.DeepEqual(normaliseGrant(prev.grant), normaliseGrant(e.Grant)):
			return lints, warnings, fmt.Errorf("plugin %v is already loaded from %v with different permissions; to load it twice, set a different `name` for one of them in the plugins section, or if it's also passed with -p, remove that", manifest.Name, prev.source)
		}
		// Already loaded, which happens when a config is read again.
		return nil, nil, nil
	}

	if err := register(l.env, l.rt, manifest, wasm, conf); err != nil {
		return lints, warnings, err
	}
	l.loaded[manifest.Name] = loaded{source: e.Source, identity: identity, grant: e.Grant}
	return lints, warnings, nil
}

// contentIdentity identifies a plugin by its content. The manifest is
// normalised first, so a plugin.yaml and the same manifest as the JSON config
// of an OCI artifact are the same.
func contentIdentity(manifest, wasm []byte) string {
	var generic any
	if err := yaml.Unmarshal(manifest, &generic); err == nil {
		if normalised, err := json.Marshal(generic); err == nil {
			manifest = normalised
		}
	}

	h := sha256.New()
	h.Write(manifest)
	h.Write([]byte{0})
	h.Write(wasm)
	return hex.EncodeToString(h.Sum(nil))
}

// fetch returns the manifest of a plugin, and its WASM binary if it isn't in
// a local directory, along with whether it is trusted.
func (l *Loader) fetch(ctx context.Context, source string) (manifest, wasm []byte, trusted bool, err error) {
	if i := strings.Index(source, "://"); i >= 0 {
		prefix := source[:i+3]
		l.mu.Lock()
		fetch, exists := l.remotes[prefix]
		l.mu.Unlock()
		if !exists {
			return nil, nil, false, fmt.Errorf("unsupported plugin source %q, expected a directory", prefix)
		}
		manifest, wasm, err = fetch(ctx, source)
		return manifest, wasm, false, err
	}

	manifest, err = ifs.ReadFile(ifs.OS(), filepath.Join(source, "plugin.yaml"))
	if err != nil {
		return nil, nil, false, fmt.Errorf("failed to read manifest: %w", err)
	}
	return manifest, nil, true, nil
}

func normaliseGrant(g runtime.Grant) runtime.Grant {
	if len(g.Mounts) == 0 {
		g.Mounts = nil
	}
	return g
}
