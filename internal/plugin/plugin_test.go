package plugin

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/plugin/runtime"
)

type fakePlugin struct{ name string }

func (p fakePlugin) Name() string                           { return p.name }
func (p fakePlugin) RegisterWith(*bundle.Environment) error { return nil }
func (p fakePlugin) Spec() docs.ComponentSpec               { return docs.ComponentSpec{Name: p.name} }

type registered struct {
	manifest *runtime.Manifest
	wasm     []byte
	conf     runtime.WasmRuntimeConfig
}

type fakeRuntime struct{ registered []registered }

func (r *fakeRuntime) Register(m *runtime.Manifest, source runtime.Source, conf runtime.WasmRuntimeConfig) (runtime.Plugin, error) {
	r.registered = append(r.registered, registered{manifest: m, wasm: source.(runtime.ByteSource), conf: conf})
	return fakePlugin{name: m.Name}, nil
}

func (r *fakeRuntime) Close(context.Context) error { return nil }

// wasmModule returns a minimal WASM module exporting the given names.
func wasmModule(exports ...string) []byte {
	b := []byte("\x00asm\x01\x00\x00\x00")
	section := []byte{byte(len(exports))}
	for _, name := range exports {
		section = append(section, byte(len(name)))
		section = append(section, name...)
		section = append(section, 0, 0)
	}
	b = append(b, 7, byte(len(section)))
	return append(b, section...)
}

func writePlugin(t *testing.T, manifest string, files map[string][]byte) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "plugin.yaml"), []byte(manifest), 0o644))
	for name, content := range files {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), content, 0o644))
	}
	return dir
}

const dataManifest = `
name: foo
type: processor
requires:
  mounts:
    - name: data
      guest_path: /data
`

func TestLoaderLoadsDirectory(t *testing.T) {
	rt := &fakeRuntime{}
	l := NewLoader(bundle.NewEnvironment(), rt)

	wasm := wasmModule("bento_abi_v1")
	dir := writePlugin(t, dataManifest, map[string][]byte{"plugin.wasm": wasm})

	grant := runtime.Grant{Mounts: map[string]runtime.MountGrant{"data": {HostPath: "./data"}}}
	lints, warnings, err := l.Load(Entry{Source: dir, Grant: grant})
	require.NoError(t, err)
	assert.Empty(t, lints)
	assert.Empty(t, warnings)

	require.Len(t, rt.registered, 1)
	assert.Equal(t, "foo", rt.registered[0].manifest.Name)
	assert.Equal(t, wasm, rt.registered[0].wasm)
	require.Len(t, rt.registered[0].conf.Mounts, 1)
	assert.Equal(t, "./data", rt.registered[0].conf.Mounts[0].HostPath)

	// Loading the same source with the same grant again does nothing.
	_, _, err = l.Load(Entry{Source: dir + "/", Grant: grant})
	require.NoError(t, err)
	assert.Len(t, rt.registered, 1)

	// Loading it with a different grant is an error.
	_, _, err = l.Load(Entry{Source: dir, Grant: runtime.Grant{
		Mounts: map[string]runtime.MountGrant{"data": {HostPath: "./other"}},
	}})
	require.ErrorContains(t, err, "loaded more than once with different permissions")
}

func TestLoaderErrors(t *testing.T) {
	v1 := wasmModule("bento_abi_v1")

	tests := []struct {
		name     string
		manifest string
		files    map[string][]byte
		grant    runtime.Grant
		err      string
	}{
		{
			name:     "missing grant",
			manifest: dataManifest,
			files:    map[string][]byte{"plugin.wasm": v1},
			err:      `mount "data" (/data) is required`,
		},
		{
			name:     "missing wasm",
			manifest: "name: foo\ntype: processor\n",
			err:      "failed to read wasm binary",
		},
		{
			name:     "unsupported abi",
			manifest: "name: foo\ntype: processor\n",
			files:    map[string][]byte{"plugin.wasm": wasmModule("bento_abi_v99")},
			err:      "built for ABI v99",
		},
		{
			name:     "legacy with grant",
			manifest: "name: foo\ntype: processor\nruntime:\n  wasm:\n    memory:\n      max_pages: 1\n",
			files:    map[string][]byte{"plugin.wasm": v1},
			grant:    runtime.Grant{Memory: runtime.MemoryConfig{MaxPages: 2}},
			err:      "migrate the plugin to `requires` first",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rt := &fakeRuntime{}
			l := NewLoader(bundle.NewEnvironment(), rt)
			dir := writePlugin(t, test.manifest, test.files)
			_, _, err := l.Load(Entry{Source: dir, Grant: test.grant})
			require.ErrorContains(t, err, test.err)
			assert.Contains(t, err.Error(), "plugin "+dir)
			assert.Empty(t, rt.registered)
		})
	}

	t.Run("not a directory", func(t *testing.T) {
		l := NewLoader(bundle.NewEnvironment(), &fakeRuntime{})
		_, _, err := l.Load(Entry{Source: "oci://ghcr.io/foo/bar:1"})
		require.ErrorContains(t, err, "only plugin directories are supported")
	})
}

func TestLoaderWarnings(t *testing.T) {
	rt := &fakeRuntime{}
	l := NewLoader(bundle.NewEnvironment(), rt)

	dir := writePlugin(t, `
name: foo
type: processor
runtime:
  wasm:
    path: custom.wasm
    memory:
      max_pages: 64
`, map[string][]byte{"custom.wasm": wasmModule()})

	_, warnings, err := l.Load(Entry{Source: dir})
	require.NoError(t, err)
	require.Len(t, warnings, 2)
	assert.Contains(t, warnings[0], "deprecated `runtime` block")
	assert.Contains(t, warnings[1], "does not declare an ABI version")

	require.Len(t, rt.registered, 1)
	assert.Equal(t, "custom.wasm", rt.registered[0].conf.Path)
	assert.Equal(t, uint32(64), rt.registered[0].conf.Memory.MaxPages)
}

func TestLoaderDuplicateNames(t *testing.T) {
	l := NewLoader(bundle.NewEnvironment(), &fakeRuntime{})
	files := map[string][]byte{"plugin.wasm": wasmModule("bento_abi_v1")}
	a := writePlugin(t, "name: foo\ntype: processor\n", files)
	b := writePlugin(t, "name: foo\ntype: processor\n", files)

	_, _, err := l.Load(Entry{Source: a}, Entry{Source: b})
	require.ErrorContains(t, err, "a plugin named foo is already loaded from "+a)
}
