package runtime

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolve(t *testing.T) {
	dataMount := MountRequirement{Name: "data", GuestPath: "/data"}
	cacheMount := MountRequirement{Name: "cache", GuestPath: "/cache", Writable: true, Optional: true}

	tests := []struct {
		name     string
		requires Requirements
		grant    Grant
		want     WasmRuntimeConfig
		errs     []string
	}{
		{
			name: "no requirements or grants",
			want: WasmRuntimeConfig{Path: DefaultWasmPath},
		},
		{
			name:  "memory limit without requirement",
			grant: Grant{Memory: MemoryConfig{MaxPages: 10}},
			want:  WasmRuntimeConfig{Path: DefaultWasmPath, Memory: MemoryConfig{MaxPages: 10}},
		},
		{
			name:     "memory limit meets requirement",
			requires: Requirements{Memory: MemoryRequirement{MinPages: 10}},
			grant:    Grant{Memory: MemoryConfig{MaxPages: 10}},
			want:     WasmRuntimeConfig{Path: DefaultWasmPath, Memory: MemoryConfig{MaxPages: 10}},
		},
		{
			name:     "no memory limit meets requirement",
			requires: Requirements{Memory: MemoryRequirement{MinPages: 10}},
			want:     WasmRuntimeConfig{Path: DefaultWasmPath},
		},
		{
			name:     "memory limit below requirement",
			requires: Requirements{Memory: MemoryRequirement{MinPages: 10}},
			grant:    Grant{Memory: MemoryConfig{MaxPages: 9}},
			errs:     []string{"below the 10 pages"},
		},
		{
			name:     "required mount granted read only",
			requires: Requirements{Mounts: []MountRequirement{dataMount}},
			grant:    Grant{Mounts: map[string]MountGrant{"data": {HostPath: "./data"}}},
			want: WasmRuntimeConfig{Path: DefaultWasmPath, Mounts: []MountConfig{
				{HostPath: "./data", GuestPath: "/data", ReadOnly: new(true)},
			}},
		},
		{
			name:     "required mount not granted",
			requires: Requirements{Mounts: []MountRequirement{dataMount}},
			errs:     []string{`mount "data" (/data) is required`},
		},
		{
			name:     "optional mount not granted",
			requires: Requirements{Mounts: []MountRequirement{cacheMount}},
			want:     WasmRuntimeConfig{Path: DefaultWasmPath},
		},
		{
			name:     "writable mount granted writable",
			requires: Requirements{Mounts: []MountRequirement{cacheMount}},
			grant:    Grant{Mounts: map[string]MountGrant{"cache": {HostPath: "./cache", Writable: true}}},
			want: WasmRuntimeConfig{Path: DefaultWasmPath, Mounts: []MountConfig{
				{HostPath: "./cache", GuestPath: "/cache", ReadOnly: new(false)},
			}},
		},
		{
			name:     "writable mount granted read only",
			requires: Requirements{Mounts: []MountRequirement{cacheMount}},
			grant:    Grant{Mounts: map[string]MountGrant{"cache": {HostPath: "./cache"}}},
			want: WasmRuntimeConfig{Path: DefaultWasmPath, Mounts: []MountConfig{
				{HostPath: "./cache", GuestPath: "/cache", ReadOnly: new(true)},
			}},
		},
		{
			name:     "read only mount granted writable",
			requires: Requirements{Mounts: []MountRequirement{dataMount}},
			grant:    Grant{Mounts: map[string]MountGrant{"data": {HostPath: "./data", Writable: true}}},
			errs:     []string{"only requests read access"},
		},
		{
			name:  "mount granted but not requested",
			grant: Grant{Mounts: map[string]MountGrant{"data": {HostPath: "./data"}}},
			errs:  []string{`mount "data" is granted but the plugin does not request it`},
		},
		{
			name:     "mount granted without host path",
			requires: Requirements{Mounts: []MountRequirement{dataMount}},
			grant:    Grant{Mounts: map[string]MountGrant{"data": {}}},
			errs:     []string{"host_path must be set"},
		},
		{
			name:     "mount requested twice",
			requires: Requirements{Mounts: []MountRequirement{dataMount, dataMount}},
			grant:    Grant{Mounts: map[string]MountGrant{"data": {HostPath: "./data"}}},
			errs:     []string{"requested more than once"},
		},
		{
			name: "two mounts at one guest path",
			requires: Requirements{Mounts: []MountRequirement{
				{Name: "a", GuestPath: "/data"},
				{Name: "b", GuestPath: "/data/"},
			}},
			grant: Grant{Mounts: map[string]MountGrant{"a": {HostPath: "./a"}, "b": {HostPath: "./b"}}},
			errs:  []string{`mounts "a" and "b" both use the guest_path /data/`},
		},
		{
			name:     "mount request without guest path",
			requires: Requirements{Mounts: []MountRequirement{{Name: "data"}}},
			errs:     []string{"need both a name and guest_path"},
		},
		{
			name:     "every problem is reported",
			requires: Requirements{Mounts: []MountRequirement{dataMount}},
			grant:    Grant{Mounts: map[string]MountGrant{"other": {HostPath: "./other"}}},
			errs:     []string{`mount "data" (/data) is required`, `mount "other" is granted`},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m := &Manifest{Name: "foo", Requires: test.requires}
			conf, warnings, err := Resolve(m, test.grant, ResolveOptions{})
			assert.Empty(t, warnings)
			if len(test.errs) > 0 {
				require.Error(t, err)
				for _, e := range test.errs {
					assert.Contains(t, err.Error(), e)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.want, conf)
		})
	}
}

func TestResolveLegacy(t *testing.T) {
	m, _, err := ReadManifestYAML([]byte(`
name: foo
type: processor
runtime:
  wasm:
    path: foo.wasm
    memory:
      max_pages: 128
    mounts:
      - host_path: ./data
        guest_path: /data
`))
	require.NoError(t, err)

	t.Run("allowed", func(t *testing.T) {
		conf, warnings, err := Resolve(m, Grant{}, ResolveOptions{AllowLegacy: true})
		require.NoError(t, err)
		assert.Equal(t, WasmRuntimeConfig{
			Path:   "foo.wasm",
			Memory: MemoryConfig{MaxPages: 128},
			Mounts: []MountConfig{{HostPath: "./data", GuestPath: "/data"}},
		}, conf)

		require.Len(t, warnings, 1)
		for _, s := range []string{
			"deprecated `runtime` block",
			LegacyMigrationURL,
			"Rename foo.wasm to plugin.wasm.",
			"guest_path: /data",
			"host_path: ./data",
			"max_pages: 128",
		} {
			assert.Contains(t, warnings[0], s)
		}
	})

	t.Run("not allowed", func(t *testing.T) {
		_, _, err := Resolve(m, Grant{}, ResolveOptions{})
		require.ErrorContains(t, err, "only supported for plugins loaded from a local directory")
	})

	t.Run("with grant", func(t *testing.T) {
		_, _, err := Resolve(m, Grant{Memory: MemoryConfig{MaxPages: 1}}, ResolveOptions{AllowLegacy: true})
		require.ErrorContains(t, err, "cannot be given mounts or memory limits")
	})

	t.Run("with requires", func(t *testing.T) {
		both := *m
		both.Requires.Memory.MinPages = 1
		_, _, err := Resolve(&both, Grant{}, ResolveOptions{AllowLegacy: true})
		require.ErrorContains(t, err, "both a `runtime` block and a `requires` block")
	})
}

func TestReadManifestRequires(t *testing.T) {
	m, lints, err := ReadManifestYAML([]byte(`
name: foo
type: processor
requires:
  memory:
    min_pages: 64
  mounts:
    - name: data
      guest_path: /data
      writable: true
      optional: true
      description: things
`))
	require.NoError(t, err)
	assert.Empty(t, lints)
	assert.Nil(t, m.Runtime)
	assert.Equal(t, Requirements{
		Memory: MemoryRequirement{MinPages: 64},
		Mounts: []MountRequirement{{Name: "data", GuestPath: "/data", Writable: true, Optional: true, Description: "things"}},
	}, m.Requires)
}
