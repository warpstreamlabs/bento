package oci

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"oras.land/oras-go/v2"
	"oras.land/oras-go/v2/content/memory"
)

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

const testManifest = `name: uppercase
type: processor
summary: Uppercases things.
fields:
  - name: prefix
    type: string
    default: ""
requires:
  mounts:
    - name: data
      guest_path: /data
`

func writePlugin(t *testing.T, manifest string, wasm []byte) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "plugin.yaml"), []byte(manifest), 0o644))
	if wasm != nil {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "plugin.wasm"), wasm, 0o644))
	}
	return dir
}

// fakeRegistry is an in-memory registry that counts how often it's used.
type fakeRegistry struct {
	repos map[string]*memory.Store
	calls int
}

func newFakeRegistry() *fakeRegistry {
	return &fakeRegistry{repos: map[string]*memory.Store{}}
}

func (r *fakeRegistry) remote(_ context.Context, ref Reference) (oras.Target, error) {
	r.calls++
	repo, exists := r.repos[ref.Repo()]
	if !exists {
		repo = memory.New()
		r.repos[ref.Repo()] = repo
	}
	return repo, nil
}

func openStore(t *testing.T, reg *fakeRegistry) *Store {
	t.Helper()
	var remote RemoteFunc
	if reg != nil {
		remote = reg.remote
	}
	s, err := OpenStore(t.Context(), t.TempDir(), remote)
	require.NoError(t, err)
	return s
}

func TestBuildAndGet(t *testing.T) {
	t.Setenv("SOURCE_DATE_EPOCH", "1700000000")
	store := openStore(t, nil)
	wasm := wasmModule("bento_abi_v1")
	dir := writePlugin(t, testManifest, wasm)

	res, err := store.Build(t.Context(), dir, []string{"ghcr.io/me/uppercase:1.0.0", "ghcr.io/me/uppercase"}, map[string]string{
		ocispec.AnnotationVersion: "1.0.0",
	})
	require.NoError(t, err)
	assert.Equal(t, "uppercase", res.Name)
	assert.Empty(t, res.Warnings)

	p, err := store.Get(t.Context(), "oci://ghcr.io/me/uppercase:latest", true)
	require.NoError(t, err)
	assert.Equal(t, res.Descriptor.Digest, p.Descriptor.Digest)
	assert.Equal(t, wasm, p.Wasm)

	// The manifest follows the documented format.
	assert.Equal(t, ArtifactType, p.Manifest.ArtifactType)
	assert.Equal(t, ConfigMediaType, p.Manifest.Config.MediaType)
	require.Len(t, p.Manifest.Layers, 1)
	assert.Equal(t, WasmMediaType, p.Manifest.Layers[0].MediaType)
	assert.Equal(t, "plugin.wasm", p.Manifest.Layers[0].Annotations[ocispec.AnnotationTitle])
	assert.Equal(t, map[string]string{
		AnnotationName:            "uppercase",
		AnnotationType:            "processor",
		AnnotationABI:             "1",
		ocispec.AnnotationVersion: "1.0.0",
		ocispec.AnnotationCreated: "2023-11-14T22:13:20Z",
	}, p.Manifest.Annotations)

	// The config is the manifest as JSON.
	var config map[string]any
	require.NoError(t, json.Unmarshal(p.Config, &config))
	assert.Equal(t, "uppercase", config["name"])
	assert.Contains(t, config, "requires")

	m, lints, err := ReadConfig(p.Config)
	require.NoError(t, err)
	assert.Empty(t, lints)
	assert.Equal(t, "/data", m.Requires.Mounts[0].GuestPath)

	// Builds are reproducible.
	again, err := store.Build(t.Context(), dir, nil, map[string]string{ocispec.AnnotationVersion: "1.0.0"})
	require.NoError(t, err)
	assert.Equal(t, res.Descriptor.Digest, again.Descriptor.Digest)

	// By digest.
	_, err = store.Get(t.Context(), "ghcr.io/me/uppercase@"+res.Descriptor.Digest.String(), false)
	require.NoError(t, err)
}

func TestBuildErrors(t *testing.T) {
	v1 := wasmModule("bento_abi_v1")
	tests := []struct {
		name     string
		manifest string
		wasm     []byte
		tags     []string
		err      string
	}{
		{name: "legacy runtime", manifest: "name: a\ntype: processor\nruntime:\n  wasm: {}\n", wasm: v1, err: "deprecated `runtime` block"},
		{name: "lints", manifest: "name: a\ntype: input\n", wasm: v1, err: "plugin.yaml has errors"},
		{name: "missing wasm", manifest: testManifest, err: "failed to read plugin.wasm"},
		{name: "bad abi", manifest: testManifest, wasm: wasmModule("bento_abi_v7"), err: "built for ABI v7"},
		{name: "no registry", manifest: testManifest, wasm: v1, tags: []string{"uppercase:1"}, err: "invalid plugin reference"},
		{name: "digest tag", manifest: testManifest, wasm: v1, tags: []string{"ghcr.io/a/b@sha256:" + string(bytes.Repeat([]byte("a"), 64))}, err: "a tag is needed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := openStore(t, nil)
			_, err := store.Build(t.Context(), writePlugin(t, test.manifest, test.wasm), test.tags, nil)
			require.ErrorContains(t, err, test.err)
		})
	}
}

func TestBuildWarnsWithoutABI(t *testing.T) {
	store := openStore(t, nil)
	res, err := store.Build(t.Context(), writePlugin(t, testManifest, wasmModule()), nil, nil)
	require.NoError(t, err)
	require.Len(t, res.Warnings, 1)
	assert.Contains(t, res.Warnings[0], "does not declare an ABI version")
}

func TestPushPullFetch(t *testing.T) {
	reg := newFakeRegistry()
	const ref = "ghcr.io/me/uppercase:1.0.0"
	wasm := wasmModule("bento_abi_v1")

	builder := openStore(t, reg)
	res, err := builder.Build(t.Context(), writePlugin(t, testManifest, wasm), []string{ref}, nil)
	require.NoError(t, err)
	_, err = builder.Push(t.Context(), ref)
	require.NoError(t, err)

	// Registries resolve digests, but the in-memory store only resolves tags.
	parsed, err := ParseReference(ref)
	require.NoError(t, err)
	repo, err := reg.remote(t.Context(), parsed)
	require.NoError(t, err)
	require.NoError(t, repo.Tag(t.Context(), res.Descriptor, res.Descriptor.Digest.String()))

	t.Run("never", func(t *testing.T) {
		store := openStore(t, reg)
		_, _, err := store.Fetch(t.Context(), "oci://"+ref, PullNever)
		require.ErrorContains(t, err, "is not in the local store")
		require.ErrorContains(t, err, "bento plugin pull")
	})

	t.Run("missing", func(t *testing.T) {
		store := openStore(t, reg)
		config, gotWasm, err := store.Fetch(t.Context(), "oci://"+ref, PullMissing)
		require.NoError(t, err)
		assert.Equal(t, wasm, gotWasm)
		assert.Contains(t, string(config), `"name":"uppercase"`)

		// Stored now, so no registry calls.
		calls := reg.calls
		_, _, err = store.Fetch(t.Context(), "oci://"+ref, PullMissing)
		require.NoError(t, err)
		_, _, err = store.Fetch(t.Context(), "oci://"+ref, PullNever)
		require.NoError(t, err)
		assert.Equal(t, calls, reg.calls)

		// Always pulls tags again.
		_, _, err = store.Fetch(t.Context(), "oci://"+ref, PullAlways)
		require.NoError(t, err)
		assert.Greater(t, reg.calls, calls)
	})

	t.Run("digest", func(t *testing.T) {
		store := openStore(t, reg)
		digestRef := "oci://ghcr.io/me/uppercase@" + res.Descriptor.Digest.String()
		_, gotWasm, err := store.Fetch(t.Context(), digestRef, PullMissing)
		require.NoError(t, err)
		assert.Equal(t, wasm, gotWasm)

		// Content at a digest can't change, so even always doesn't pull it
		// again.
		calls := reg.calls
		_, _, err = store.Fetch(t.Context(), digestRef, PullAlways)
		require.NoError(t, err)
		assert.Equal(t, calls, reg.calls)
	})

	t.Run("unknown", func(t *testing.T) {
		store := openStore(t, reg)
		_, err := store.Pull(t.Context(), "ghcr.io/me/nope:1")
		require.ErrorContains(t, err, "failed to find")
	})
}

func TestPullRefusesOtherArtifacts(t *testing.T) {
	reg := newFakeRegistry()
	ref, err := ParseReference("ghcr.io/me/image:1")
	require.NoError(t, err)
	target, err := reg.remote(t.Context(), ref)
	require.NoError(t, err)

	desc, err := oras.PackManifest(t.Context(), target, oras.PackManifestVersion1_1, "application/vnd.something.else", oras.PackManifestOptions{})
	require.NoError(t, err)
	require.NoError(t, target.Tag(t.Context(), desc, "1"))

	store := openStore(t, reg)
	_, err = store.Pull(t.Context(), "ghcr.io/me/image:1")
	require.ErrorContains(t, err, "not a Bento plugin")

	entries, err := store.List(t.Context())
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestTagRemovePrune(t *testing.T) {
	store := openStore(t, nil)
	res, err := store.Build(t.Context(), writePlugin(t, testManifest, wasmModule("bento_abi_v1")), []string{"ghcr.io/me/a:1"}, nil)
	require.NoError(t, err)
	require.NoError(t, store.Tag(t.Context(), "ghcr.io/me/a:1", "ghcr.io/me/b"))

	entries, err := store.List(t.Context())
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, "ghcr.io/me/a:1", entries[0].Tag)
	assert.Equal(t, "ghcr.io/me/b:latest", entries[1].Tag)
	assert.Equal(t, entries[0].Descriptor.Digest, entries[1].Descriptor.Digest)

	// Removing one tag keeps the plugin for the other.
	require.NoError(t, store.Remove(t.Context(), "ghcr.io/me/a:1"))
	_, err = store.Get(t.Context(), "ghcr.io/me/b", true)
	require.NoError(t, err)

	// Removing the last tag removes the plugin.
	require.NoError(t, store.Remove(t.Context(), "ghcr.io/me/b:latest"))
	_, err = store.Get(t.Context(), "ghcr.io/me/b@"+res.Descriptor.Digest.String(), false)
	require.ErrorContains(t, err, "not in the local store")

	// Untagged builds are pruned.
	untagged, err := store.Build(t.Context(), writePlugin(t, testManifest, wasmModule("bento_abi_v1", "x")), nil, nil)
	require.NoError(t, err)
	removed, err := store.Prune(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []string{untagged.Descriptor.Digest.String()}, removed)

	blobs, err := os.ReadDir(filepath.Join(store.Dir(), "blobs", "sha256"))
	require.NoError(t, err)
	assert.Empty(t, blobs)
}

func TestSaveLoad(t *testing.T) {
	src := openStore(t, nil)
	wasm := wasmModule("bento_abi_v1")
	_, err := src.Build(t.Context(), writePlugin(t, testManifest, wasm), []string{"ghcr.io/me/a:1", "ghcr.io/me/b:2"}, nil)
	require.NoError(t, err)

	archive := filepath.Join(t.TempDir(), "plugins.tar")
	f, err := os.Create(archive)
	require.NoError(t, err)
	require.NoError(t, src.Save(t.Context(), []string{"ghcr.io/me/a:1", "ghcr.io/me/b:2"}, f))
	require.NoError(t, f.Close())

	dst := openStore(t, nil)
	tags, err := dst.Load(t.Context(), archive)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"ghcr.io/me/a:1", "ghcr.io/me/b:2"}, tags)

	_, gotWasm, err := dst.Fetch(t.Context(), "ghcr.io/me/b:2", PullNever)
	require.NoError(t, err)
	assert.Equal(t, wasm, gotWasm)
}

func TestParseReference(t *testing.T) {
	tests := map[string]string{
		"ghcr.io/me/a":            "ghcr.io/me/a:latest",
		"oci://ghcr.io/me/a:1.0":  "ghcr.io/me/a:1.0",
		"localhost:5000/a:dev":    "localhost:5000/a:dev",
		"docker.io/library/foo:1": "docker.io/library/foo:1",
	}
	for in, want := range tests {
		ref, err := ParseReference(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, ref.String(), in)
	}

	for _, bad := range []string{"a", "a:1", "", "oci://", "Ghcr.io/ME/a:1"} {
		_, err := ParseReference(bad)
		assert.Error(t, err, bad)
	}

	assert.True(t, isLocalRegistry("localhost:5000"))
	assert.True(t, isLocalRegistry("127.0.0.1:5000"))
	assert.True(t, isLocalRegistry("[::1]:5000"))
	assert.False(t, isLocalRegistry("ghcr.io"))
}

func TestConcurrentStores(t *testing.T) {
	// Two handles on one directory, as two processes would have. Without the
	// lock and index reloads, they overwrite each other's tags.
	dir := t.TempDir()
	a, err := OpenStore(t.Context(), dir, nil)
	require.NoError(t, err)
	b, err := OpenStore(t.Context(), dir, nil)
	require.NoError(t, err)

	plugin := writePlugin(t, testManifest, wasmModule("bento_abi_v1"))

	const n = 20
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			store := a
			if i%2 == 1 {
				store = b
			}
			_, err := store.Build(t.Context(), plugin, []string{fmt.Sprintf("ghcr.io/me/p:%d", i)}, nil)
			assert.NoError(t, err)
		})
	}
	wg.Wait()

	entries, err := a.List(t.Context())
	require.NoError(t, err)
	assert.Len(t, entries, n)
}
