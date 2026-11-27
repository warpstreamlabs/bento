package oci

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/content/oci"
	"oras.land/oras-go/v2/errdef"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/errcode"

	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
	"github.com/warpstreamlabs/bento/internal/plugin/runtime"
)

// PullPolicy decides when plugins are pulled from their registry, with the
// same meaning as docker run --pull.
type PullPolicy string

const (
	// PullMissing pulls a plugin only if it isn't in the local store.
	PullMissing PullPolicy = "missing"
	// PullAlways pulls a plugin every time, unless it's referenced by digest
	// and already stored.
	PullAlways PullPolicy = "always"
	// PullNever only uses plugins already in the local store.
	PullNever PullPolicy = "never"
)

// ParsePullPolicy parses a pull policy.
func ParsePullPolicy(s string) (PullPolicy, error) {
	switch p := PullPolicy(s); p {
	case PullMissing, PullAlways, PullNever:
		return p, nil
	case "":
		return PullMissing, nil
	}
	return "", fmt.Errorf("invalid plugin pull policy %q, expected missing, always or never", s)
}

// EnvStoreDir is the environment variable that sets the local store's
// directory.
const EnvStoreDir = "BENTO_PLUGIN_STORE"

// DefaultStoreDir returns the directory of the local plugin store when none
// is set.
func DefaultStoreDir() string {
	if dir := os.Getenv(EnvStoreDir); dir != "" {
		return dir
	}
	if dir, err := os.UserCacheDir(); err == nil {
		return filepath.Join(dir, "bento", "plugins")
	}
	return filepath.Join(os.TempDir(), "bento", "plugins")
}

// RemoteFunc returns the registry repository a reference refers to.
type RemoteFunc func(ctx context.Context, ref Reference) (oras.Target, error)

// Store is a local store of plugins, kept as an OCI image layout directory
// that other OCI tools can read. Tags are full references, such as
// ghcr.io/org/plugin:1.0.0.
//
// Processes sharing a store take a file lock on it, shared to read and
// exclusive to change it, and reload its index each time, so they see each
// other's changes and don't overwrite them.
type Store struct {
	dir    string
	remote RemoteFunc

	mu  sync.Mutex // serialises use of the store within this process
	oci *oci.Store // valid only while locked
}

// OpenStore opens the local store in dir, creating it if needed. Registries
// are reached with remote, which may be nil to disable pushing and pulling.
func OpenStore(ctx context.Context, dir string, remote RemoteFunc) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create plugin store: %w", err)
	}
	s := &Store{dir: dir, remote: remote}
	// Check the store can be opened, which creates its layout if it's new.
	if err := s.locked(ctx, false, func() error { return nil }); err != nil {
		return nil, err
	}
	return s, nil
}

// locked runs fn with the store locked, and its index freshly loaded.
func (s *Store) locked(ctx context.Context, exclusive bool, fn func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	unlock, err := lockStore(s.dir, exclusive)
	if err != nil {
		return err
	}
	defer unlock()

	if s.oci, err = oci.NewWithContext(ctx, s.dir); err != nil {
		return fmt.Errorf("failed to open plugin store %v: %w", s.dir, err)
	}
	return fn()
}

// Dir returns the directory of the store.
func (s *Store) Dir() string {
	return s.dir
}

//------------------------------------------------------------------------------

// BuildResult is the outcome of building a plugin.
type BuildResult struct {
	Descriptor ocispec.Descriptor
	Name       string
	Warnings   []string
}

// Build checks the plugin in dir and stores it, tagged with each of tags.
// Extra annotations, such as org.opencontainers.image.version, are added to
// the plugin's manifest. If SOURCE_DATE_EPOCH is set it is used as the
// creation time, so that builds are reproducible.
func (s *Store) Build(ctx context.Context, dir string, tags []string, extra map[string]string) (*BuildResult, error) {
	refs, err := parseAll(tags)
	if err != nil {
		return nil, err
	}

	manifestYAML, err := ifs.ReadFile(ifs.OS(), filepath.Join(dir, "plugin.yaml"))
	if err != nil {
		return nil, fmt.Errorf("failed to read plugin.yaml: %w", err)
	}
	manifest, config, err := ConfigFromManifestYAML(manifestYAML)
	if err != nil {
		return nil, err
	}

	wasm, err := ifs.ReadFile(ifs.OS(), filepath.Join(dir, runtime.DefaultWasmPath))
	if err != nil {
		return nil, fmt.Errorf("failed to read %v: %w", runtime.DefaultWasmPath, err)
	}
	if len(wasm) > MaxWasmSize {
		return nil, fmt.Errorf("%v is %v bytes, more than the limit of %v", runtime.DefaultWasmPath, len(wasm), MaxWasmSize)
	}
	abi, warnings, err := runtime.CheckABI(wasm)
	if err != nil {
		return nil, err
	}

	created := time.Now().UTC()
	if epoch := os.Getenv("SOURCE_DATE_EPOCH"); epoch != "" {
		secs, err := strconv.ParseInt(epoch, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid SOURCE_DATE_EPOCH: %w", err)
		}
		created = time.Unix(secs, 0).UTC()
	}

	ann := annotations(manifest, abi)
	ann[ocispec.AnnotationCreated] = created.Format(time.RFC3339)
	maps.Copy(ann, extra)

	configDesc := descriptorFor(ConfigMediaType, config)
	layerDesc := descriptorFor(WasmMediaType, wasm)
	layerDesc.Annotations = map[string]string{ocispec.AnnotationTitle: runtime.DefaultWasmPath}

	manifestBytes, desc, err := packManifest(configDesc, layerDesc, ann)
	if err != nil {
		return nil, err
	}

	err = s.locked(ctx, true, func() error {
		for _, blob := range []struct {
			desc ocispec.Descriptor
			b    []byte
		}{{configDesc, config}, {layerDesc, wasm}, {desc, manifestBytes}} {
			if err := s.pushBlob(ctx, blob.desc, blob.b); err != nil {
				return err
			}
		}
		for _, ref := range refs {
			if err := s.oci.Tag(ctx, desc, ref.String()); err != nil {
				return fmt.Errorf("failed to tag %v: %w", ref, err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &BuildResult{Descriptor: desc, Name: manifest.Name, Warnings: warnings}, nil
}

func (s *Store) pushBlob(ctx context.Context, desc ocispec.Descriptor, b []byte) error {
	err := s.oci.Push(ctx, desc, readerFor(b))
	if err != nil && !errors.Is(err, errdef.ErrAlreadyExists) {
		return fmt.Errorf("failed to store blob: %w", err)
	}
	return nil
}

func parseAll(refs []string) ([]Reference, error) {
	parsed := make([]Reference, len(refs))
	for i, r := range refs {
		ref, err := ParseReference(r)
		if err != nil {
			return nil, err
		}
		if ref.IsDigest() {
			return nil, fmt.Errorf("%v: a tag is needed, not a digest", r)
		}
		parsed[i] = ref
	}
	return parsed, nil
}

//------------------------------------------------------------------------------

// resolve finds a reference in the store.
func (s *Store) resolve(ctx context.Context, ref Reference) (ocispec.Descriptor, error) {
	desc, err := s.oci.Resolve(ctx, ref.String())
	if err == nil {
		return desc, nil
	}
	if ref.IsDigest() {
		if desc, err = s.oci.Resolve(ctx, ref.Reference.Reference); err == nil {
			return desc, nil
		}
	}
	if errors.Is(err, errdef.ErrNotFound) {
		return desc, fmt.Errorf("plugin %v is not in the local store: %w", ref, errdef.ErrNotFound)
	}
	return desc, err
}

// Get reads a plugin from the store. The WASM binary is only read if withWasm
// is true.
func (s *Store) Get(ctx context.Context, reference string, withWasm bool) (p *Plugin, err error) {
	ref, err := ParseReference(reference)
	if err != nil {
		return nil, err
	}
	err = s.locked(ctx, false, func() error {
		desc, err := s.resolve(ctx, ref)
		if err != nil {
			return err
		}
		p, err = readPlugin(ctx, s.oci, desc, withWasm)
		return err
	})
	return p, err
}

// Fetch returns the config and WASM binary of a plugin, pulling it from its
// registry first according to policy.
func (s *Store) Fetch(ctx context.Context, reference string, policy PullPolicy) (config, wasm []byte, err error) {
	ref, err := ParseReference(reference)
	if err != nil {
		return nil, nil, err
	}

	var resolveErr error
	if err := s.locked(ctx, false, func() error {
		_, resolveErr = s.resolve(ctx, ref)
		return nil
	}); err != nil {
		return nil, nil, err
	}
	stored := resolveErr == nil
	if resolveErr != nil && !errors.Is(resolveErr, errdef.ErrNotFound) {
		return nil, nil, resolveErr
	}

	switch {
	case policy == PullNever && !stored:
		return nil, nil, fmt.Errorf("plugin %v is not in the local store %v and pulling is disabled; run `bento plugin pull %v` first", ref, s.dir, ref)
	case !stored, policy == PullAlways && !ref.IsDigest():
		if _, err := s.Pull(ctx, ref.String()); err != nil {
			return nil, nil, err
		}
	}

	p, err := s.Get(ctx, ref.String(), true)
	if err != nil {
		return nil, nil, err
	}
	return p.Config, p.Wasm, nil
}

//------------------------------------------------------------------------------

func (s *Store) repository(ctx context.Context, ref Reference) (oras.Target, error) {
	if s.remote == nil {
		return nil, errors.New("registry access is not enabled")
	}
	return s.remote(ctx, ref)
}

// Pull copies a plugin from its registry into the store. The plugin is
// checked before its content is downloaded.
func (s *Store) Pull(ctx context.Context, reference string) (desc ocispec.Descriptor, err error) {
	ref, err := ParseReference(reference)
	if err != nil {
		return ocispec.Descriptor{}, err
	}
	err = s.locked(ctx, true, func() error {
		desc, err = s.pull(ctx, ref)
		return err
	})
	return desc, err
}

func (s *Store) pull(ctx context.Context, ref Reference) (ocispec.Descriptor, error) {
	repo, err := s.repository(ctx, ref)
	if err != nil {
		return ocispec.Descriptor{}, err
	}

	desc, err := repo.Resolve(ctx, ref.Reference.Reference)
	if err != nil {
		return desc, withLoginHint(ref, fmt.Errorf("failed to find %v: %w", ref, err))
	}
	if _, err := fetchManifest(ctx, repo, desc); err != nil {
		return desc, fmt.Errorf("%v: %w", ref, err)
	}

	if err := oras.CopyGraph(ctx, repo, s.oci, desc, oras.DefaultCopyGraphOptions); err != nil {
		return desc, withLoginHint(ref, fmt.Errorf("failed to pull %v: %w", ref, err))
	}
	if err := s.oci.Tag(ctx, desc, ref.String()); err != nil {
		return desc, fmt.Errorf("failed to tag %v: %w", ref, err)
	}
	return desc, nil
}

// withLoginHint adds a hint to log in to errors caused by missing or rejected
// credentials.
func withLoginHint(ref Reference, err error) error {
	var resp *errcode.ErrorResponse
	if errors.Is(err, auth.ErrBasicCredentialNotFound) ||
		(errors.As(err, &resp) && (resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden)) {
		return fmt.Errorf("%w\nlog in with `bento plugin login %v` or `docker login %v`", err, ref.Registry, ref.Registry)
	}
	return err
}

// Push copies a plugin from the store to its registry.
func (s *Store) Push(ctx context.Context, reference string) (ocispec.Descriptor, error) {
	ref, err := ParseReference(reference)
	if err != nil {
		return ocispec.Descriptor{}, err
	}
	if ref.IsDigest() {
		return ocispec.Descriptor{}, fmt.Errorf("%v: a tag is needed to push, not a digest", ref)
	}

	var desc ocispec.Descriptor
	err = s.locked(ctx, false, func() error {
		if desc, err = s.resolve(ctx, ref); err != nil {
			return err
		}
		if _, err := readPlugin(ctx, s.oci, desc, false); err != nil {
			return err
		}
		repo, err := s.repository(ctx, ref)
		if err != nil {
			return err
		}
		if _, err := oras.Copy(ctx, s.oci, ref.String(), repo, ref.Reference.Reference, oras.DefaultCopyOptions); err != nil {
			return withLoginHint(ref, fmt.Errorf("failed to push %v: %w", ref, err))
		}
		return nil
	})
	return desc, err
}

//------------------------------------------------------------------------------

// Tag adds the tag target to the plugin source refers to.
func (s *Store) Tag(ctx context.Context, source, target string) error {
	src, err := ParseReference(source)
	if err != nil {
		return err
	}
	refs, err := parseAll([]string{target})
	if err != nil {
		return err
	}
	return s.locked(ctx, true, func() error {
		desc, err := s.resolve(ctx, src)
		if err != nil {
			return err
		}
		return s.oci.Tag(ctx, desc, refs[0].String())
	})
}

// Remove removes a tag from the store, and the plugin it refers to if no
// other tags do.
func (s *Store) Remove(ctx context.Context, reference string) error {
	ref, err := ParseReference(reference)
	if err != nil {
		return err
	}
	return s.locked(ctx, true, func() error { return s.remove(ctx, ref) })
}

func (s *Store) remove(ctx context.Context, ref Reference) error {
	desc, err := s.resolve(ctx, ref)
	if err != nil {
		return err
	}

	if !ref.IsDigest() {
		if err := s.oci.Untag(ctx, ref.String()); err != nil {
			return err
		}
	} else {
		// Remove every tag of the digest.
		entries, err := s.list(ctx)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if e.Descriptor.Digest == desc.Digest {
				if err := s.oci.Untag(ctx, e.Tag); err != nil {
					return err
				}
			}
		}
	}

	stillTagged, err := s.isTagged(ctx, desc)
	if err != nil || stillTagged {
		return err
	}
	if err := s.oci.Delete(ctx, desc); err != nil && !errors.Is(err, errdef.ErrNotFound) {
		return err
	}
	return nil
}

func (s *Store) isTagged(ctx context.Context, desc ocispec.Descriptor) (bool, error) {
	entries, err := s.list(ctx)
	if err != nil {
		return false, err
	}
	for _, e := range entries {
		if e.Descriptor.Digest == desc.Digest {
			return true, nil
		}
	}
	return false, nil
}

// Prune removes plugins that have no tags, and any content they leave
// unused. It returns the digests of the removed plugins.
func (s *Store) Prune(ctx context.Context) (removed []string, err error) {
	err = s.locked(ctx, true, func() error {
		removed, err = s.prune(ctx)
		return err
	})
	return removed, err
}

// prune finds untagged manifests from the store's index.json, which the OCI
// image layout spec defines, as oras doesn't list them.
func (s *Store) prune(ctx context.Context) ([]string, error) {
	if err := s.oci.SaveIndex(); err != nil {
		return nil, err
	}
	index, err := s.readIndex()
	if err != nil {
		return nil, err
	}

	var removed []string
	for _, desc := range index.Manifests {
		if _, tagged := desc.Annotations[ocispec.AnnotationRefName]; tagged {
			continue
		}
		isTagged, err := s.isTagged(ctx, desc)
		if err != nil {
			return removed, err
		}
		if isTagged {
			continue
		}
		if err := s.oci.Delete(ctx, desc); err != nil && !errors.Is(err, errdef.ErrNotFound) {
			return removed, err
		}
		removed = append(removed, desc.Digest.String())
	}
	return removed, s.oci.GC(ctx)
}

func (s *Store) readIndex() (*ocispec.Index, error) {
	b, err := os.ReadFile(filepath.Join(s.dir, ocispec.ImageIndexFile))
	if err != nil {
		return nil, err
	}
	var index ocispec.Index
	if err := json.Unmarshal(b, &index); err != nil {
		return nil, fmt.Errorf("failed to read store index: %w", err)
	}
	return &index, nil
}

//------------------------------------------------------------------------------

// Entry is a tagged plugin in the store.
type Entry struct {
	Tag        string
	Descriptor ocispec.Descriptor
	Manifest   ocispec.Manifest
}

// Size returns the total size of the plugin's content.
func (e Entry) Size() int64 {
	return e.Descriptor.Size + e.Manifest.Config.Size + e.Manifest.Layers[0].Size
}

// List returns the tagged plugins in the store, sorted by tag. Tagged
// artifacts that aren't plugins are skipped.
func (s *Store) List(ctx context.Context) (entries []Entry, err error) {
	err = s.locked(ctx, false, func() error {
		entries, err = s.list(ctx)
		return err
	})
	return entries, err
}

func (s *Store) list(ctx context.Context) ([]Entry, error) {
	var tags []string
	if err := s.oci.Tags(ctx, "", func(t []string) error {
		tags = append(tags, t...)
		return nil
	}); err != nil {
		return nil, err
	}

	entries := make([]Entry, 0, len(tags))
	for _, tag := range tags {
		desc, err := s.oci.Resolve(ctx, tag)
		if err != nil {
			return nil, err
		}
		m, err := fetchManifest(ctx, s.oci, desc)
		if err != nil {
			continue
		}
		entries = append(entries, Entry{Tag: tag, Descriptor: desc, Manifest: m})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Tag < entries[j].Tag })
	return entries, nil
}

var _ content.ReadOnlyStorage = (*oci.Store)(nil)
