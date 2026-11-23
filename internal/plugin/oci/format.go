// Package oci stores Bento WASM plugins as OCI artifacts, in a local store
// and in container registries.
//
// A plugin is an OCI image manifest with the artifact type ArtifactType, a
// config of ConfigMediaType holding the plugin's manifest as JSON, and a single
// layer holding its WASM binary.
package oci

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"gopkg.in/yaml.v3"
	"oras.land/oras-go/v2/content"

	"github.com/warpstreamlabs/bento/internal/plugin/runtime"
)

const (
	// ArtifactType identifies an OCI manifest as a Bento plugin.
	ArtifactType = "application/vnd.bento.plugin.v1"

	// ConfigMediaType is the media type of a plugin's config, its manifest
	// as JSON.
	ConfigMediaType = "application/vnd.bento.plugin.config.v1+json"

	// WasmMediaType is the media type of a plugin's WASM binary.
	WasmMediaType = "application/wasm"

	// AnnotationName is the annotation holding a plugin's name.
	AnnotationName = "vnd.bento.plugin.name"

	// AnnotationType is the annotation holding a plugin's component type.
	AnnotationType = "vnd.bento.plugin.type"

	// AnnotationABI is the annotation holding the ABI version a plugin was
	// built against.
	AnnotationABI = "vnd.bento.plugin.abi"
)

// Size limits for the parts of a plugin, to protect against huge or malicious
// artifacts.
const (
	MaxManifestSize = 4 << 20
	MaxConfigSize   = 4 << 20
	MaxWasmSize     = 256 << 20
)

// Plugin is a plugin read from an OCI store.
type Plugin struct {
	Descriptor ocispec.Descriptor
	Manifest   ocispec.Manifest

	// Config is the plugin's manifest as JSON.
	Config []byte

	// Wasm is the plugin's WASM binary, nil unless requested.
	Wasm []byte
}

// ConfigFromManifestYAML converts a plugin.yaml manifest to the JSON config of
// a plugin artifact. Manifests using the deprecated runtime block are refused,
// as they can't be published.
func ConfigFromManifestYAML(manifestYAML []byte) (*runtime.Manifest, []byte, error) {
	m, lints, err := runtime.ReadManifestYAML(manifestYAML)
	if err != nil {
		return nil, nil, err
	}
	if len(lints) > 0 {
		errs := make([]error, len(lints))
		for i, l := range lints {
			errs[i] = errors.New(l.Error())
		}
		return nil, nil, fmt.Errorf("plugin.yaml has errors:\n%w", errors.Join(errs...))
	}
	if m.Runtime != nil {
		return nil, nil, fmt.Errorf("plugin.yaml uses the deprecated `runtime` block, which can't be published; migrate it to `requires`: %v", runtime.LegacyMigrationURL)
	}

	var generic any
	if err := yaml.Unmarshal(manifestYAML, &generic); err != nil {
		return nil, nil, err
	}
	config, err := json.Marshal(generic)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert plugin.yaml to JSON: %w", err)
	}
	return m, config, nil
}

// ReadConfig parses the config of a plugin artifact. JSON is YAML, so this is
// the same as reading a plugin.yaml, except that the deprecated runtime block
// is refused.
func ReadConfig(config []byte) (*runtime.Manifest, []string, error) {
	m, lints, err := runtime.ReadManifestYAML(config)
	if err != nil {
		return nil, nil, err
	}
	if m.Runtime != nil {
		return nil, nil, errors.New("plugin config uses the deprecated `runtime` block, which isn't supported for published plugins")
	}
	strLints := make([]string, len(lints))
	for i, l := range lints {
		strLints[i] = l.Error()
	}
	return m, strLints, nil
}

// annotations returns the annotations of a plugin's OCI manifest.
func annotations(m *runtime.Manifest, abi int) map[string]string {
	return map[string]string{
		AnnotationName: m.Name,
		AnnotationType: m.Type,
		AnnotationABI:  strconv.Itoa(abi),
	}
}

// checkManifest parses an OCI manifest and checks it describes a plugin.
func checkManifest(desc ocispec.Descriptor, manifestBytes []byte) (ocispec.Manifest, error) {
	var m ocispec.Manifest

	switch desc.MediaType {
	case ocispec.MediaTypeImageManifest:
	case ocispec.MediaTypeImageIndex:
		return m, errors.New("not a Bento plugin: it is a multi-platform index, but plugins are a single manifest")
	default:
		return m, fmt.Errorf("not a Bento plugin: unexpected media type %q", desc.MediaType)
	}

	if err := json.Unmarshal(manifestBytes, &m); err != nil {
		return m, fmt.Errorf("failed to parse manifest: %w", err)
	}
	if m.ArtifactType != ArtifactType {
		return m, fmt.Errorf("not a Bento plugin: artifact type is %q, expected %q", m.ArtifactType, ArtifactType)
	}
	if m.Config.MediaType != ConfigMediaType {
		return m, fmt.Errorf("not a Bento plugin: config media type is %q, expected %q", m.Config.MediaType, ConfigMediaType)
	}
	if m.Config.Size > MaxConfigSize {
		return m, fmt.Errorf("plugin config is %v bytes, more than the limit of %v", m.Config.Size, MaxConfigSize)
	}
	if len(m.Layers) != 1 || m.Layers[0].MediaType != WasmMediaType {
		return m, fmt.Errorf("not a Bento plugin: expected exactly one layer of type %q", WasmMediaType)
	}
	if m.Layers[0].Size > MaxWasmSize {
		return m, fmt.Errorf("plugin wasm is %v bytes, more than the limit of %v", m.Layers[0].Size, MaxWasmSize)
	}
	return m, nil
}

// fetchManifest fetches and checks a plugin's OCI manifest.
func fetchManifest(ctx context.Context, src content.Fetcher, desc ocispec.Descriptor) (ocispec.Manifest, error) {
	if desc.Size > MaxManifestSize {
		return ocispec.Manifest{}, fmt.Errorf("manifest is %v bytes, more than the limit of %v", desc.Size, MaxManifestSize)
	}
	manifestBytes, err := content.FetchAll(ctx, src, desc)
	if err != nil {
		return ocispec.Manifest{}, fmt.Errorf("failed to fetch manifest: %w", err)
	}
	return checkManifest(desc, manifestBytes)
}

// readPlugin reads a plugin from src, checking it is a valid plugin artifact.
// Content is verified against its digest as it's read.
func readPlugin(ctx context.Context, src content.Fetcher, desc ocispec.Descriptor, withWasm bool) (*Plugin, error) {
	m, err := fetchManifest(ctx, src, desc)
	if err != nil {
		return nil, err
	}

	config, err := content.FetchAll(ctx, src, m.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch plugin config: %w", err)
	}

	p := &Plugin{Descriptor: desc, Manifest: m, Config: config}
	if withWasm {
		if p.Wasm, err = content.FetchAll(ctx, src, m.Layers[0]); err != nil {
			return nil, fmt.Errorf("failed to fetch plugin wasm: %w", err)
		}
	}
	return p, nil
}

// packManifest builds the OCI manifest of a plugin.
func packManifest(config, layer ocispec.Descriptor, annotations map[string]string) ([]byte, ocispec.Descriptor, error) {
	m := ocispec.Manifest{
		SchemaVersion: 2,
		MediaType:     ocispec.MediaTypeImageManifest,
		ArtifactType:  ArtifactType,
		Config:        config,
		Layers:        []ocispec.Descriptor{layer},
		Annotations:   annotations,
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, ocispec.Descriptor{}, err
	}
	return b, descriptorFor(ocispec.MediaTypeImageManifest, b), nil
}

func descriptorFor(mediaType string, b []byte) ocispec.Descriptor {
	return ocispec.Descriptor{
		MediaType: mediaType,
		Digest:    digest.FromBytes(b),
		Size:      int64(len(b)),
	}
}

func readerFor(b []byte) io.Reader {
	return bytes.NewReader(b)
}
