// Package jsonschema centralises the JSON Schema implementation used across
// Bento components, so that schema compilation, instance normalisation and
// error rendering stay consistent between them.
package jsonschema

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"net/url"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// Schema is a compiled JSON Schema definition.
type Schema = jsonschema.Schema

// Compiler accumulates schema documents before compiling them.
type Compiler = jsonschema.Compiler

// registryBase is the synthetic base for schemas whose references are resolved
// by name rather than by location, as with a schema registry.
const registryBase = "registry:///"

// inlineURL names schemas supplied literally in a config rather than loaded
// from a path. It serves as the base for relative references and is stripped
// from validation errors so it never reaches users.
const inlineURL = "inline://schema.json"

// remoteTimeout bounds fetching a schema over HTTP. The previous
// implementation used the default client, which has no timeout at all.
const remoteTimeout = time.Second * 30

func newCompiler() *jsonschema.Compiler {
	c := jsonschema.NewCompiler()

	// Drafts 2019-09 and later treat `format` as an annotation unless asserted,
	// whereas the previous implementation asserted it for every draft.
	c.AssertFormat()

	return c
}

// CompileString compiles a schema supplied inline. An inline schema has no
// location to resolve relative references against, but absolute ones are
// resolved on the same terms as CompilePath so that both config fields of a
// component behave alike.
func CompileString(schema string) (*Schema, error) {
	doc, err := jsonschema.UnmarshalJSON(strings.NewReader(schema))
	if err != nil {
		return nil, err
	}

	c := newCompiler()
	c.UseLoader(remoteOnlyLoader())

	if err := c.AddResource(inlineURL, doc); err != nil {
		return nil, err
	}
	return c.Compile(inlineURL)
}

// CompilePath compiles a schema from a file:// or http(s):// location. Files
// are read through the provided filesystem, and remote references are only
// resolved for the schemes registered here.
func CompilePath(path string, f fs.FS) (*Schema, error) {
	remote := &httpLoader{client: &http.Client{Timeout: remoteTimeout}}

	c := newCompiler()
	c.UseLoader(jsonschema.SchemeURLLoader{
		"file":  fileLoader{fs: f},
		"http":  remote,
		"https": remote,
	})
	return c.Compile(path)
}

// remoteOnlyLoader resolves absolute http(s) references but no file ones, for
// schemas that did not come from a path on disk.
func remoteOnlyLoader() jsonschema.SchemeURLLoader {
	remote := &httpLoader{client: &http.Client{Timeout: remoteTimeout}}
	return jsonschema.SchemeURLLoader{
		"http":  remote,
		"https": remote,
	}
}

// RegistryURL names a registry-supplied schema. Relative references within a
// registry schema resolve against this base, so they match sibling reference
// names rather than paths on the host running Bento.
func RegistryURL(name string) string {
	return registryBase + name
}

// NewRegistryCompiler returns a compiler for registry-supplied schemas. It
// keeps the default file-only loader, so a reference the registry did not
// supply fails to compile rather than being fetched over the network.
func NewRegistryCompiler() *Compiler {
	return newCompiler()
}

// AddResourceString registers a schema document under a URL.
func AddResourceString(c *Compiler, url, schema string) error {
	doc, err := jsonschema.UnmarshalJSON(strings.NewReader(schema))
	if err != nil {
		return err
	}
	return c.AddResource(url, doc)
}

// UnmarshalBytes decodes a JSON document for validation, preserving numbers
// exactly as written.
func UnmarshalBytes(b []byte) (any, error) {
	return jsonschema.UnmarshalJSON(bytes.NewReader(b))
}

// Normalise converts a value into the types the validator accepts. The previous
// implementation round-tripped every instance through encoding/json, which
// rendered []byte as base64 and time.Time as RFC3339. Both remain reachable
// here as Bloblang values, so the round trip is preserved rather than dropped.
func Normalise(v any) (any, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return jsonschema.UnmarshalJSON(bytes.NewReader(b))
}

// Validate checks a value against a schema, normalising it first.
func Validate(s *Schema, v any) error {
	doc, err := Normalise(v)
	if err != nil {
		return err
	}
	return FormatError(s.Validate(doc))
}

// FormatError rewrites a validation failure for display. The first line of the
// underlying message names the schema resource, which for inline schemas is a
// synthetic URL, so it is dropped and the tree of causes beneath it kept.
func FormatError(err error) error {
	var verr *jsonschema.ValidationError
	if !errors.As(err, &verr) {
		return err
	}

	lines := strings.Split(verr.Error(), "\n")
	if len(lines) < 2 {
		return err
	}
	return errors.New(strings.Join(lines[1:], "\n"))
}

// fileLoader resolves file:// URLs through a Bento filesystem.
type fileLoader struct {
	fs fs.FS
}

func (l fileLoader) Load(rawURL string) (any, error) {
	// Deliberately not url.Parse: a scheme-relative location such as
	// file://../schema.json parses `..` as the host and loses path segments.
	// The previous implementation trimmed the prefix verbatim and existing
	// configs depend on that.
	path := strings.TrimPrefix(rawURL, "file://")

	path, err := url.QueryUnescape(path)
	if err != nil {
		return nil, err
	}

	if runtime.GOOS == "windows" {
		path = strings.TrimPrefix(path, "/")
		path = filepath.FromSlash(path)
	}

	file, err := l.fs.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	return jsonschema.UnmarshalJSON(file)
}

// httpLoader resolves http(s):// URLs with a bounded timeout.
type httpLoader struct {
	client *http.Client
}

func (l *httpLoader) Load(rawURL string) (any, error) {
	resp, err := l.client.Get(rawURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, errors.New("failed to fetch schema: " + resp.Status)
	}
	return jsonschema.UnmarshalJSON(resp.Body)
}
