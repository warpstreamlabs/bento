package jsonschema

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
)

// A schema reaching a sibling document by absolute URL must compile whether it
// was supplied inline or loaded from a path. The two entry points install their
// loaders separately, so this guards against them drifting apart.
func TestCompileResolvesAbsoluteRefs(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{
  "$id": %q,
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$defs": { "Positive": { "type": "integer", "minimum": 0 } }
}`, srv.URL+"/root.json")
	}))
	t.Cleanup(srv.Close)

	schema := fmt.Sprintf(`{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "properties": { "n": { "$ref": "%s/root.json#/$defs/Positive" } }
}`, srv.URL)

	assertRef := func(t *testing.T, s *Schema) {
		t.Helper()
		require.NoError(t, Validate(s, map[string]any{"n": 5}))
		require.Error(t, Validate(s, map[string]any{"n": -5}))
	}

	t.Run("inline", func(t *testing.T) {
		s, err := CompileString(schema)
		require.NoError(t, err)
		assertRef(t, s)
	})

	t.Run("path", func(t *testing.T) {
		p := filepath.Join(t.TempDir(), "facet.json")
		require.NoError(t, os.WriteFile(p, []byte(schema), 0o600))

		s, err := CompilePath("file://"+p, ifs.OS())
		require.NoError(t, err)
		assertRef(t, s)
	})
}

// Values that only became JSON-compatible through the previous implementation's
// implicit marshal round trip must still validate.
func TestValidateNormalisesGoValues(t *testing.T) {
	s, err := CompileString(`{
  "type": "object",
  "properties": { "b": { "type": "string" } }
}`)
	require.NoError(t, err)

	assert.NoError(t, Validate(s, map[string]any{"b": []byte("hello")}))
}

// Newer drafts must be enforced rather than silently skipped.
func TestValidateEnforces2020Keywords(t *testing.T) {
	s, err := CompileString(`{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$defs": { "positive": { "type": "integer", "minimum": 0 } },
  "type": "object",
  "properties": { "n": { "$ref": "#/$defs/positive", "maximum": 10 } }
}`)
	require.NoError(t, err)

	// The sibling `maximum` alongside `$ref` applies from 2019-09 onwards.
	assert.Error(t, Validate(s, map[string]any{"n": 50}))
	assert.NoError(t, Validate(s, map[string]any{"n": 5}))
}
