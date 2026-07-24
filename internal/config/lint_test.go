package config

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/docs"
)

func TestReadYAMLFileLintedRemoteSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`
input:
  label: fooin
  inproc: foo

output:
  label: fooout
  inproc: bar
`))
	}))
	defer srv.Close()

	conf, lints, err := ReadYAMLFileLinted(nil, Spec(), srv.URL, false, docs.NewLintConfig(bundle.GlobalEnvironment))
	require.NoError(t, err)
	require.Empty(t, lints)
	assert.Equal(t, "fooin", conf.Input.Label)
}

func TestReadYAMLFileLintedRemoteNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	_, _, err := ReadYAMLFileLinted(nil, Spec(), srv.URL, false, docs.NewLintConfig(bundle.GlobalEnvironment))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "404")
}

func TestReadYAMLFileLintedRemoteEnvVar(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`
input:
  label: ${BENTO_TEST_LINT_LABEL:fooin}
  inproc: foo

output:
  label: fooout
  inproc: bar
`))
	}))
	defer srv.Close()

	t.Setenv("BENTO_TEST_LINT_LABEL", "customlabel")

	conf, lints, err := ReadYAMLFileLinted(nil, Spec(), srv.URL, false, docs.NewLintConfig(bundle.GlobalEnvironment))
	require.NoError(t, err)
	require.Empty(t, lints)
	assert.Equal(t, "customlabel", conf.Input.Label)
}

func TestReadYAMLFileLintedRemoteSkipEnvVarCheck(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`
input:
  label: ${BENTO_TEST_MISSING_VAR}
  inproc: foo

output:
  label: fooout
  inproc: bar
`))
	}))
	defer srv.Close()

	_, lints, err := ReadYAMLFileLinted(nil, Spec(), srv.URL, true, docs.NewLintConfig(bundle.GlobalEnvironment))
	require.NoError(t, err)
	require.Empty(t, lints)
}
