package config

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemoteConfigFetcherRegistration(t *testing.T) {
	// Save and restore original fetcher
	orig := remoteConfigFetcher
	t.Cleanup(func() { remoteConfigFetcher = orig })

	called := false
	SetRemoteConfigFetcher(func(ctx context.Context, specBytes []byte) ([]byte, error) {
		called = true
		return specBytes, nil // echo back for this test
	})

	assert.NotNil(t, remoteConfigFetcher)
	_, _ = remoteConfigFetcher(context.Background(), []byte("test"))
	assert.True(t, called)
}

func TestOptSetRemoteConfigPath(t *testing.T) {
	r := &Reader{}
	opt := OptSetRemoteConfigPath("/some/path/remote.yaml")
	opt(r)
	assert.Equal(t, "/some/path/remote.yaml", r.RemoteConfigPath)
}

func TestReadMainRemoteConfig(t *testing.T) {
	// Save and restore original fetcher
	orig := remoteConfigFetcher
	t.Cleanup(func() { remoteConfigFetcher = orig })

	// A minimal valid bento config returned by the fetcher.
	remoteConf := `
input:
  generate:
    mapping: 'root = {"hello":"from remote"}'
    interval: "1s"
    count: 1
output:
  drop: {}
`
	// Write a spec file (the --remote-config argument)
	specFile := filepath.Join(t.TempDir(), "remote-config.yaml")
	require.NoError(t, os.WriteFile(specFile, []byte("url: https://example.com/config\nverb: GET\n"), 0o600))

	// Register a fetcher that ignores the spec and returns our fixed config.
	SetRemoteConfigFetcher(func(ctx context.Context, specBytes []byte) ([]byte, error) {
		assert.Contains(t, string(specBytes), "url: https://example.com/config")
		return []byte(remoteConf), nil
	})

	rdr := newDummyReader("", nil, OptSetRemoteConfigPath(specFile))
	conf, _, lints, _, err := rdr.readMain("")
	require.NoError(t, err)
	assert.Empty(t, lints)
	assert.Equal(t, "generate", conf.Input.Type)
}

func TestReadMainRemoteConfigNilFetcher(t *testing.T) {
	orig := remoteConfigFetcher
	t.Cleanup(func() { remoteConfigFetcher = orig })

	remoteConfigFetcher = nil

	specFile := filepath.Join(t.TempDir(), "remote-config.yaml")
	require.NoError(t, os.WriteFile(specFile, []byte("url: https://example.com/config\n"), 0o600))

	rdr := newDummyReader("", nil, OptSetRemoteConfigPath(specFile))
	_, _, _, _, err := rdr.readMain("")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetcher not initialised")
}

func TestReadMainRemoteConfigMissingSpecFile(t *testing.T) {
	orig := remoteConfigFetcher
	t.Cleanup(func() { remoteConfigFetcher = orig })

	SetRemoteConfigFetcher(func(ctx context.Context, specBytes []byte) ([]byte, error) {
		return nil, nil
	})

	rdr := newDummyReader("", nil, OptSetRemoteConfigPath("/nonexistent/path.yaml"))
	_, _, _, _, err := rdr.readMain("")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read remote config spec")
}

func TestReadMainRemoteConfigEnvVarInSpec(t *testing.T) {
	// Save and restore original fetcher
	orig := remoteConfigFetcher
	t.Cleanup(func() { remoteConfigFetcher = orig })

	// Set an env var that will be referenced in the spec file.
	t.Setenv("BENTO_TEST_TOKEN", "supersecret")

	remoteConf := `
input:
  generate:
    mapping: 'root = {"hello":"from remote"}'
    interval: "1s"
    count: 1
output:
  drop: {}
`
	// Spec file uses ${BENTO_TEST_TOKEN} — should be expanded before the
	// fetcher receives the bytes.
	specContent := "url: https://example.com/config\nverb: GET\nheaders:\n  Authorization: \"Bearer ${BENTO_TEST_TOKEN}\"\n"
	specFile := filepath.Join(t.TempDir(), "remote-config_env.yaml")
	require.NoError(t, os.WriteFile(specFile, []byte(specContent), 0o600))

	SetRemoteConfigFetcher(func(ctx context.Context, specBytes []byte) ([]byte, error) {
		// Env var must have been substituted before we receive the bytes.
		assert.Contains(t, string(specBytes), "Bearer supersecret")
		assert.NotContains(t, string(specBytes), "${BENTO_TEST_TOKEN}")
		return []byte(remoteConf), nil
	})

	rdr := newDummyReader("", nil, OptSetRemoteConfigPath(specFile))
	_, _, lints, _, err := rdr.readMain("")
	require.NoError(t, err)
	assert.Empty(t, lints)
}

func TestTriggerMainUpdateRemoteConfigNoOp(t *testing.T) {
	r := &Reader{RemoteConfigPath: "/some/spec.yaml"}
	// TriggerMainUpdate should return nil immediately without doing anything
	// when remoteConfigPath is set (file watching not supported for remote configs).
	err := r.TriggerMainUpdate(nil, false, "")
	assert.NoError(t, err)
}
