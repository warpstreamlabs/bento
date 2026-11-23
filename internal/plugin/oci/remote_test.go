package oci

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"oras.land/oras-go/v2/registry/remote/credentials"
)

// fakeAuthRegistry answers the registry API's ping, requiring basic auth if
// a password is set.
func fakeAuthRegistry(password string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/" {
			http.NotFound(w, r)
			return
		}
		if password != "" {
			if _, p, ok := r.BasicAuth(); !ok || p != password {
				w.Header().Set("WWW-Authenticate", `Basic realm="test"`)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	})
}

func hostOf(t *testing.T, server *httptest.Server) string {
	t.Helper()
	u, err := url.Parse(server.URL)
	require.NoError(t, err)
	return u.Host
}

func TestRegistriesInsecure(t *testing.T) {
	r := NewRegistries(nil, []string{"registry.internal", "other.internal:5000", " "})

	assert.True(t, r.IsInsecure("registry.internal"))
	assert.True(t, r.IsInsecure("registry.internal:443"))
	assert.True(t, r.IsInsecure("other.internal:5000"))
	assert.False(t, r.IsInsecure("other.internal"))
	assert.True(t, r.IsInsecure("localhost:5000"))
	assert.True(t, r.IsInsecure("127.0.0.1:5000"))
	assert.True(t, r.IsInsecure("[::1]:5000"))
	assert.False(t, r.IsInsecure("ghcr.io"))
}

func TestRegistriesPlainHTTPFallback(t *testing.T) {
	plain := httptest.NewServer(fakeAuthRegistry(""))
	defer plain.Close()
	selfSigned := httptest.NewTLSServer(fakeAuthRegistry(""))
	defer selfSigned.Close()

	r := NewRegistries(nil, nil)

	// Both are on localhost, so insecure. One only speaks HTTP, the other
	// HTTPS with a certificate that can't be verified.
	_, usePlain := r.configure(t.Context(), hostOf(t, plain))
	assert.True(t, usePlain)
	_, usePlain = r.configure(t.Context(), hostOf(t, selfSigned))
	assert.False(t, usePlain)

	creds, err := credentials.NewStore(filepath.Join(t.TempDir(), "config.json"), credentials.StoreOptions{AllowPlaintextPut: true})
	require.NoError(t, err)
	require.NoError(t, r.Login(t.Context(), creds, hostOf(t, selfSigned), "me", "anything"))
	require.NoError(t, r.Login(t.Context(), creds, hostOf(t, plain), "me", "anything"))
}

func TestRegistriesLogin(t *testing.T) {
	server := httptest.NewServer(fakeAuthRegistry("secret"))
	defer server.Close()
	host := hostOf(t, server)

	// An existing auth entry stops a system credential helper being detected
	// and used, so the test never touches the real keychain.
	dockerConfig := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dockerConfig, "config.json"), []byte(`{"auths":{"unused.invalid":{"auth":"eDp5"}}}`), 0o600))
	t.Setenv("DOCKER_CONFIG", dockerConfig)
	store, err := CredentialStore()
	require.NoError(t, err)
	r := NewRegistries(store, nil)

	require.Error(t, r.Login(t.Context(), store, host, "me", "wrong"))
	stored, err := store.Get(t.Context(), host)
	require.NoError(t, err)
	assert.Empty(t, stored.Password)

	require.NoError(t, r.Login(t.Context(), store, host, "me", "secret"))
	stored, err = store.Get(t.Context(), host)
	require.NoError(t, err)
	assert.Equal(t, "me", stored.Username)
	assert.Equal(t, "secret", stored.Password)

	// No credential helper is configured, so they're in the config file.
	assert.True(t, StoredInPlaintext(store, host))
	assert.False(t, StoredInPlaintext(store, "elsewhere.io"))

	require.NoError(t, Logout(t.Context(), store, host))
	stored, err = store.Get(t.Context(), host)
	require.NoError(t, err)
	assert.Empty(t, stored.Password)
	assert.False(t, StoredInPlaintext(store, host))

	assert.Equal(t, "https://index.docker.io/v1/", ServerAddress(DefaultRegistry))
}
