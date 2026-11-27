package oci

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"

	"oras.land/oras-go/v2"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/credentials"
	"oras.land/oras-go/v2/registry/remote/retry"
)

// DefaultRegistry is the registry login and logout use when none is given,
// as with docker.
const DefaultRegistry = "docker.io"

// CredentialStore returns the store of registry credentials, which is
// Docker's config file and credential helpers, so that docker login and
// bento plugin login share credentials.
func CredentialStore() (*credentials.DynamicStore, error) {
	store, err := credentials.NewStoreFromDocker(credentials.StoreOptions{
		AllowPlaintextPut:        true,
		DetectDefaultNativeStore: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open registry credentials: %w", err)
	}
	return store, nil
}

// ServerAddress returns the key credentials for a registry are stored under,
// which for Docker Hub isn't its host name.
func ServerAddress(registry string) string {
	return credentials.ServerAddressFromRegistry(registry)
}

// StoredInPlaintext returns true if the credentials for registry are stored
// in the config file of store itself, rather than a credential helper.
func StoredInPlaintext(store *credentials.DynamicStore, registry string) bool {
	b, err := os.ReadFile(store.ConfigPath())
	if err != nil {
		return false
	}
	var conf struct {
		Auths map[string]struct {
			Auth     string `json:"auth"`
			Password string `json:"password"`
		} `json:"auths"`
	}
	if json.Unmarshal(b, &conf) != nil {
		return false
	}
	a := conf.Auths[ServerAddress(registry)]
	return a.Auth != "" || a.Password != ""
}

//------------------------------------------------------------------------------

// Registries reaches container registries, with credentials and the same
// handling of insecure registries as Docker: registries on the local machine,
// and those listed as insecure, are reached over HTTPS without verifying
// certificates, or over plain HTTP if they don't support HTTPS.
type Registries struct {
	creds    credentials.Store
	insecure map[string]struct{}

	secureClient, insecureClient *auth.Client

	mu        sync.Mutex
	plainHTTP map[string]bool // by host, for insecure registries
}

// NewRegistries returns access to registries using the credentials of creds,
// which may be nil. insecure lists registries, as host or host:port, that may
// be reached without TLS verification or over plain HTTP.
func NewRegistries(creds credentials.Store, insecure []string) *Registries {
	r := &Registries{
		creds:     creds,
		insecure:  map[string]struct{}{},
		plainHTTP: map[string]bool{},
	}
	for _, host := range insecure {
		if host = strings.TrimSpace(host); host != "" {
			r.insecure[host] = struct{}{}
		}
	}

	r.secureClient = r.newClient(retry.DefaultClient)

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // Only for registries the user lists as insecure.
	r.insecureClient = r.newClient(&http.Client{Transport: retry.NewTransport(transport)})
	return r
}

func (r *Registries) newClient(base *http.Client) *auth.Client {
	client := &auth.Client{
		Client: base,
		Cache:  auth.NewCache(),
	}
	client.SetUserAgent("bento")
	if r.creds != nil {
		client.Credential = credentials.Credential(r.creds)
	}
	return client
}

// IsInsecure returns true if a registry may be reached without TLS
// verification.
func (r *Registries) IsInsecure(host string) bool {
	if _, listed := r.insecure[host]; listed {
		return true
	}
	hostname := host
	if h, _, err := net.SplitHostPort(host); err == nil {
		hostname = h
		if _, listed := r.insecure[hostname]; listed {
			return true
		}
	}
	return isLocalRegistry(hostname)
}

// usePlainHTTP decides whether to reach an insecure registry over plain
// HTTP, by checking whether it supports HTTPS.
func (r *Registries) usePlainHTTP(ctx context.Context, host string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if plain, known := r.plainHTTP[host]; known {
		return plain
	}

	plain := false
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+host+"/v2/", http.NoBody)
	if err == nil {
		resp, err := r.insecureClient.Client.Do(req)
		if err == nil {
			resp.Body.Close()
		} else {
			plain = errors.Is(err, http.ErrSchemeMismatch) ||
				strings.Contains(err.Error(), "server gave HTTP response to HTTPS client")
		}
		if err == nil || plain {
			r.plainHTTP[host] = plain
		}
	}
	return plain
}

func (r *Registries) configure(ctx context.Context, host string) (*auth.Client, bool) {
	if !r.IsInsecure(host) {
		return r.secureClient, false
	}
	return r.insecureClient, r.usePlainHTTP(ctx, host)
}

// Remote returns a RemoteFunc for reaching registries.
func (r *Registries) Remote() RemoteFunc {
	return func(ctx context.Context, ref Reference) (oras.Target, error) {
		repo, err := remote.NewRepository(ref.Repo())
		if err != nil {
			return nil, err
		}
		repo.Client, repo.PlainHTTP = r.configure(ctx, ref.Registry)
		return repo, nil
	}
}

// Login checks credentials work with a registry, and saves them.
func (r *Registries) Login(ctx context.Context, store credentials.Store, registry, username, password string) error {
	reg, err := remote.NewRegistry(registry)
	if err != nil {
		return fmt.Errorf("invalid registry %q: %w", registry, err)
	}
	client, plain := r.configure(ctx, registry)
	reg.Client = &auth.Client{Client: client.Client, Header: client.Header}
	reg.PlainHTTP = plain
	return credentials.Login(ctx, store, reg, auth.Credential{Username: username, Password: password})
}

// Logout removes the saved credentials of a registry.
func Logout(ctx context.Context, store credentials.Store, registry string) error {
	return credentials.Logout(ctx, store, registry)
}
