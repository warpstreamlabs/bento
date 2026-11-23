package oci

import (
	"context"
	"fmt"

	"oras.land/oras-go/v2"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/credentials"
	"oras.land/oras-go/v2/registry/remote/retry"
)

// CredentialStore returns the store of registry credentials, which is
// Docker's config file and credential helpers, so that docker login and
// bento plugin login share credentials.
func CredentialStore() (credentials.Store, error) {
	store, err := credentials.NewStoreFromDocker(credentials.StoreOptions{
		AllowPlaintextPut:        true,
		DetectDefaultNativeStore: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open registry credentials: %w", err)
	}
	return store, nil
}

func newClient(creds credentials.Store) *auth.Client {
	client := &auth.Client{
		Client: retry.DefaultClient,
		Cache:  auth.NewCache(),
	}
	client.SetUserAgent("bento")
	if creds != nil {
		client.Credential = credentials.Credential(creds)
	}
	return client
}

// RegistryRemote returns a RemoteFunc that reaches registries with the
// credentials of creds, which may be nil. Registries on the local machine are
// reached over plain HTTP.
func RegistryRemote(creds credentials.Store) RemoteFunc {
	client := newClient(creds)
	return func(_ context.Context, ref Reference) (oras.Target, error) {
		repo, err := remote.NewRepository(ref.Repo())
		if err != nil {
			return nil, err
		}
		repo.Client = client
		repo.PlainHTTP = isLocalRegistry(ref.Registry)
		return repo, nil
	}
}

// Login checks the credentials work with a registry and saves them.
func Login(ctx context.Context, creds credentials.Store, registry, username, password string) error {
	reg, err := remote.NewRegistry(registry)
	if err != nil {
		return fmt.Errorf("invalid registry %q: %w", registry, err)
	}
	reg.Client = newClient(nil)
	reg.PlainHTTP = isLocalRegistry(registry)
	return credentials.Login(ctx, creds, reg, auth.Credential{Username: username, Password: password})
}

// Logout removes the saved credentials of a registry.
func Logout(ctx context.Context, creds credentials.Store, registry string) error {
	return credentials.Logout(ctx, creds, registry)
}
