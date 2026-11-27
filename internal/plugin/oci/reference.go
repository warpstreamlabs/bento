package oci

import (
	"fmt"
	"net"
	"strings"

	"oras.land/oras-go/v2/registry"
)

// SourcePrefix marks a plugin source as a reference to an OCI artifact.
const SourcePrefix = "oci://"

// Reference is a parsed reference to a plugin, such as
// ghcr.io/org/plugin:1.0.0 or ghcr.io/org/plugin@sha256:...
type Reference struct {
	registry.Reference
}

// ParseReference parses a plugin reference. An oci:// prefix is allowed, and
// a reference without a tag or digest gets the tag latest, as with Docker.
// The registry can't be left out, as there's no default registry for plugins.
func ParseReference(s string) (Reference, error) {
	s = strings.TrimPrefix(s, SourcePrefix)
	ref, err := registry.ParseReference(s)
	if err != nil || !strings.Contains(s, "/") {
		return Reference{}, fmt.Errorf("invalid plugin reference %q, expected a registry, repository and tag such as ghcr.io/org/plugin:1.0.0", s)
	}
	if ref.Reference == "" {
		ref.Reference = "latest"
	}
	return Reference{Reference: ref}, nil
}

// IsDigest returns true if the reference is to a digest rather than a tag.
func (r Reference) IsDigest() bool {
	return r.ValidateReferenceAsDigest() == nil
}

// Repo returns the registry and repository, without the tag or digest.
func (r Reference) Repo() string {
	return r.Registry + "/" + r.Repository
}

// isLocalRegistry returns true for registries on the local machine, which
// are treated as insecure, as Docker does.
func isLocalRegistry(host string) bool {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
