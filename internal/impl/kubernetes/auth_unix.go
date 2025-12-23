//go:build unix

package kubernetes

import "github.com/warpstreamlabs/bento/internal/filepath/ifs"

// InClusterNamespace returns the namespace this pod is running in,
// or "default" if not running in a cluster.
func InClusterNamespace() string {
	// Try to read the namespace from the service account
	nsBytes, err := ifs.ReadFile(ifs.OS(), "/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err == nil {
		return string(nsBytes)
	}
	return "default"
}
