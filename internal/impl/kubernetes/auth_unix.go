//go:build unix

package kubernetes

import "os"

// InClusterNamespace returns the namespace this pod is running in,
// or "default" if not running in a cluster.
func InClusterNamespace() string {
	// Try to read the namespace from the service account
	nsBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err == nil {
		return string(nsBytes)
	}
	return "default"
}
