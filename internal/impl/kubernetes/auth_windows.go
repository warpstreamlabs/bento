//go:build windows

package kubernetes

// InClusterNamespace returns "default" on Windows as in-cluster
// authentication is not supported.
func InClusterNamespace() string {
	return "default"
}
