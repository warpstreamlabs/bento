// Package kubernetes provides Bento inputs for Kubernetes observability.
//
// This package implements native Kubernetes integrations that feel as natural
// as kubectl commands. System administrators can build pipelines for:
//
//   - Pod log streaming (kubernetes_logs)
//   - Cluster event monitoring (kubernetes_events)
//   - Resource state changes (kubernetes_watch)
//   - Prometheus metrics scraping (kubernetes_metrics)
//
// All components share a common authentication layer supporting:
//   - Automatic in-cluster detection
//   - Kubeconfig file authentication
//   - Service account tokens
//   - Explicit credentials
package kubernetes

import (
	"math/rand"
	"sync"
	"time"
)

const (
	// Backoff parameters for watch reconnection
	minBackoff = 1 * time.Second
	maxBackoff = 60 * time.Second
)

// keyCache optimizes metadata key generation by interning strings
// like "kubernetes_labels_app" to avoid repeated allocations.
var keyCache sync.Map

func getMetaKey(prefix, key string) string {
	fullKey := prefix + key
	if val, ok := keyCache.Load(fullKey); ok {
		return val.(string)
	}
	keyCache.Store(fullKey, fullKey)
	return fullKey
}

// calculateBackoff returns an exponential backoff duration with jitter.
// The backoff doubles with each attempt, capped at maxBackoff.
// Jitter adds randomness to prevent synchronized retries across instances.
func calculateBackoff(attempt int) time.Duration {
	backoff := minBackoff * time.Duration(1<<uint(attempt))
	if backoff > maxBackoff {
		backoff = maxBackoff
	}
	// Add up to 25% jitter
	jitter := time.Duration(rand.Int63n(int64(backoff / 4)))
	return backoff + jitter
}
