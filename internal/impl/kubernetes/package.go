package kubernetes

import (
	"sync"
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
