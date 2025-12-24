package kubernetes

import (
	"sync"
)

// metaKeyCache optimizes metadata key generation by interning strings
// like "kubernetes_labels_app" to avoid repeated allocations.
// Each instance maintains its own cache, which is garbage collected
// when the instance is closed.
type metaKeyCache struct {
	cache sync.Map
}

// getMetaKey returns an interned string for the metadata key.
// This avoids repeated allocations for commonly used label and annotation keys.
func (c *metaKeyCache) getMetaKey(prefix, key string) string {
	fullKey := prefix + key
	if val, ok := c.cache.Load(fullKey); ok {
		return val.(string)
	}
	c.cache.Store(fullKey, fullKey)
	return fullKey
}
