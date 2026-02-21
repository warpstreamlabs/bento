//go:build !openbsd

package pulsar

import (
	// Bring in the internal plugin definitions.
	_ "github.com/warpstreamlabs/bento/internal/impl/pulsar"
)
