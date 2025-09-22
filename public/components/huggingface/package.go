//go:build !arm

package all

import (
	// Bring in the internal plugin definitions.
	_ "github.com/warpstreamlabs/bento/internal/impl/huggingface"
)
