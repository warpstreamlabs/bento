//go:build x_bento_extra

package all

import (
	// Import extra packages, these are packages only imported with the tag
	// x_bento_extra, which is normally reserved for -cgo suffixed builds
	_ "github.com/warpstreamlabs/bento/v4/internal/impl/zeromq"
)
