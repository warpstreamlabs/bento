// Package extended contains component implementations that have a larger
// dependency footprint but do not interact with external systems (so an
// extension of pure components)
//
// EXPERIMENTAL: The specific components excluded by this package may change
// outside of major version releases. This means we may choose to remove certain
// plugins if we determine that their dependencies are likely to interfere with
// the goals of this package.
package extended

import (
	// Import pure but larger packages.
	_ "github.com/warpstreamlabs/bento/v1/internal/impl/awk"
	_ "github.com/warpstreamlabs/bento/v1/internal/impl/jsonpath"
	_ "github.com/warpstreamlabs/bento/v1/internal/impl/lang"
	_ "github.com/warpstreamlabs/bento/v1/internal/impl/msgpack"
	_ "github.com/warpstreamlabs/bento/v1/internal/impl/parquet"
	_ "github.com/warpstreamlabs/bento/v1/internal/impl/protobuf"
	_ "github.com/warpstreamlabs/bento/v1/internal/impl/pure/extended"
	_ "github.com/warpstreamlabs/bento/v1/internal/impl/xml"
)
