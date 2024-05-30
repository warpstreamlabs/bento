package bloblang

import (
	"github.com/warpstreamlabs/bento/v4/internal/bloblang/plugins"
)

func init() {
	if err := plugins.Register(); err != nil {
		panic(err)
	}
}
