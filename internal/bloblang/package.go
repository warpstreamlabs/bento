package bloblang

import (
	"github.com/warpstreamlabs/bento/v1/internal/bloblang/plugins"
)

func init() {
	if err := plugins.Register(); err != nil {
		panic(err)
	}
}
