package cypher

import "github.com/warpstreamlabs/bento/internal/bundle"

func init() {
	bundle.GlobalEnvironment.AllowExperimental()
	bundle.GlobalEnvironment.AllowBeta()
}
