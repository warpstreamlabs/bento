package twitter

import (
	_ "embed"

	"github.com/warpstreamlabs/bento/v1/public/service"

	// bloblang functions are registered in init functions under this package
	// so ensure they are loaded first
	_ "github.com/warpstreamlabs/bento/v1/public/components/pure"
)

//go:embed template_search_input.yaml
var searchInputTemplate []byte

func init() {
	if err := service.RegisterTemplateYAML(string(searchInputTemplate)); err != nil {
		panic(err)
	}
}
