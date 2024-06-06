package splunk

import (
	_ "embed"

	"github.com/warpstreamlabs/bento/v1/public/service"
)

//go:embed template_output.yaml
var outputTemplate []byte

func init() {
	if err := service.RegisterTemplateYAML(string(outputTemplate)); err != nil {
		panic(err)
	}
}
