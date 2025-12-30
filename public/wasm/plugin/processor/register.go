package processor

import (
	plugin "github.com/warpstreamlabs/bento/internal/plugin/components/processor"
	"github.com/warpstreamlabs/bento/public/service"
)

func RegisterBatchProcessor(name string, spec *service.ConfigSpec, ctor service.ProcessorConstructor) {
	plugin.RegisterProcessor(name, spec, ctor)
}

func IsRegistered() bool {
	return plugin.IsRegistered()
}

func PluginName() string {
	return plugin.PluginName()
}
