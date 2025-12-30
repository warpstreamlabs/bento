package batch_processor

import (
	plugin "github.com/warpstreamlabs/bento/internal/plugin/components/batch_processor"
	"github.com/warpstreamlabs/bento/public/service"
)

func RegisterBatchProcessor(name string, spec *service.ConfigSpec, ctor service.BatchProcessorConstructor) {
	plugin.RegisterBatchProcessor(name, spec, ctor)
}

func IsRegistered() bool {
	return plugin.IsRegistered()
}

func PluginName() string {
	return plugin.PluginName()
}
