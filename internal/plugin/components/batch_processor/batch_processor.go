package batch_processor

import (
	"github.com/warpstreamlabs/bento/internal/plugin/components"
	"github.com/warpstreamlabs/bento/internal/plugin/model"
	"github.com/warpstreamlabs/bento/public/service"
)

// BatchProcessorPlugin singleton
var plugin *components.BatchProcessorPlugin = &components.BatchProcessorPlugin{}

//------------------------------------------------------------------------------

func RegisterBatchProcessor(name string, spec *service.ConfigSpec, ctor service.BatchProcessorConstructor) {
	plugin = components.RegisterPlugin(name, spec, model.BatchProcessorType, ctor)
}

func IsRegistered() bool {
	return plugin.IsRegistered()
}

func PluginName() string {
	return plugin.Name()
}
