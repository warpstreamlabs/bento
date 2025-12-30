package processor

import (
	"github.com/warpstreamlabs/bento/internal/plugin/components"
	"github.com/warpstreamlabs/bento/internal/plugin/model"
	"github.com/warpstreamlabs/bento/public/service"
)

// ProcessorPlugin singleton
var plugin *components.ProcessorPlugin = &components.ProcessorPlugin{}

//------------------------------------------------------------------------------

func RegisterProcessor(name string, spec *service.ConfigSpec, ctor service.ProcessorConstructor) {
	plugin = components.RegisterPlugin(name, spec, model.ProcessorType, ctor)
}

func IsRegistered() bool {
	return plugin.IsRegistered()
}

func PluginName() string {
	return plugin.Name()
}

//------------------------------------------------------------------------------
