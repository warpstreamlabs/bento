package processor

import (
	"github.com/warpstreamlabs/bento/internal/plugin/components"
	"github.com/warpstreamlabs/bento/public/wasm/service"
)

// ProcessorPlugin singleton
var plugin *components.ProcessorPlugin = components.NewPlugin[service.Processor]()

//------------------------------------------------------------------------------

func RegisterProcessor[Config any](ctor func(*Config, *service.Resources) (service.Processor, error)) error {
	return plugin.Register(components.WrapConstructor(ctor))
}
