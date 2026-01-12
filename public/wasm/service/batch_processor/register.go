package batch_processor

import (
	"github.com/warpstreamlabs/bento/internal/plugin/components"
	"github.com/warpstreamlabs/bento/public/wasm/service"
)

// BatchProcessorPlugin singleton
var plugin *components.BatchProcessorPlugin = components.NewPlugin[service.BatchProcessor]()

//------------------------------------------------------------------------------

func RegisterBatchProcessor[Config any](ctor func(*Config, *service.Resources) (service.BatchProcessor, error)) error {
	return plugin.Register(components.WrapConstructor(ctor))
}
