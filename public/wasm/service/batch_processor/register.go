// Package batch_processor registers a plugin that is called once per batch,
// with a service.BatchProcessor. To process one message at a time, use the
// processor package instead.
//
// Experimental: This package is experimental and therefore subject to change
// outside of major version releases.
package batch_processor

import (
	"github.com/warpstreamlabs/bento/internal/plugin/components"
	"github.com/warpstreamlabs/bento/public/wasm/service"
)

// BatchProcessorPlugin singleton
var plugin *components.BatchProcessorPlugin = components.NewPlugin[service.BatchProcessor]()

//------------------------------------------------------------------------------

// RegisterBatchProcessor registers the batch processor of the plugin, and must
// be called from an init function. A plugin registers exactly one processor.
//
// Bento calls ctor once for each instance of the plugin, with the processor's
// config decoded from JSON into a Config, including the defaults of the fields
// of the plugin's manifest.
func RegisterBatchProcessor[Config any](ctor func(*Config, *service.Resources) (service.BatchProcessor, error)) error {
	return plugin.Register(components.WrapConstructor(ctor))
}
