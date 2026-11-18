// Package processor registers a plugin that is called once per message, with a
// service.Processor. To process every message of a batch at once, use the
// batch_processor package instead.
//
// Experimental: This package is experimental and therefore subject to change
// outside of major version releases.
package processor

import (
	"github.com/warpstreamlabs/bento/internal/plugin/components"
	"github.com/warpstreamlabs/bento/public/wasm/service"
)

// ProcessorPlugin singleton
var plugin *components.ProcessorPlugin = components.NewPlugin[service.Processor]()

//------------------------------------------------------------------------------

// RegisterProcessor registers the processor of the plugin, and must be called
// from an init function. A plugin registers exactly one processor.
//
// Bento calls ctor once for each instance of the plugin, with the processor's
// config decoded from JSON into a Config, including the defaults of the fields
// of the plugin's manifest.
func RegisterProcessor[Config any](ctor func(*Config, *service.Resources) (service.Processor, error)) error {
	return plugin.Register(components.WrapConstructor(ctor))
}
