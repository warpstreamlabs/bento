package components

import (
	"github.com/warpstreamlabs/bento/internal/plugin/model"
	"github.com/warpstreamlabs/bento/public/service"
)

type PluginType interface {
	Name() string
	Type() model.Type
	Spec() model.Buffer
	Init(conf string) error
	IsRegistered() bool
}

var plugin PluginType = &noopPlugin{}

// ------------------------------------------------------------------------------

type noopPlugin struct{}

// TODO(gregfurman): These should either panic OR have proper error codes returned that the host
// can understand

func (noopPlugin) Name() string           { return "" }
func (noopPlugin) Type() model.Type       { return model.Unregistered }
func (noopPlugin) Spec() model.Buffer     { return 0 }
func (noopPlugin) Init(conf string) error { return errInvalidInitNotRegistered }
func (noopPlugin) IsRegistered() bool     { return false }

// ------------------------------------------------------------------------------

func RegisterPlugin[T any](
	name string,
	spec *service.ConfigSpec,
	typ model.Type,
	ctor func(*service.ParsedConfig, *service.Resources) (T, error),
) *Plugin[T] {
	if plugin.IsRegistered() {
		return plugin.(*Plugin[T])
	}

	p := &Plugin[T]{
		name: name,
		ctor: ctor,
		spec: spec,
		typ:  typ,
	}
	plugin = p
	return p
}

//------------------------------------------------------------------------------
