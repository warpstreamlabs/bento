package components

type PluginType interface {
	Init(rawConf []byte) error
	IsRegistered() bool
}

//lint:ignore U1000 Read by init_plugin in register.go when building for WASM.
var plugin PluginType = &noopPlugin{}

// ------------------------------------------------------------------------------

type noopPlugin struct{}

// TODO(gregfurman): These should either panic OR have proper error codes returned that the host
// can understand

func (noopPlugin) Init(_ []byte) error { return errInvalidInitNotRegistered }
func (noopPlugin) IsRegistered() bool  { return false }

// ------------------------------------------------------------------------------

// NewPlugin returns a Plugin implementation whose underlying instance type is
// Component.
func NewPlugin[Component any]() *Plugin[Component] {
	p := &Plugin[Component]{}
	plugin = p
	return p
}
