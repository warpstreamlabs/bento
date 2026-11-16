package logv1

import (
	"context"
	"fmt"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"

	"github.com/warpstreamlabs/bento/internal/bundle"
)

const ModuleName = "bento_log_v1"

// Module is the bento_log_v1 host module, which writes guest logs to the
// logger of the processor it is instantiated for.
type Module struct {
	plugin string
}

func New(plugin string) Module { return Module{plugin: plugin} }

func (Module) Name() string { return ModuleName }

// Instantiate a bento_log_v1 host module against the shared wazero.Runtime.
func (m Module) Instantiate(ctx context.Context, rt wazero.Runtime, nm bundle.NewManagement) (api.Module, error) {
	l := nm.Logger().WithFields(map[string]string{"plugin": m.plugin})

	b := rt.NewHostModuleBuilder(m.Name())

	logCalls := map[string]func(format string, v ...any){
		"trace": l.Trace,
		"debug": l.Debug,
		"info":  l.Info,
		"warn":  l.Warn,
		"error": l.Error,
	}

	for name, call := range logCalls {
		m.export(b, name, call)
	}

	compiled, err := b.Compile(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = compiled.Close(ctx) }()

	// NOTE: Each module is anonymous since we want to ensure an instance has exclusive access to the assigned
	// host module.
	return rt.InstantiateModule(ctx, compiled, wazero.NewModuleConfig().WithName(""))
}

func (m Module) export(b wazero.HostModuleBuilder, name string, logf func(format string, v ...any)) {
	b.NewFunctionBuilder().
		WithGoModuleFunction(api.GoModuleFunc(func(ctx context.Context, mod api.Module, stack []uint64) {
			address, length := api.DecodeU32(stack[0]), api.DecodeU32(stack[1])

			mem := mod.Memory()
			if mem == nil {
				panic(fmt.Errorf("%s.%s: plugin %q exports no memory", ModuleName, name, m.plugin))
			}

			buf, ok := mem.Read(address, length)
			if !ok {
				panic(fmt.Errorf("%s.%s: out-of-bounds read from plugin %q", ModuleName, name, m.plugin))
			}

			// TODO(gregfurman): Consider providing a cap to the log length.
			logf("%s", string(buf))
		}), []api.ValueType{api.ValueTypeI32, api.ValueTypeI32}, nil).
		WithParameterNames("address", "length").
		Export(name)
}
