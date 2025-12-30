//go:build !wasm

package runtime

import (
	"fmt"

	"github.com/tetratelabs/wazero/api"
	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/plugin"
)

type WasmPlugin struct {
	name string
	spec docs.ComponentSpec
	pool plugin.Pool[api.Module]
}

func NewWasmPlugin(
	spec docs.ComponentSpec,
	pool plugin.Pool[api.Module],
	defs map[string]api.FunctionDefinition,
) (*WasmPlugin, error) {
	switch spec.Type {
	case docs.TypeProcessor:
		if err := hasExportedFunctions(defs, "process_batch"); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported plugin type: %v", spec.Type)
	}

	return &WasmPlugin{
		name: spec.Name,
		spec: spec,
		pool: pool,
	}, nil
}

func (wp *WasmPlugin) Name() string {
	return wp.name
}

func (wp *WasmPlugin) Spec() docs.ComponentSpec {
	return wp.spec
}

func (wp *WasmPlugin) RegisterWith(env *bundle.Environment) error {
	switch wp.spec.Type {
	case docs.TypeProcessor:
		return env.ProcessorAdd(wp.newProcessor, wp.spec)
	default:
		return fmt.Errorf("unsupported plugin type: %v", wp.spec.Type)
	}
}

func (wp *WasmPlugin) newProcessor(conf processor.Config, nm bundle.NewManagement) (processor.V1, error) {
	pconf, err := wp.spec.Config.ParsedConfigFromAny(conf.Plugin)
	if err != nil {
		return nil, err
	}
	return newWasmPooledProcessor(pconf, wp.pool)
}
