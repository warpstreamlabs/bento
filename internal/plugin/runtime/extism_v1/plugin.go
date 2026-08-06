package extismv1

import (
	"fmt"

	extism "github.com/extism/go-sdk"
	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/plugin/runtime"
)

var _ runtime.Plugin[*extism.CompiledPlugin] = (*extismPlugin)(nil)

type extismPlugin struct {
	spec     docs.ComponentSpec
	compiled *extism.CompiledPlugin
}

func (p *extismPlugin) Name() string {
	return p.spec.Name
}

func (p *extismPlugin) Spec() docs.ComponentSpec {
	return p.spec
}

func (p *extismPlugin) RegisterWith(env *bundle.Environment) error {
	switch p.spec.Type {
	case docs.TypeProcessor:
		return env.ProcessorAdd(p.newProcessor, p.spec)
	default:
		return fmt.Errorf("unsupported plugin type: %v", p.spec.Type)
	}
}

func (p *extismPlugin) newProcessor(conf processor.Config, nm bundle.NewManagement) (processor.V1, error) {
	pconf, err := p.spec.Config.ParsedConfigFromAny(conf.Plugin)
	if err != nil {
		return nil, err
	}

	proc, err := newWasmProcessor(pconf, p.compiled)
	if err != nil {
		return nil, err
	}
	return processor.NewAutoObservedBatchedProcessor(conf.Type, proc, nm), nil
}
