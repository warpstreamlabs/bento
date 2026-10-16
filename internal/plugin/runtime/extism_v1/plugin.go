package extismv1

import (
	"context"
	"fmt"
	"sync"

	extism "github.com/extism/go-sdk"
	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/plugin/runtime"
)

var _ runtime.Plugin = (*extismPlugin)(nil)

type extismPlugin struct {
	spec docs.ComponentSpec

	compile  func() (*extism.CompiledPlugin, error)
	compiled *extism.CompiledPlugin
}

func newPlugin(spec docs.ComponentSpec, compile func() (*extism.CompiledPlugin, error)) *extismPlugin {
	p := &extismPlugin{spec: spec}

	p.compile = sync.OnceValues(func() (*extism.CompiledPlugin, error) {
		cm, err := compile()
		if err != nil {
			return nil, err
		}

		p.compiled = cm
		return cm, nil
	})

	return p
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

	compiled, err := p.compile()
	if err != nil {
		return nil, err
	}

	proc, err := newWasmProcessor(pconf, compiled)
	if err != nil {
		return nil, err
	}
	return processor.NewAutoObservedBatchedProcessor(conf.Type, proc, nm), nil
}

func (p *extismPlugin) Close(ctx context.Context) error {
	if p.compiled == nil {
		return nil
	}
	err := p.compiled.Close(ctx)
	if err != nil {
		return err
	}

	p.compiled = nil
	return nil
}
