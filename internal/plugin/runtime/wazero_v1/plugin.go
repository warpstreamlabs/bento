package wazerov1

import (
	"context"
	"fmt"
	"sync"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/plugin/runtime"
)

var _ runtime.Plugin = (*wazeroPlugin)(nil)

type wazeroPlugin struct {
	spec docs.ComponentSpec

	compile  func() (*compiledPlugin, error)
	compiled *compiledPlugin
}

func newPlugin(spec docs.ComponentSpec, compile func() (*compiledPlugin, error)) *wazeroPlugin {
	p := &wazeroPlugin{spec: spec}

	p.compile = sync.OnceValues(func() (*compiledPlugin, error) {
		m, err := compile()
		if err != nil {
			return nil, err
		}

		p.compiled = m
		return m, nil
	})

	return p
}

func (p *wazeroPlugin) Name() string {
	return p.spec.Name
}

func (p *wazeroPlugin) Spec() docs.ComponentSpec {
	return p.spec
}

func (p *wazeroPlugin) RegisterWith(env *bundle.Environment) error {
	switch p.spec.Type {
	case docs.TypeProcessor:
		return env.ProcessorAdd(p.newProcessor, p.spec)
	default:
		return fmt.Errorf("unsupported plugin type: %v", p.spec.Type)
	}
}

func (p *wazeroPlugin) newProcessor(conf processor.Config, nm bundle.NewManagement) (processor.V1, error) {
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

	if compiled.processFn == pluginProcessBatchFn {
		return processor.NewAutoObservedBatchedProcessor(conf.Type, proc, nm), nil
	}

	return processor.NewAutoObservedProcessor(conf.Type, proc, nm), nil
}

func (p *wazeroPlugin) Close(ctx context.Context) error {
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
