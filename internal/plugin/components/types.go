package components

import (
	"errors"

	"github.com/warpstreamlabs/bento/internal/component/interop/private"
	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/plugin/model"
	"github.com/warpstreamlabs/bento/public/service"
	"google.golang.org/protobuf/proto"
)

type (
	ProcessorPlugin      = Plugin[service.Processor]
	BatchProcessorPlugin = Plugin[service.BatchProcessor]

	// InputPlugin      = Plugin[service.Input]
	// BatchInputPlugin = Plugin[service.BatchInput]
)

var (
	errInvalidPluginType        = errors.New("Invalid or unsupported plugin type")
	errInvalidInstanceID        = errors.New("instance ID does not correspond to an active plugin")
	errInvalidInitNotRegistered = errors.New("cannot initialize an unregistered plugin")
)

type Plugin[T any] struct {
	name string
	ctor func(*service.ParsedConfig, *service.Resources) (T, error)
	spec *service.ConfigSpec
	typ  model.Type

	instance T
}

func (p *Plugin[_]) Name() string {
	return p.name
}

func (p *Plugin[T]) Type() model.Type {
	return p.typ
}

func (p *Plugin[_]) IsRegistered() bool {
	return any(p.instance) != nil
}

func (p *Plugin[T]) GetInstance() (T, error) {
	var zero T
	if any(p.instance) == nil {
		return zero, errInvalidInstanceID
	}
	return p.instance, nil
}

func (p *Plugin[T]) Init(conf string) error {
	pConf, err := p.spec.ParseYAML(conf, service.GlobalEnvironment())
	if err != nil {
		return err
	}

	p.instance, err = p.ctor(pConf, &service.Resources{})
	if err != nil {
		return err
	}

	return nil
}

func (p *Plugin[_]) Spec() model.Buffer {
	spec := private.FromInternal(p.spec.XUnwrap())

	var err error
	spec.Name = p.Name()
	spec.Type, err = model.FromPluginType(p.typ)
	if err != nil {
		panic(err.Error())
	}

	specpb, err := docs.FromComponentSpec(spec)
	if err != nil {
		panic(err.Error())
	}

	opts := proto.MarshalOptions{
		Deterministic: true,
	}

	specBytes, err := opts.Marshal(specpb)
	if err != nil {
		panic(err.Error())
	}

	return model.FromSlice(specBytes)
}
