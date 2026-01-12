package components

import (
	"encoding/json"
	"errors"

	"github.com/warpstreamlabs/bento/public/wasm/service"
)

type (
	ProcessorPlugin      = Plugin[service.Processor]
	BatchProcessorPlugin = Plugin[service.BatchProcessor]
)

var (
	errPluginAlreadyRegistered  = errors.New("plugin already registered")
	errPluginNotRegistered      = errors.New("plugin is not registered")
	errInvalidPluginType        = errors.New("Invalid or unsupported plugin type")
	errInvalidInstanceID        = errors.New("instance ID does not correspond to an active plugin")
	errInvalidInitNotRegistered = errors.New("cannot initialize an unregistered plugin")
)

type Plugin[T any] struct {
	ctor     func([]byte) (T, error)
	instance T
}

func (p *Plugin[T]) Register(
	ctor func([]byte) (T, error),
) error {
	if p.IsRegistered() {
		return errPluginAlreadyRegistered
	}

	p.ctor = ctor
	return nil
}

func (p *Plugin[_]) IsRegistered() bool {
	return p.ctor != nil
}

func (p *Plugin[T]) GetInstance() (T, error) {
	var zero T
	if any(p.instance) == nil {
		return zero, errInvalidInstanceID
	}
	return p.instance, nil
}

func (p *Plugin[T]) Init(rawConf []byte) error {
	var err error
	p.instance, err = p.ctor(rawConf)
	if err != nil {
		return err
	}
	return nil
}

// ------------------------------------------------------------------------------

func WrapConstructor[Config, Component any](
	ctor func(*Config, *service.Resources) (Component, error),
) func([]byte) (Component, error) {
	return func(rawBytes []byte) (Component, error) {
		var conf Config

		if len(rawBytes) > 0 {
			if err := json.Unmarshal(rawBytes, &conf); err != nil {
				var zero Component
				return zero, err
			}
		}

		return ctor(&conf, &service.Resources{})
	}
}
