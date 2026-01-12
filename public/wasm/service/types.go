package service

import (
	"errors"
)

type (
	ProcessorPlugin      = Plugin[Processor]
	BatchProcessorPlugin = Plugin[BatchProcessor]
)

var (
	errPluginAlreadyRegistered  = errors.New("plugin already registered")
	errInvalidPluginType        = errors.New("Invalid or unsupported plugin type")
	errInvalidInstanceID        = errors.New("instance ID does not correspond to an active plugin")
	errInvalidInitNotRegistered = errors.New("cannot initialize an unregistered plugin")
)

type Plugin[T any] struct {
	name string
	ctor func(*ParsedConfig, *Resources) (T, error)

	instance T
}

func (p *Plugin[_]) Name() string {
	return p.name
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

func (p *Plugin[T]) Init(rawConf []byte) error {
	var err error
	p.instance, err = p.ctor(NewParsedConfig(rawConf), &Resources{})
	if err != nil {
		return err
	}
	return nil
}
