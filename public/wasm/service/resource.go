package service

import (
	"context"
	"encoding/json"
)

type ConstructorFunction[Component any] func(*ParsedConfig, *Resources) (Component, error)

type BatchProcessor interface {
	ProcessBatch(ctx context.Context, batch MessageBatch) ([]MessageBatch, error)
	Close(ctx context.Context) error
}

type Processor interface {
	Process(ctx context.Context, msg *Message) (MessageBatch, error)
	Close(ctx context.Context) error
}

type Resources struct{}

type ParsedConfig struct {
	generic any
}

func NewParsedConfig(cfg any) *ParsedConfig {
	return &ParsedConfig{generic: cfg}
}

func (p *ParsedConfig) Unmarshal(target any) error {
	// HACK(gregfurman): Quick and dirty way of handling custom
	// config. Should probably allow config to be passed in as
	// a generic type.

	if p.generic == nil {
		return nil
	}

	if data, ok := p.generic.([]byte); ok {
		return json.Unmarshal(data, target)
	}

	data, err := json.Marshal(p.generic)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}
