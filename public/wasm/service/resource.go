package service

import (
	"context"
	"encoding/json"
)

// ConstructorFunction is a func that creates a component from its parsed
// config and the resources available to it.
type ConstructorFunction[Component any] func(*ParsedConfig, *Resources) (Component, error)

// BatchProcessor is a processor that is called once per batch, with every
// message of the batch.
type BatchProcessor interface {
	// ProcessBatch returns the batches that replace batch. Return no batches
	// to drop its messages, or more to split them. Returning an error marks
	// every message of batch with it, and passes them on unchanged. To fail
	// a single message instead, call SetError on it.
	ProcessBatch(ctx context.Context, batch MessageBatch) ([]MessageBatch, error)
}

// Processor is a processor that is called once per message.
type Processor interface {
	// Process returns the messages that replace msg. Return no messages to
	// drop it, or more to split it. Returning an error marks msg with it, and
	// passes it on unchanged.
	Process(ctx context.Context, msg *Message) (MessageBatch, error)
}

// TODO: Add callbacks into host here

// Resources provides access to the resources of the Bento instance that runs
// a plugin. It doesn't yet provide any.
type Resources struct{}

// ParsedConfig is the config of a component, which can be decoded into a
// struct with Unmarshal.
type ParsedConfig struct {
	generic any
}

// NewParsedConfig returns a ParsedConfig holding cfg, which is either JSON or
// any value that can be encoded as JSON.
func NewParsedConfig(cfg any) *ParsedConfig {
	return &ParsedConfig{generic: cfg}
}

// Unmarshal decodes the config into target, as encoding/json does. It does
// nothing when the config is empty.
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
