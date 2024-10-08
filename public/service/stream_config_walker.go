package service

import (
	"errors"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/warpstreamlabs/bento/internal/docs"
)

// ErrSkipComponents is used as a return value from a component walking func to
// indicate that the component currently viewed should not have its child fields
// walked. It is not returned as an error by any function.
var ErrSkipComponents = errors.New("skip components")

// StreamConfigWalker provides utilities for parsing and then walking stream
// configs, allowing you to analyse the structure of a given config.
type StreamConfigWalker struct {
	env  *Environment
	spec docs.FieldSpecs
}

// NewStreamConfigWalker creates a component for parsing and then walking stream
// configs, allowing you to analyse the structure of a given config.
func (s *ConfigSchema) NewStreamConfigWalker() *StreamConfigWalker {
	return &StreamConfigWalker{
		env:  s.env,
		spec: s.fields,
	}
}

// WalkedComponent is a struct containing information about a component yielded
// via the WalkComponents method.
type WalkedComponent struct {
	ComponentType string
	Name          string
	Path          string
	Label         string

	LineStart int
	LineEnd   int

	jpPath string // Memoized

	conf any
	c    docs.WalkedComponent
}

func walkedComponentFromInternal(c docs.WalkedComponent) *WalkedComponent {
	return &WalkedComponent{
		ComponentType: string(c.Field.Type),
		Name:          c.Name,
		Path:          c.Path,
		Label:         c.Label,
		LineStart:     c.LineStart,
		LineEnd:       c.LineEnd,
		conf:          c.Value,
		c:             c,
	}
}

// WalkComponentsYAML attempts to walk the config tree from the currently walked
// component, calling the provided func for all child components.
//
// Deprecated: This method is functionally the equivalent to WalkComponents and
// doesn't require nor exhibit YAML specific behaviour.
func (w *WalkedComponent) WalkComponentsYAML(fn func(w *WalkedComponent) error) error {
	return w.c.WalkComponents(func(c docs.WalkedComponent) error {
		tmpErr := fn(walkedComponentFromInternal(c))
		if errors.Is(tmpErr, ErrSkipComponents) {
			tmpErr = docs.ErrSkipChildComponents
		}
		return tmpErr
	})
}

// WalkComponents attempts to walk the config tree from the currently walked
// component, calling the provided func for all child components.
func (w *WalkedComponent) WalkComponents(fn func(w *WalkedComponent) error) error {
	return w.c.WalkComponents(func(c docs.WalkedComponent) error {
		tmpErr := fn(walkedComponentFromInternal(c))
		if errors.Is(tmpErr, ErrSkipComponents) {
			tmpErr = docs.ErrSkipChildComponents
		}
		return tmpErr
	})
}

// PathAsJSONPointer returns the Path, stored as a dot path, as a JSON pointer.
func (w *WalkedComponent) PathAsJSONPointer() string {
	if w.jpPath != "" {
		return w.jpPath
	}
	w.jpPath = "/" + strings.ReplaceAll(w.Path, ".", "/")
	return w.jpPath
}

// ConfigYAML returns the configuration of a walked component in YAML form.
func (w *WalkedComponent) ConfigYAML() string {
	var node yaml.Node
	switch t := w.conf.(type) {
	case *yaml.Node:
		node = *t
	default:
		_ = node.Encode(t)
	}

	yamlBytes, err := docs.MarshalYAML(node)
	if err != nil {
		return ""
	}
	return string(yamlBytes)
}

// ConfigAny returns the configuration of a walked component in any form.
func (w *WalkedComponent) ConfigAny() (any, error) {
	var v any
	switch t := w.conf.(type) {
	case *yaml.Node:
		if err := t.Decode(&v); err != nil {
			return nil, err
		}
	default:
		v = t
	}
	return v, nil
}

// WalkComponentsAny attempts walk the structure of a parsed config, calling
// a provided function for each component found within the config.
func (s *StreamConfigWalker) WalkComponentsAny(v any, fn func(w *WalkedComponent) error) error {
	conf := docs.WalkComponentConfig{
		Provider: s.env.internal,
		Func: func(c docs.WalkedComponent) error {
			tmpErr := fn(walkedComponentFromInternal(c))
			if errors.Is(tmpErr, ErrSkipComponents) {
				tmpErr = docs.ErrSkipChildComponents
			}
			return tmpErr
		},
	}

	return s.spec.WalkComponentsAny(conf, v)
}

// WalkComponentsYAML attempts to parse a YAML config and walk its structure,
// calling a provided function for each component found within the config.
func (s *StreamConfigWalker) WalkComponentsYAML(confYAML []byte, fn func(w *WalkedComponent) error) error {
	node, err := docs.UnmarshalYAML(confYAML)
	if err != nil {
		return err
	}

	conf := docs.WalkComponentConfig{
		Provider: s.env.internal,
		Func: func(c docs.WalkedComponent) error {
			tmpErr := fn(walkedComponentFromInternal(c))
			if errors.Is(tmpErr, ErrSkipComponents) {
				tmpErr = docs.ErrSkipChildComponents
			}
			return tmpErr
		},
	}

	return s.spec.WalkComponentsYAML(conf, node)
}
