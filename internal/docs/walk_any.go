package docs

import (
	"errors"
	"fmt"
	"strconv"

	"gopkg.in/yaml.v3"
)

// ErrSkipChildComponents is used as a return value from WalkComponentsYAML to
// indicate that the component currently viewed should not have its child fields
// walked. It is not returned as an error by any function.
var ErrSkipChildComponents = errors.New("skip children")

// WalkComponentFunc is called for each component type within a YAML config,
// where the node representing that component is provided along with the type
// and implementation name.
type WalkComponentFunc func(c WalkedComponent) error

// WalkComponentConfig controls the behaviour of a walk function.
type WalkComponentConfig struct {
	path     string
	Provider Provider
	Func     WalkComponentFunc
}

func (w WalkComponentConfig) intoPath(str string) WalkComponentConfig {
	tmp := w
	if tmp.path != "" {
		tmp.path += "." + str
	} else {
		tmp.path = str
	}
	return tmp
}

// WalkedComponent is a struct containing information about a component yielded
// via the WalkComponentsX methods.
type WalkedComponent struct {
	Field FieldSpec
	Path  string
	Label string
	Name  string
	Value any // *yaml.Node

	LineStart int
	LineEnd   int

	spec ComponentSpec
	conf WalkComponentConfig
}

// WalkComponents walks each child field of a given node and for any component
// types within the config the provided func is called.
func (w WalkedComponent) WalkComponents(fn WalkComponentFunc) error {
	switch t := w.Value.(type) {
	case *yaml.Node:
		return w.walkComponentsYAML(t, fn)
	case map[string]any:
		return w.walkComponentsAny(t, fn)
	}
	return nil
}

func (w WalkedComponent) walkComponentsAny(vMap map[string]any, fn WalkComponentFunc) error {
	tmpConf := w.conf
	tmpConf.Func = fn

	reservedFields := ReservedFieldsByType(w.spec.Type)
	for k, v := range vMap {
		if k == w.Name {
			if err := w.spec.Config.WalkComponentsAny(tmpConf.intoPath(w.Name), v); err != nil {
				return err
			}
			continue
		}
		if k == "type" || k == "label" {
			continue
		}
		if spec, exists := reservedFields[k]; exists {
			if err := spec.WalkComponentsAny(tmpConf.intoPath(spec.Name), v); err != nil {
				return err
			}
		}
	}
	return nil
}

func walkComponentAny(conf WalkComponentConfig, coreType Type, f FieldSpec, value any) error {
	switch t := value.(type) {
	case *yaml.Node:
		return walkComponentYAML(conf, coreType, f, t)
	case map[string]any:
		return walkComponentAnyMap(conf, coreType, f, t)
	}
	return nil
}

func walkComponentAnyMap(conf WalkComponentConfig, coreType Type, f FieldSpec, value map[string]any) error {
	name, spec, err := GetInferenceCandidateFromMap(conf.Provider, coreType, value)
	if err != nil {
		return err
	}

	label, _ := value["label"].(string)
	if err := conf.Func(WalkedComponent{
		Field: f,
		Label: label,
		Name:  name,
		Path:  conf.path,
		Value: value,

		spec: spec,
		conf: conf,
	}); err != nil {
		if errors.Is(err, ErrSkipChildComponents) {
			err = nil
		}
		return err
	}

	reservedFields := ReservedFieldsByType(coreType)
	for k, v := range value {
		if k == name {
			if err := spec.Config.WalkComponentsAny(conf.intoPath(name), v); err != nil {
				return err
			}
			continue
		}
		if k == "type" || k == "label" {
			continue
		}
		if spec, exists := reservedFields[k]; exists {
			if err := spec.WalkComponentsAny(conf.intoPath(spec.Name), v); err != nil {
				return err
			}
		}
	}
	return nil
}

// WalkComponentsAny walks each node of a generic tree and for any component
// types within the config a provided func is called.
func (f FieldSpec) WalkComponentsAny(conf WalkComponentConfig, value any) error {
	if coreType, isCore := f.Type.IsCoreComponent(); isCore {
		switch f.Kind {
		case Kind2DArray:
			valueArray, ok := value.([]any)
			if !ok {
				return fmt.Errorf("path %v field %v expected array, found %T", conf.path, f.Name, value)
			}
			for i, ia := range valueArray {
				innerArray, ok := ia.([]any)
				if !ok {
					return fmt.Errorf("path %v field %v expected array, found %T", conf.path, f.Name, ia)
				}
				for j, v := range innerArray {
					if err := walkComponentAny(conf.intoPath(fmt.Sprintf("%v.%v", i, j)), coreType, f, v); err != nil {
						return err
					}
				}
			}
		case KindArray:
			valueArray, ok := value.([]any)
			if !ok {
				return fmt.Errorf("path %v field %v expected array, found %T", conf.path, f.Name, value)
			}
			for i, v := range valueArray {
				if err := walkComponentAny(conf.intoPath(strconv.Itoa(i)), coreType, f, v); err != nil {
					return err
				}
			}
		case KindMap:
			valueMap, ok := value.(map[string]any)
			if !ok {
				return fmt.Errorf("path %v field %v expected object, found %T", conf.path, f.Name, value)
			}
			for k, v := range valueMap {
				if err := walkComponentAny(conf.intoPath(k), coreType, f, v); err != nil {
					return err
				}
			}
		default:
			if err := walkComponentAny(conf, coreType, f, value); err != nil {
				return err
			}
		}
	} else if len(f.Children) > 0 {
		switch f.Kind {
		case Kind2DArray:
			valueArray, ok := value.([]any)
			if !ok {
				return fmt.Errorf("path %v field %v expected array, found %T", conf.path, f.Name, value)
			}
			for i, ia := range valueArray {
				innerArray, ok := ia.([]any)
				if !ok {
					return fmt.Errorf("path %v field %v expected array, found %T", conf.path, f.Name, ia)
				}
				for j, v := range innerArray {
					if err := f.Children.WalkComponentsAny(conf.intoPath(fmt.Sprintf("%v.%v", i, j)), v); err != nil {
						return err
					}
				}
			}
		case KindArray:
			valueArray, ok := value.([]any)
			if !ok {
				return fmt.Errorf("path %v field %v expected array, found %T", conf.path, f.Name, value)
			}
			for i, v := range valueArray {
				if err := f.Children.WalkComponentsAny(conf.intoPath(strconv.Itoa(i)), v); err != nil {
					return err
				}
			}
		case KindMap:
			valueMap, ok := value.(map[string]any)
			if !ok {
				return fmt.Errorf("path %v field %v expected object, found %T", conf.path, f.Name, value)
			}
			for k, v := range valueMap {
				if err := f.Children.WalkComponentsAny(conf.intoPath(k), v); err != nil {
					return err
				}
			}
		default:
			if err := f.Children.WalkComponentsAny(conf, value); err != nil {
				return err
			}
		}
	}
	return nil
}

// WalkComponentsAny walks each node of a generic tree and for any component
// types within the config a provided func is called.
func (f FieldSpecs) WalkComponentsAny(conf WalkComponentConfig, m any) error {
	switch t := m.(type) {
	case *yaml.Node:
		return f.WalkComponentsYAML(conf, t)
	case map[string]any:
		return f.walkComponentsAnyMap(conf, t)
	}
	return nil
}

func (f FieldSpecs) walkComponentsAnyMap(conf WalkComponentConfig, m map[string]any) error {
	// Following the order of our field specs, walk each field.
	for _, field := range f {
		value, exists := m[field.Name]
		if !exists {
			continue
		}
		if err := field.WalkComponentsAny(conf.intoPath(field.Name), value); err != nil {
			return err
		}
	}
	return nil
}
