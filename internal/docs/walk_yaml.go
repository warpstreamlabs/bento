package docs

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

func getYAMLLastLine(node *yaml.Node) int {
	if len(node.Content) == 0 {
		lines := strings.Count(node.Value, "\n")
		return node.Line + lines
	}
	return getYAMLLastLine(node.Content[len(node.Content)-1])
}

// walkComponentsYAML walks each child field of a given node and for any
// component types within the config the provided func is called.
func (w WalkedComponent) walkComponentsYAML(v *yaml.Node, fn WalkComponentFunc) error {
	tmpConf := w.conf
	tmpConf.Func = fn

	reservedFields := ReservedFieldsByType(w.spec.Type)
	for i := 0; i < len(v.Content)-1; i += 2 {
		if v.Content[i].Value == w.Name {
			if err := w.spec.Config.WalkComponentsYAML(tmpConf.intoPath(w.Name), v.Content[i+1]); err != nil {
				return err
			}
			continue
		}
		if v.Content[i].Value == "type" || v.Content[i].Value == "label" {
			continue
		}
		if spec, exists := reservedFields[v.Content[i].Value]; exists {
			if err := spec.WalkComponentsYAML(tmpConf.intoPath(spec.Name), v.Content[i+1]); err != nil {
				return err
			}
		}
	}
	return nil
}

func walkComponentYAML(conf WalkComponentConfig, coreType Type, f FieldSpec, node *yaml.Node) error {
	node = unwrapDocumentNode(node)

	name, spec, err := GetInferenceCandidateFromYAML(conf.Provider, coreType, node)
	if err != nil {
		return err
	}

	var label string
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == "label" {
			label = node.Content[i+1].Value
			break
		}
	}

	if err := conf.Func(WalkedComponent{
		Field:     f,
		Label:     label,
		Name:      name,
		Path:      conf.path,
		Value:     node,
		LineStart: node.Line,
		LineEnd:   getYAMLLastLine(node),

		spec: spec,
		conf: conf,
	}); err != nil {
		if errors.Is(err, ErrSkipChildComponents) {
			err = nil
		}
		return err
	}

	reservedFields := ReservedFieldsByType(coreType)
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == name {
			if err := spec.Config.WalkComponentsYAML(conf.intoPath(name), node.Content[i+1]); err != nil {
				return err
			}
			continue
		}
		if node.Content[i].Value == "type" || node.Content[i].Value == "label" {
			continue
		}
		if spec, exists := reservedFields[node.Content[i].Value]; exists {
			if err := spec.WalkComponentsYAML(conf.intoPath(spec.Name), node.Content[i+1]); err != nil {
				return err
			}
		}
	}
	return nil
}

// WalkComponentsYAML walks each node of a YAML tree and for any component types
// within the config a provided func is called.
func (f FieldSpec) WalkComponentsYAML(conf WalkComponentConfig, node *yaml.Node) error {
	node = unwrapDocumentNode(node)

	if coreType, isCore := f.Type.IsCoreComponent(); isCore {
		switch f.Kind {
		case Kind2DArray:
			for i := 0; i < len(node.Content); i++ {
				for j := 0; j < len(node.Content[i].Content); j++ {
					if err := walkComponentYAML(conf.intoPath(fmt.Sprintf("%v.%v", i, j)), coreType, f, node.Content[i].Content[j]); err != nil {
						return err
					}
				}
			}
		case KindArray:
			for i := 0; i < len(node.Content); i++ {
				if err := walkComponentYAML(conf.intoPath(strconv.Itoa(i)), coreType, f, node.Content[i]); err != nil {
					return err
				}
			}
		case KindMap:
			for i := 0; i < len(node.Content)-1; i += 2 {
				if err := walkComponentYAML(conf.intoPath(node.Content[i].Value), coreType, f, node.Content[i+1]); err != nil {
					return err
				}
			}
		default:
			if err := walkComponentYAML(conf, coreType, f, node); err != nil {
				return err
			}
		}
	} else if len(f.Children) > 0 {
		switch f.Kind {
		case Kind2DArray:
			for i := 0; i < len(node.Content); i++ {
				for j := 0; j < len(node.Content[i].Content); j++ {
					if err := f.Children.WalkComponentsYAML(conf.intoPath(fmt.Sprintf("%v.%v", i, j)), node.Content[i].Content[j]); err != nil {
						return err
					}
				}
			}
		case KindArray:
			for i := 0; i < len(node.Content); i++ {
				if err := f.Children.WalkComponentsYAML(conf.intoPath(strconv.Itoa(i)), node.Content[i]); err != nil {
					return err
				}
			}
		case KindMap:
			for i := 0; i < len(node.Content)-1; i += 2 {
				if err := f.Children.WalkComponentsYAML(conf.intoPath(node.Content[i].Value), node.Content[i+1]); err != nil {
					return err
				}
			}
		default:
			if err := f.Children.WalkComponentsYAML(conf, node); err != nil {
				return err
			}
		}
	}
	return nil
}

// WalkComponentsYAML walks each node of a YAML tree and for any component types
// within the config a provided func is called.
func (f FieldSpecs) WalkComponentsYAML(conf WalkComponentConfig, node *yaml.Node) error {
	node = unwrapDocumentNode(node)

	nodeKeys := map[string]*yaml.Node{}
	for i := 0; i < len(node.Content)-1; i += 2 {
		nodeKeys[node.Content[i].Value] = node.Content[i+1]
	}

	// Following the order of our field specs, walk each field.
	for _, field := range f {
		value, exists := nodeKeys[field.Name]
		if !exists {
			continue
		}
		if err := field.WalkComponentsYAML(conf.intoPath(field.Name), value); err != nil {
			return err
		}
	}
	return nil
}
