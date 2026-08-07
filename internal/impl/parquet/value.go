package parquet

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/parquet-go/parquet-go"

	"github.com/warpstreamlabs/bento/public/service"
)

// GenerateSchema builds a parquet schema directly from the `schema` config
// instead of round-tripping through reflect.StructOf and parquet struct tags.
//
// The struct tag approach can only describe one level of list nesting, because
// `parquet-element` is a struct tag and a field only gets one of them. Nodes
// compose, so LIST nesting is just recursion.
func GenerateSchema(config *service.ParsedConfig, opts schemaOpts) (*parquet.Schema, error) {
	fields, err := config.FieldAnyList("schema")
	if err != nil {
		return nil, fmt.Errorf("getting schema fields: %w", err)
	}

	root, err := groupNodeOf(fields, opts)
	if err != nil {
		return nil, err
	}

	// parquet.SchemaOf on an anonymous reflect.StructOf type yields an empty
	// name, so keep it empty to avoid changing existing output.
	return parquet.NewSchema("", root), nil
}

func groupNodeOf(fields []*service.ParsedConfig, opts schemaOpts) (parquet.Node, error) {
	if len(fields) == 0 {
		return nil, errors.New("group requires at least one field")
	}

	group := &orderedGroup{
		Group:  make(parquet.Group, len(fields)),
		fields: make([]parquet.Field, 0, len(fields)),
	}

	for _, field := range fields {
		name, err := field.FieldString("name")
		if err != nil {
			return nil, fmt.Errorf("getting field name: %w", err)
		}

		node, err := nodeOf(field, opts)
		if err != nil {
			return nil, fmt.Errorf("generating node for field %q: %w", name, err)
		}

		group.Group[name] = node
		group.fields = append(group.fields, &groupField{Node: node, name: name})
	}

	return group, nil
}

// nodeOf builds the node for a single schema entry, applying repetition last so
// the wrapping order matches parquet-go's own makeNodeOf: encoding, then list,
// then repeated/optional.
func nodeOf(field *service.ParsedConfig, opts schemaOpts) (parquet.Node, error) {
	node, err := baseNodeOf(field, opts)
	if err != nil {
		return nil, err
	}

	// Nested `fields` entries come from an AnyListField, so defaults are not
	// applied and the keys may genuinely be absent. Hence the Contains guards.
	if field.Contains("repeated") {
		repeated, err := field.FieldBool("repeated")
		if err != nil {
			return nil, fmt.Errorf("getting repeated flag: %w", err)
		}
		if repeated {
			return parquet.Repeated(node), nil
		}
	}

	if field.Contains("optional") {
		optional, err := field.FieldBool("optional")
		if err != nil {
			return nil, fmt.Errorf("getting optional flag: %w", err)
		}
		if optional {
			return parquet.Optional(node), nil
		}
	}

	return node, nil
}

func baseNodeOf(field *service.ParsedConfig, opts schemaOpts) (parquet.Node, error) {
	if !field.Contains("type") {
		// A field with children but no type is a struct (backwards compatibility).
		if !field.Contains("fields") {
			return nil, errors.New("field has neither type nor fields")
		}
		return childGroupOf(field, opts)
	}

	typeStr, err := field.FieldString("type")
	if err != nil {
		return nil, fmt.Errorf("getting field type: %w", err)
	}

	switch typeStr {
	case "STRUCT":
		return childGroupOf(field, opts)

	case "LIST":
		children, err := field.FieldAnyList("fields")
		if err != nil {
			return nil, fmt.Errorf("getting list fields: %w", err)
		}
		if len(children) != 1 {
			return nil, fmt.Errorf("list type must have exactly one field (element), got %d", len(children))
		}

		// This is the entire nested-list fix: parquet.List takes a Node, so a
		// list of lists of lists is three calls deep and nothing has to be
		// encoded into a tag string.
		element, err := nodeOf(children[0], opts)
		if err != nil {
			return nil, fmt.Errorf("generating list element: %w", err)
		}
		return parquet.List(element), nil

	case "MAP":
		children, err := field.FieldAnyList("fields")
		if err != nil {
			return nil, fmt.Errorf("getting map fields: %w", err)
		}
		if len(children) != 2 {
			return nil, fmt.Errorf("map type must have exactly two fields (key and value), got %d", len(children))
		}

		key, err := nodeOf(children[0], opts)
		if err != nil {
			return nil, fmt.Errorf("generating map key: %w", err)
		}
		value, err := nodeOf(children[1], opts)
		if err != nil {
			return nil, fmt.Errorf("generating map value: %w", err)
		}
		return parquet.Map(key, value), nil

	case "DECIMAL32", "DECIMAL64":
		scale, err := field.FieldInt("decimal_scale")
		if err != nil {
			return nil, fmt.Errorf("getting decimal_scale: %w", err)
		}
		precision, err := field.FieldInt("decimal_precision")
		if err != nil {
			return nil, fmt.Errorf("getting decimal_precision: %w", err)
		}

		baseType := parquet.Int32Type
		if typeStr == "DECIMAL64" {
			baseType = parquet.Int64Type
		}
		return parquet.Decimal(scale, precision, baseType), nil

	default:
		return leafNodeOf(typeStr, opts)
	}
}

func childGroupOf(field *service.ParsedConfig, opts schemaOpts) (parquet.Node, error) {
	children, err := field.FieldAnyList("fields")
	if err != nil {
		return nil, fmt.Errorf("struct type requires 'fields' to be specified: %w", err)
	}
	return groupNodeOf(children, opts)
}

// leafNodeOf mirrors what parquet-go's nodeOf derives from the equivalent Go
// kind, so the emitted logical types are unchanged from the reflect path.
func leafNodeOf(typeStr string, opts schemaOpts) (parquet.Node, error) {
	var node parquet.Node

	switch strings.ToUpper(typeStr) {
	case "BOOLEAN":
		node = parquet.Leaf(parquet.BooleanType)
	case "INT8":
		node = parquet.Int(8)
	case "INT16":
		node = parquet.Int(16)
	case "INT32":
		node = parquet.Int(32)
	case "INT64":
		node = parquet.Int(64)
	case "FLOAT":
		node = parquet.Leaf(parquet.FloatType)
	case "DOUBLE":
		node = parquet.Leaf(parquet.DoubleType)
	case "UTF8":
		node = parquet.String()
	case "BYTE_ARRAY":
		node = parquet.Leaf(parquet.ByteArrayType)
	default:
		return nil, fmt.Errorf("unsupported type: %s", typeStr)
	}

	// Replaces the `plain` struct tag. parquet-go defaults byte array columns to
	// DELTA_LENGTH_BYTE_ARRAY, which some readers do not support.
	// See https://github.com/parquet-go/parquet-go/issues/50
	if opts.defaultEncoding == "plain" && isDeltaLengthByteArrayEncodable(typeStr) {
		node = parquet.Encoded(node, &parquet.Plain)
	}

	return node, nil
}

//------------------------------------------------------------------------------

// parquet.Group is a map[string]Node, so Group.Fields sorts by name. The schema
// config is an ordered list and column order is visible to anything reading the
// file positionally, so keep declaration order.
//
// The embedded Group still backs Type, String and GoType. GoType returns an
// alphabetically ordered struct, which is fine because rows are deconstructed
// through Field.Value rather than by field index.
type orderedGroup struct {
	parquet.Group
	fields []parquet.Field
}

func (g *orderedGroup) Fields() []parquet.Field { return g.fields }

// groupField associates a node with its name in the parent group and resolves
// the matching Go value. Lookup is by parquet tag name, which is what lets the
// generated row structs keep mangled Go field names.
type groupField struct {
	parquet.Node
	name string
}

func (f *groupField) Name() string { return f.name }

func (f *groupField) Value(base reflect.Value) reflect.Value {
	if base.Kind() == reflect.Interface {
		if base.IsNil() {
			return reflect.Value{}
		}
		base = base.Elem()
	}

	switch base.Kind() {
	case reflect.Pointer:
		if base.IsNil() {
			if !base.CanSet() {
				return reflect.Value{}
			}
			base.Set(reflect.New(base.Type().Elem()))
		}
		return fieldByParquetName(base.Elem(), f.name)
	case reflect.Struct:
		return fieldByParquetName(base, f.name)
	case reflect.Map:
		return base.MapIndex(reflect.ValueOf(f.name))
	default:
		return reflect.Value{}
	}
}

func fieldByParquetName(base reflect.Value, name string) reflect.Value {
	t := base.Type()
	for i := range t.NumField() {
		field := t.Field(i)
		if tag := field.Tag.Get("parquet"); tag != "" {
			if tagName, _, _ := strings.Cut(tag, ","); tagName == name {
				return base.Field(i)
			}
		}
		if field.Name == name {
			return base.Field(i)
		}
	}
	return reflect.Value{}
}
