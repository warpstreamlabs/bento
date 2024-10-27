package parquet

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/warpstreamlabs/bento/public/service"
)

func GenerateStructType(config *service.ParsedConfig) (reflect.Type, error) {
	fields, err := config.FieldAnyList("schema")
	if err != nil {
		return nil, fmt.Errorf("getting schema fields: %w", err)
	}

	return generateStructTypeFromFields(fields)
}

func generateStructTypeFromFields(fields []*service.ParsedConfig) (reflect.Type, error) {
	var structFields []reflect.StructField

	for _, field := range fields {
		name, err := field.FieldString("name")
		if err != nil {
			return nil, fmt.Errorf("getting field name: %w", err)
		}

		var (
			exportedName = strings.ToUpper(name[:1]) + name[1:]
			components   = []string{name}
		)

		if field.Contains("type") {
			typeStr, err := field.FieldString("type")
			if err != nil {
				return nil, fmt.Errorf("failed to read field type, err: %w", err)
			}

			if typeStr == "LIST" {
				components = append(components, "list")
			}

			if typeStr == "DECIMAL32" || typeStr == "DECIMAL64" {
				scale, err := field.FieldInt("decimal_scale")
				if err != nil {
					return nil, fmt.Errorf("failed to read decimal_scale, err: %w", err)
				}

				precision, err := field.FieldInt("decimal_precision")
				if err != nil {
					return nil, fmt.Errorf("failed to read decimal_precision, err: %w", err)
				}

				components = append(components, fmt.Sprintf("decimal(%d:%d)", scale, precision))
			}
		}

		fieldType, err := generateFieldType(field)
		if err != nil {
			return nil, fmt.Errorf("generating type for field %q: %w", name, err)
		}

		parquetTag := strings.Join(components, ",")

		structField := reflect.StructField{
			Name: exportedName,
			Type: fieldType,
			Tag: reflect.StructTag(fmt.Sprintf(
				`parquet:"%s" json:"%s"`,
				parquetTag, name)),
		}
		structFields = append(structFields, structField)
	}

	return reflect.StructOf(structFields), nil
}

func generateFieldType(field *service.ParsedConfig) (reflect.Type, error) {
	if field.Contains("type") {
		typeStr, err := field.FieldString("type")
		if err != nil {
			return nil, fmt.Errorf("getting field type: %w", err)
		}

		var outputType reflect.Type
		switch typeStr {
		case "MAP":
			outputType, err = generateMapType(field)
			if err != nil {
				return nil, err
			}
		case "LIST":
			outputType, err = generateListType(field)
			if err != nil {
				return nil, err
			}
		default:
			outputType, err = getReflectType(typeStr)
			if err != nil {
				return nil, err
			}
		}

		return wrapType(outputType, field)
	}

	// Handle nested struct
	if field.Contains("fields") {
		subfields, err := field.FieldAnyList("fields")
		if err != nil {
			return nil, fmt.Errorf("getting subfields: %w", err)
		}

		structType, err := generateStructTypeFromFields(subfields)
		if err != nil {
			return nil, fmt.Errorf("generating nested struct: %w", err)
		}

		return wrapType(structType, field)
	}

	return nil, fmt.Errorf("field has neither type nor fields")
}

func generateListType(field *service.ParsedConfig) (reflect.Type, error) {
	fields, err := field.FieldAnyList("fields")
	if err != nil {
		return nil, fmt.Errorf("getting map fields: %w", err)
	}

	if len(fields) != 1 {
		return nil, fmt.Errorf("list type must have exactly one field (element), got %d", len(fields))
	}

	elementType, err := generateFieldType(fields[0])
	if err != nil {
		return nil, fmt.Errorf("generating map key type: %w", err)
	}

	return reflect.SliceOf(elementType), nil
}

func generateMapType(field *service.ParsedConfig) (reflect.Type, error) {
	fields, err := field.FieldAnyList("fields")
	if err != nil {
		return nil, fmt.Errorf("getting map fields: %w", err)
	}

	if len(fields) != 2 {
		return nil, fmt.Errorf("map type must have exactly two fields (key and value), got %d", len(fields))
	}

	keyType, err := generateFieldType(fields[0])
	if err != nil {
		return nil, fmt.Errorf("generating map key type: %w", err)
	}

	valueType, err := generateFieldType(fields[1])
	if err != nil {
		return nil, fmt.Errorf("generating map value type: %w", err)
	}

	return reflect.MapOf(keyType, valueType), nil
}

func getReflectType(typeStr string) (reflect.Type, error) {
	switch strings.ToUpper(typeStr) {
	case "UTF8":
		return reflect.TypeOf(""), nil
	case "BYTE_ARRAY":
		return reflect.TypeOf([]byte(nil)), nil
	case "INT32":
		return reflect.TypeOf(int32(0)), nil
	case "INT64":
		return reflect.TypeOf(int64(0)), nil
	case "FLOAT":
		return reflect.TypeOf(float32(0)), nil
	case "BOOLEAN":
		return reflect.TypeOf(false), nil
	case "DOUBLE":
		return reflect.TypeOf(float64(0)), nil
	case "DECIMAL32":
		return reflect.TypeOf(int32(0)), nil
	case "DECIMAL64":
		return reflect.TypeOf(int64(0)), nil
	default:
		return nil, fmt.Errorf("unsupported type: %s", typeStr)
	}
}

func wrapType(baseType reflect.Type, field *service.ParsedConfig) (reflect.Type, error) {
	if field.Contains("repeated") {
		repeated, err := field.FieldBool("repeated")
		if err != nil {
			return nil, fmt.Errorf("getting repeated flag: %w", err)
		}
		if repeated {
			return reflect.SliceOf(baseType), nil
		}
	}

	if field.Contains("optional") {
		optional, err := field.FieldBool("optional")
		if err != nil {
			return nil, fmt.Errorf("getting optional flag: %w", err)
		}
		if optional {
			return reflect.PtrTo(baseType), nil
		}
	}

	return baseType, nil
}
