package parquet

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/warpstreamlabs/bento/public/service"
)

type schemaOpts struct {
	optionalsAsStructTags bool
	optionalAsPtrs        bool
	defaultEncoding       string
}

func GenerateStructType(
	config *service.ParsedConfig,
	schemaOpts schemaOpts,
) (reflect.Type, error) {
	fields, err := config.FieldAnyList("schema")
	if err != nil {
		return nil, fmt.Errorf("getting schema fields: %w", err)
	}

	return generateStructTypeFromFields(fields, schemaOpts)
}

func generateStructTypeFromFields(
	fields []*service.ParsedConfig,
	schemaOpts schemaOpts,
) (reflect.Type, error) {
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

			// Change the default encoding from DELTA_LENGTH_BYTE_ARRAY to PLAIN for string fields
			// See https://github.com/parquet-go/parquet-go/issues/50#issuecomment-1667639181
			if schemaOpts.defaultEncoding == "plain" && isDeltaLengthByteArrayEncodable(typeStr) {
				components = append(components, schemaOpts.defaultEncoding)
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

		if schemaOpts.optionalsAsStructTags {
			if field.Contains("optional") {
				optional, err := field.FieldBool("optional")
				if err != nil {
					return nil, fmt.Errorf("getting optional flag: %w", err)
				}
				if optional {
					components = append(components, "optional")
				}
			}
		}

		fieldType, err := generateFieldType(field, schemaOpts)
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

func generateFieldType(
	field *service.ParsedConfig,
	schemaOpts schemaOpts,
) (reflect.Type, error) {
	if field.Contains("type") {
		typeStr, err := field.FieldString("type")
		if err != nil {
			return nil, fmt.Errorf("getting field type: %w", err)
		}

		var outputType reflect.Type
		switch typeStr {
		case "MAP":
			outputType, err = generateMapType(field, schemaOpts)
			if err != nil {
				return nil, err
			}
		case "LIST":
			outputType, err = generateListType(field, schemaOpts)
			if err != nil {
				return nil, err
			}
		default:
			outputType, err = getReflectType(typeStr)
			if err != nil {
				return nil, err
			}
		}

		return wrapType(outputType, field, schemaOpts)
	}

	// Handle nested struct
	if field.Contains("fields") {
		subfields, err := field.FieldAnyList("fields")
		if err != nil {
			return nil, fmt.Errorf("getting subfields: %w", err)
		}

		structType, err := generateStructTypeFromFields(subfields, schemaOpts)
		if err != nil {
			return nil, fmt.Errorf("generating nested struct: %w", err)
		}

		return wrapType(structType, field, schemaOpts)
	}

	return nil, errors.New("field has neither type nor fields")
}

func generateListType(
	field *service.ParsedConfig,
	schemaOpts schemaOpts,
) (reflect.Type, error) {
	fields, err := field.FieldAnyList("fields")
	if err != nil {
		return nil, fmt.Errorf("getting list fields: %w", err)
	}

	if len(fields) != 1 {
		return nil, fmt.Errorf("list type must have exactly one field (element), got %d", len(fields))
	}

	elementType, err := generateFieldType(fields[0], schemaOpts)
	if err != nil {
		return nil, fmt.Errorf("generating list key type: %w", err)
	}

	return reflect.SliceOf(elementType), nil
}

func generateMapType(
	field *service.ParsedConfig,
	schemaOpts schemaOpts,
) (reflect.Type, error) {
	fields, err := field.FieldAnyList("fields")
	if err != nil {
		return nil, fmt.Errorf("getting map fields: %w", err)
	}

	if len(fields) != 2 {
		return nil, fmt.Errorf("map type must have exactly two fields (key and value), got %d", len(fields))
	}

	keyType, err := generateFieldType(fields[0], schemaOpts)
	if err != nil {
		return nil, fmt.Errorf("generating map key type: %w", err)
	}

	valueType, err := generateFieldType(fields[1], schemaOpts)
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
	case "INT8":
		return reflect.TypeOf(int8(0)), nil
	case "INT16":
		return reflect.TypeOf(int16(0)), nil
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

func wrapType(
	baseType reflect.Type,
	field *service.ParsedConfig,
	schemaOpts schemaOpts,
) (reflect.Type, error) {
	if field.Contains("repeated") {
		repeated, err := field.FieldBool("repeated")
		if err != nil {
			return nil, fmt.Errorf("getting repeated flag: %w", err)
		}
		if repeated {
			return reflect.SliceOf(baseType), nil
		}
	}

	if schemaOpts.optionalAsPtrs {
		if field.Contains("optional") {
			optional, err := field.FieldBool("optional")
			if err != nil {
				return nil, fmt.Errorf("getting optional flag: %w", err)
			}
			if optional {
				return reflect.PointerTo(baseType), nil
			}
		}
	}

	return baseType, nil
}

func isDeltaLengthByteArrayEncodable(typeStr string) bool {
	switch typeStr {
	case "BYTE_ARRAY", "UTF8":
		return true
	}
	return false
}
