package parquet

import (
	"errors"
	"fmt"
	"reflect"
)

// MapToStruct converts a map[string]any to a struct using reflection.
// The dest parameter must be a pointer to a struct.
func MapToStruct(m map[string]any, dest any) error {
	destValue := reflect.ValueOf(dest)
	if destValue.Kind() != reflect.Ptr {
		return fmt.Errorf("destination must be a pointer to struct, got %v", destValue.Kind())
	}

	destElem := destValue.Elem()
	if destElem.Kind() != reflect.Struct {
		return fmt.Errorf("destination must be a pointer to struct, got pointer to %v", destElem.Kind())
	}

	return mapToValue(m, destElem)
}

// mapToValue handles the recursive conversion of map values to struct fields
func mapToValue(m map[string]any, destValue reflect.Value) error {
	destType := destValue.Type()

	for i := 0; i < destType.NumField(); i++ {
		field := destType.Field(i)
		fieldValue := destValue.Field(i)

		if !fieldValue.CanSet() {
			continue // Skip unexported fields
		}

		mapKey := field.Name
		if tag := field.Tag.Get("json"); tag != "" {
			mapKey = tag
		}

		mapValue, exists := m[mapKey]
		if !exists {
			continue // Skip fields not present in map
		}

		if err := setField(fieldValue, mapValue); err != nil {
			return fmt.Errorf("failed to set field %s: %w", field.Name, err)
		}
	}

	return nil
}

// setField sets the appropriate value for a struct field based on its type
func setField(field reflect.Value, value any) error {
	if value == nil {
		return nil
	}

	fieldType := field.Type()

	// Handle pointer types
	if field.Kind() == reflect.Ptr {
		if field.IsNil() {
			field.Set(reflect.New(fieldType.Elem()))
		}
		return setField(field.Elem(), value)
	}

	switch field.Kind() {
	case reflect.Interface:
		// For interface{} fields, we can directly set the value
		field.Set(reflect.ValueOf(value))
		return nil
	case reflect.Int32:
		switch v := value.(type) {
		case int:
			field.SetInt(int64(v))
		case int64:
			if int64(int32(v)) == v {
				field.SetInt(v)
				return nil
			}
			return fmt.Errorf("cannot represent %v as int32", value)
		case float64:
			if float64(int32(v)) == v {
				field.SetInt(int64(v))
				return nil
			}
			return fmt.Errorf("cannot represent %v as int32", value)
		default:
			return fmt.Errorf("cannot convert %T to int64", value)
		}

	case reflect.Int64:
		switch v := value.(type) {
		case int:
			field.SetInt(int64(v))
		case int64:
			field.SetInt(v)
		case float64:
			field.SetInt(int64(v))
		default:
			return fmt.Errorf("cannot convert %T to int64", value)
		}

	case reflect.Float64:
		switch v := value.(type) {
		case float32:
			field.SetFloat(float64(v))
		case float64:
			field.SetFloat(v)
		case int:
			if int(float64(v)) == v {
				field.SetFloat(float64(v))
				return nil
			}
			return fmt.Errorf("cannot represent %v as float64", value)
		case int64:
			if int64(float64(v)) == v {
				field.SetFloat(float64(v))
				return nil
			}
			return fmt.Errorf("cannot represent %v as float64", value)
		default:
			return fmt.Errorf("cannot convert %T to float64", value)
		}
	case reflect.Float32:
		switch v := value.(type) {
		case float64:
			if float64(float32(v)) == v {
				field.SetFloat(v)
				return nil
			}
			return fmt.Errorf("cannot represent %v as float32", value)
		case int:
			if int(float32(v)) == v {
				field.SetFloat(float64(v))
				return nil
			}
			return fmt.Errorf("cannot represent %v as float32", value)
		case int64:
			if int64(float32(v)) == v {
				field.SetFloat(float64(v))
				return nil
			}
			return fmt.Errorf("cannot represent %v as float32", value)
		default:
			return fmt.Errorf("cannot convert %T to float64", value)
		}
	case reflect.Bool:
		if v, ok := value.(bool); ok {
			field.SetBool(v)
		} else {
			return fmt.Errorf("cannot convert %T to bool", value)
		}

	case reflect.String:
		if v, ok := value.(string); ok {
			field.SetString(v)
		} else if v, ok := value.([]byte); ok {
			field.SetString(string(v))
		} else {
			return fmt.Errorf("cannot convert %T to string", value)
		}

	case reflect.Struct:
		if m, ok := value.(map[string]any); ok {
			return mapToValue(m, field)
		}
		return fmt.Errorf("cannot convert %T to struct", value)

	case reflect.Slice:
		// Fast path for string->[]byte.
		str, ok := value.(string)
		if ok && field.Type() == reflect.TypeOf([]byte(nil)) {
			field.Set(reflect.ValueOf([]byte(str)))
			return nil
		}

		valueSlice, ok := value.([]any)
		if !ok {
			return fmt.Errorf("cannot convert %T to slice", value)
		}

		sliceValue := reflect.MakeSlice(fieldType, len(valueSlice), len(valueSlice))

		for i, elem := range valueSlice {
			if err := setField(sliceValue.Index(i), elem); err != nil {
				return fmt.Errorf("error setting slice element %d: %w", i, err)
			}
		}
		field.Set(sliceValue)
	case reflect.Map:
		if fieldType.Key().Kind() != reflect.String {
			return errors.New("only string keys are supported for maps")
		}

		valueMap, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("cannot convert %T to map", value)
		}

		mapValue := reflect.MakeMap(fieldType)
		elemType := fieldType.Elem()

		for k, v := range valueMap {
			elemValue := reflect.New(elemType).Elem()
			if err := setField(elemValue, v); err != nil {
				return fmt.Errorf("error setting map value for key %s: %w", k, err)
			}
			mapValue.SetMapIndex(reflect.ValueOf(k), elemValue)
		}
		field.Set(mapValue)

	default:
		return fmt.Errorf("unsupported field type: %v", field.Kind())
	}

	return nil
}
