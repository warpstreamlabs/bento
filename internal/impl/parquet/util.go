package parquet

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/warpstreamlabs/bento/internal/value"
)

func scrubJSONNumbers(v any) any {
	switch t := v.(type) {
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}
		if f, err := t.Float64(); err == nil {
			return f
		}
		return 0
	case map[string]any:
		scrubJSONNumbersObj(t)
		return t
	case []any:
		scrubJSONNumbersArr(t)
		return t
	}
	return v
}

func scrubJSONNumbersObj(obj map[string]any) {
	for k, v := range obj {
		obj[k] = scrubJSONNumbers(v)
	}
}

func scrubJSONNumbersArr(arr []any) {
	for i, v := range arr {
		arr[i] = scrubJSONNumbers(v)
	}
}

func valueOf(val any, field parquet.Field) (any, error) {
	if val == nil {
		return nil, nil
	}

	if !field.Leaf() {
		return val, nil
	}

	if arr, ok := val.([]any); ok {
		for i, v := range arr {
			res, err := valueOf(v, field)
			if err != nil {
				return nil, fmt.Errorf("index %d: %w", i, err)
			}
			arr[i] = res
		}
		return arr, nil
	}

	switch field.Type().Kind() {
	case parquet.Int32:
		return value.IToInt32(val)
	case parquet.Int64:
		return value.IToInt(val)
	case parquet.Float:
		return value.IToFloat32(val)
	case parquet.Double:
		return value.IToFloat64(val)
	case parquet.Boolean:
		return value.IToBool(val)
	default:
		return val, nil
	}
}

// HACK(gregfurman): use an rng to make this approach testable
var randSource = rand.New(rand.NewSource(time.Now().UnixNano()))

func randomFieldName(length int) string {
	var builder strings.Builder
	for range length {
		builder.WriteRune(rune(randSource.Intn(26) + 'A'))
	}

	return builder.String()
}
