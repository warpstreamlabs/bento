package parquet

import (
	"encoding/json"
	"math/rand"
	"strings"
	"time"
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

// HACK(gregfurman): use an rng to make this approach testable
var randSource = rand.New(rand.NewSource(time.Now().UnixNano()))

func randomFieldName(length int) string {
	var builder strings.Builder
	for i := 0; i < length; i++ {
		builder.WriteRune(rune(randSource.Intn(26) + 'A'))
	}

	return builder.String()
}
