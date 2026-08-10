package query

import (
	"testing"

	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
)

// resolveFieldPath must be indistinguishable from the gabs expression it replaces, including on
// every failure mode: missing keys, nil intermediates, scalars mid-path, arrays, negative and
// out-of-range indices, and the "*" wildcard that gabs' Search permits.
func TestResolveFieldPathMatchesGabs(t *testing.T) {
	tests := []struct {
		name   string
		target any
		path   []string
	}{
		{"simple", map[string]any{"a": map[string]any{"b": "c"}}, []string{"a", "b"}},
		{"numeric_map_keys", map[string]any{"983": map[string]any{"002": "BSK"}}, []string{"983", "002"}},
		{"missing_leaf", map[string]any{"a": map[string]any{"b": "c"}}, []string{"a", "z"}},
		{"missing_intermediate", map[string]any{"a": map[string]any{"b": "c"}}, []string{"z", "b"}},
		{"nil_intermediate", map[string]any{"a": nil}, []string{"a", "b"}},
		{"nil_leaf", map[string]any{"a": nil}, []string{"a"}},
		{"past_scalar", map[string]any{"a": "scalar"}, []string{"a", "b"}},
		{"scalar_root", "scalar", []string{"a"}},
		{"nil_root", nil, []string{"a"}},
		{"array_index", map[string]any{"a": []any{"x", "y"}}, []string{"a", "1"}},
		{"array_index_zero", map[string]any{"a": []any{"x", "y"}}, []string{"a", "0"}},
		{"array_out_of_range", map[string]any{"a": []any{"x"}}, []string{"a", "5"}},
		{"array_negative", map[string]any{"a": []any{"x"}}, []string{"a", "-1"}},
		{"array_non_numeric", map[string]any{"a": []any{"x"}}, []string{"a", "b"}},
		{"array_wildcard", map[string]any{"a": []any{
			map[string]any{"b": 1}, map[string]any{"b": 2},
		}}, []string{"a", "*", "b"}},
		{"array_wildcard_leaf", map[string]any{"a": []any{"x", "y"}}, []string{"a", "*"}},
		{"wildcard_on_map", map[string]any{"a": map[string]any{"b": 1}}, []string{"a", "*"}},
		{"array_of_arrays", map[string]any{"a": []any{[]any{"deep"}}}, []string{"a", "0", "0"}},
		{"map_in_array", map[string]any{"a": []any{map[string]any{"b": "c"}}}, []string{"a", "0", "b"}},
		{"empty_string_key", map[string]any{"": "blank"}, []string{""}},
		{"typed_nil_map", map[string]any{"a": map[string]any(nil)}, []string{"a", "b"}},
		{"deep_all_maps", map[string]any{"a": map[string]any{"b": map[string]any{"c": map[string]any{"d": 4}}}}, []string{"a", "b", "c", "d"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			want := gabs.Wrap(test.target).S(test.path...).Data()
			got := resolveFieldPath(test.target, test.path)
			assert.Equal(t, want, got)
		})
	}
}
