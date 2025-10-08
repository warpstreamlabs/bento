package pure

import (
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/bloblang"
)

func TestCosine(t *testing.T) {
	tests := []struct {
		name    string
		a       []float64
		b       []float64
		want    float64
		wantErr error
	}{
		{
			name: "identical vectors",
			a:    []float64{1.0, 2.0, 3.0},
			b:    []float64{1.0, 2.0, 3.0},
			want: 1.0,
		},
		{
			name: "opposite vectors",
			a:    []float64{1.0, 2.0, 3.0},
			b:    []float64{-1.0, -2.0, -3.0},
			want: -1.0,
		},
		{
			name: "orthogonal vectors",
			a:    []float64{1.0, 0.0},
			b:    []float64{0.0, 1.0},
			want: 0.0,
		},
		{
			name: "45 degree vectors",
			a:    []float64{1.0, 0.0},
			b:    []float64{1.0, 1.0},
			want: math.Sqrt(2) / 2,
		},
		{
			name: "normalized vectors",
			a:    []float64{0.6, 0.8},
			b:    []float64{0.8, 0.6},
			want: 0.96,
		},
		{
			name:    "unequal length vectors",
			a:       []float64{1.0, 2.0},
			b:       []float64{1.0, 2.0, 3.0},
			wantErr: errVectorUnequalLength,
		},
		{
			name:    "empty vectors",
			a:       []float64{},
			b:       []float64{},
			wantErr: errVectorEmpty,
		},
		{
			name:    "null vector a",
			a:       []float64{0.0, 0.0, 0.0},
			b:       []float64{1.0, 2.0, 3.0},
			wantErr: errVectorNull,
		},
		{
			name:    "null vector b",
			a:       []float64{1.0, 2.0, 3.0},
			b:       []float64{0.0, 0.0, 0.0},
			wantErr: errVectorNull,
		},
		{
			name:    "both null vectors",
			a:       []float64{0.0, 0.0},
			b:       []float64{0.0, 0.0},
			wantErr: errVectorNull,
		},
		{
			name: "single element vectors",
			a:    []float64{5.0},
			b:    []float64{3.0},
			want: 1.0,
		},
		{
			name: "large vectors",
			a:    []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0},
			b:    []float64{10.0, 9.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0},
			want: 0.5714285714285715,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := cosine(tt.a, tt.b)
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("cosine() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Errorf("cosine() unexpected error = %v", err)
				return
			}
			if math.Abs(got-tt.want) > 1e-10 {
				t.Errorf("cosine() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestToFloat64(t *testing.T) {
	tests := []struct {
		name    string
		input   any
		want    []float64
		wantErr error
	}{
		{
			name:  "float64 slice",
			input: []float64{1.0, 2.5, 3.7},
			want:  []float64{1.0, 2.5, 3.7},
		},
		{
			name:  "float32 slice",
			input: []float32{1.0, 2.5, 3.7},
			want:  []float64{1.0, 2.5, 3.700000047683716},
		},
		{
			name:  "int slice",
			input: []int{1, 2, 3},
			want:  []float64{1.0, 2.0, 3.0},
		},
		{
			name:  "int32 slice",
			input: []int32{1, 2, 3},
			want:  []float64{1.0, 2.0, 3.0},
		},
		{
			name:  "int64 slice",
			input: []int64{1, 2, 3},
			want:  []float64{1.0, 2.0, 3.0},
		},
		{
			name:  "empty float64 slice",
			input: []float64{},
			want:  []float64{},
		},
		{
			name:  "empty int slice",
			input: []int{},
			want:  []float64{},
		},
		{
			name:    "unsupported string slice",
			input:   []string{"1", "2", "3"},
			wantErr: errVectorInvalidType,
		},
		{
			name:    "unsupported single string",
			input:   "not a slice",
			wantErr: errVectorInvalidType,
		},
		{
			name:    "unsupported bool slice",
			input:   []bool{true, false},
			wantErr: errVectorInvalidType,
		},
		{
			name:    "nil input",
			input:   nil,
			wantErr: errVectorInvalidType,
		},
		{
			name:  "large int slice",
			input: []int{-100, 0, 100, 1000, -1000},
			want:  []float64{-100.0, 0.0, 100.0, 1000.0, -1000.0},
		},
		{
			name:  "interface slice with mixed numbers",
			input: []any{1.0, 2, int32(3), int64(4), float32(5.5)},
			want:  []float64{1.0, 2.0, 3.0, 4.0, 5.5},
		},
		{
			name:  "empty interface slice",
			input: []any{},
			want:  []float64{},
		},
		{
			name:    "interface slice with invalid type",
			input:   []any{1.0, "invalid", 3.0},
			wantErr: errVectorInvalidType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := toFloat64(tt.input)
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("toFloat64() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Errorf("toFloat64() unexpected error = %v", err)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("toFloat64() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCosineSimilarityBloblang(t *testing.T) {
	tests := []struct {
		name               string
		mapping            string
		input              any
		output             any
		parseErrorContains string
		execErrorContains  string
	}{
		{
			name:    "identical vectors",
			mapping: `root = cosine_similarity([1.0, 2.0, 3.0], [1.0, 2.0, 3.0])`,
			output:  1.0,
		},
		{
			name:    "opposite vectors",
			mapping: `root = cosine_similarity([1.0, 2.0, 3.0], [-1.0, -2.0, -3.0])`,
			output:  -1.0,
		},
		{
			name:    "orthogonal vectors",
			mapping: `root = cosine_similarity([1.0, 0.0], [0.0, 1.0])`,
			output:  0.0,
		},
		{
			name:    "mixed numeric types",
			mapping: `root = cosine_similarity([1, 2, 3], [1.0, 2.0, 3.0])`,
			output:  1.0,
		},
		{
			name:    "more mixed numeric types",
			mapping: `root = cosine_similarity([1.0, 2], [1, 2.0])`,
			output:  1.0,
		},
		{
			name: "using variables",
			mapping: `let a = [1.0, 2.0, 3.0]
let b = [4.0, 5.0, 6.0]
root = cosine_similarity($a, $b)`,
			output: 0.9746318461970762,
		},
		{
			name:              "unequal length vectors",
			mapping:           `root = cosine_similarity([1.0, 2.0], [1.0, 2.0, 3.0])`,
			execErrorContains: "vectors must be equal length",
		},
		{
			name:              "empty vectors",
			mapping:           `root = cosine_similarity([], [])`,
			execErrorContains: "vectors cannot be empty",
		},
		{
			name:              "null vector",
			mapping:           `root = cosine_similarity([0.0, 0.0], [1.0, 2.0])`,
			execErrorContains: "vectors must not be null (all zeros)",
		},
		{
			name:              "invalid vector type",
			mapping:           `root = cosine_similarity(["a", "b"], [1.0, 2.0])`,
			execErrorContains: "vector must be an array of numeric types",
		},
		{
			name:    "single element vectors",
			mapping: `root = cosine_similarity([5], [3])`,
			output:  1.0,
		},
		{
			name:    "from input fields",
			mapping: `root = cosine_similarity(this.vector_a, this.vector_b)`,
			input: map[string]any{
				"vector_a": []float64{1.0, 2.0, 3.0},
				"vector_b": []float64{4.0, 5.0, 6.0},
			},
			output: 0.9746318461970762,
		},
		{
			name:              "missing field error",
			mapping:           `root = cosine_similarity(this.missing, [1.0, 2.0])`,
			input:             map[string]any{},
			execErrorContains: "vector must be an array of numeric types",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exec, err := bloblang.Parse(tt.mapping)
			if tt.parseErrorContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.parseErrorContains)
				return
			}
			require.NoError(t, err)

			res, err := exec.Query(tt.input)
			if tt.execErrorContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.execErrorContains)
				return
			}
			require.NoError(t, err)

			if floatRes, ok := res.(float64); ok && tt.output != nil {
				if floatExpected, ok := tt.output.(float64); ok {
					assert.InDelta(t, floatExpected, floatRes, 1e-10)
					return
				}
			}
			assert.Equal(t, tt.output, res)
		})
	}
}
