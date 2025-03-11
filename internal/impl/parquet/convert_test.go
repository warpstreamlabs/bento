package parquet

import (
	"reflect"
	"testing"
)

type SimpleStruct struct {
	String  string  `json:"string"`
	Int     int64   `json:"int"`
	Float   float64 `json:"float"`
	Bool    bool    `json:"bool"`
	Default string  // Test unexported and non-json-tagged fields
}

type NestedStruct struct {
	Name     string       `json:"name"`
	Simple   SimpleStruct `json:"simple"`
	Siblings []string     `json:"siblings"`
}

type ComplexStruct struct {
	ID           string             `json:"id"`
	Nested       NestedStruct       `json:"nested"`
	Numbers      []int64            `json:"numbers"`
	Floats       []float64          `json:"floats"`
	Mixed        []any              `json:"mixed"`
	Structs      []SimpleStruct     `json:"structs"`
	StringMap    map[string]string  `json:"stringMap"`
	IntMap       map[string]int64   `json:"intMap"`
	FloatMap     map[string]float64 `json:"floatMap"`
	BoolMap      map[string]bool    `json:"boolMap"`
	TinyNumbers  []int8             `json:"tinyNumbers"`
	SmallNumbers []int16            `json:"smallNumbers"`
}

func TestMapToStruct(t *testing.T) {
	tests := []struct {
		name    string
		input   map[string]any
		dest    any
		want    any
		wantErr bool
	}{
		{
			name: "simple struct conversion",
			input: map[string]any{
				"string": "hello",
				"int":    int64(42),
				"float":  3.14,
				"bool":   true,
			},
			dest: &SimpleStruct{},
			want: &SimpleStruct{
				String: "hello",
				Int:    42,
				Float:  3.14,
				Bool:   true,
			},
		},
		{
			name: "nested struct conversion",
			input: map[string]any{
				"name": "John",
				"simple": map[string]any{
					"string": "world",
					"int":    42,
					"float":  3.14,
					"bool":   true,
				},
				"siblings": []any{"Jane", "Jack"},
			},
			dest: &NestedStruct{},
			want: &NestedStruct{
				Name: "John",
				Simple: SimpleStruct{
					String: "world",
					Int:    42,
					Float:  3.14,
					Bool:   true,
				},
				Siblings: []string{"Jane", "Jack"},
			},
		},
		{
			name: "complex struct conversion",
			input: map[string]any{
				"id": "123",
				"nested": map[string]any{
					"name": "John",
					"simple": map[string]any{
						"string": "nested",
						"int":    42,
						"float":  3.14,
						"bool":   true,
					},
					"siblings": []any{"Jane", "Jack"},
				},
				"numbers": []any{int64(1), int64(2), int64(3)},
				"floats":  []any{1.1, 2.2, 3.3},
				"mixed":   []any{"string", int64(42), 3.14, true},
				"structs": []any{
					map[string]any{
						"string": "first",
						"int":    int64(1),
						"float":  1.1,
						"bool":   true,
					},
					map[string]any{
						"string": "second",
						"int":    int64(2),
						"float":  2.2,
						"bool":   false,
					},
				},
				"stringMap": map[string]any{
					"key1": "value1",
					"key2": "value2",
				},
				"intMap": map[string]any{
					"one": int64(1),
					"two": int64(2),
				},
				"floatMap": map[string]any{
					"pi":    3.14,
					"euler": 2.718,
				},
				"boolMap": map[string]any{
					"true":  true,
					"false": false,
				},
				"tinyNumbers":  []any{int8(-128), int8(-1), int8(0), int8(1), int8(127)},
				"smallNumbers": []any{int16(-32768), int16(-1), int16(0), int16(1), int16(32767)},
			},
			dest: &ComplexStruct{},
			want: &ComplexStruct{
				ID: "123",
				Nested: NestedStruct{
					Name: "John",
					Simple: SimpleStruct{
						String: "nested",
						Int:    42,
						Float:  3.14,
						Bool:   true,
					},
					Siblings: []string{"Jane", "Jack"},
				},
				Numbers: []int64{1, 2, 3},
				Floats:  []float64{1.1, 2.2, 3.3},
				Mixed:   []any{"string", int64(42), 3.14, true},
				Structs: []SimpleStruct{
					{
						String: "first",
						Int:    1,
						Float:  1.1,
						Bool:   true,
					},
					{
						String: "second",
						Int:    2,
						Float:  2.2,
						Bool:   false,
					},
				},
				StringMap: map[string]string{
					"key1": "value1",
					"key2": "value2",
				},
				IntMap: map[string]int64{
					"one": 1,
					"two": 2,
				},
				FloatMap: map[string]float64{
					"pi":    3.14,
					"euler": 2.718,
				},
				BoolMap: map[string]bool{
					"true":  true,
					"false": false,
				},
				TinyNumbers:  []int8{-128, -1, 0, 1, 127},
				SmallNumbers: []int16{-32768, -1, 0, 1, 32767},
			},
		},
		{
			name: "nil values in maps and slices",
			input: map[string]any{
				"string": nil,
				"simple": map[string]any{
					"string": nil,
					"int":    nil,
				},
				"siblings": []any{"Jane", nil, "Jack"},
			},
			dest: &NestedStruct{},
			want: &NestedStruct{
				Simple:   SimpleStruct{},
				Siblings: []string{"Jane", "", "Jack"},
			},
		},
		{
			name:    "error: non-pointer destination",
			input:   map[string]any{},
			dest:    SimpleStruct{},
			wantErr: true,
		},
		{
			name:    "error: pointer to non-struct",
			input:   map[string]any{},
			dest:    new(string),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := MapToStruct(tt.input, tt.dest)
			if (err != nil) != tt.wantErr {
				t.Errorf("MapToStruct() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(tt.dest, tt.want) {
				t.Errorf("MapToStruct() = %+v, want %+v", tt.dest, tt.want)
			}
		})
	}
}

func TestTypeConversion(t *testing.T) {
	tests := []struct {
		name  string
		input map[string]any
		want  SimpleStruct
	}{
		{
			name: "int to int64",
			input: map[string]any{
				"int": 42,
			},
			want: SimpleStruct{Int: 42},
		},
		{
			name: "float64 to int64",
			input: map[string]any{
				"int": 42.0,
			},
			want: SimpleStruct{Int: 42},
		},
		{
			name: "int to float64",
			input: map[string]any{
				"float": 42,
			},
			want: SimpleStruct{Float: 42.0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got SimpleStruct
			if err := MapToStruct(tt.input, &got); err != nil {
				t.Errorf("MapToStruct() error = %v", err)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MapToStruct() = %v, want %v", got, tt.want)
			}
		})
	}
}

type StructWithPointers struct {
	String      *string            `json:"string"`
	Int         *int64             `json:"int"`
	Float       *float64           `json:"float"`
	Bool        *bool              `json:"bool"`
	Struct      *SimpleStruct      `json:"struct"`
	StringPtr   *string            `json:"stringPtr"`
	IntSlice    []*int64           `json:"intSlice"`
	StringMap   map[string]*string `json:"stringMap"`
	OptionalMap *map[string]*int64 `json:"optionalMap"`
}

func TestPointerFields(t *testing.T) {
	str := "hello"
	num := int64(42)
	flt := 3.14
	bl := true
	str2 := "world"
	num2 := int64(24)
	str3 := "map value"
	optMapAny := map[string]any{
		str:  nil,
		str2: num2,
	}
	optMapInt := map[string]*int64{
		str:  nil,
		str2: &num2,
	}

	tests := []struct {
		name    string
		input   map[string]any
		dest    any
		want    any
		wantErr bool
	}{
		{
			name: "optional map",
			input: map[string]any{
				"optionalMap": optMapAny,
			},
			dest: &StructWithPointers{},
			want: &StructWithPointers{
				OptionalMap: &optMapInt,
			},
		},
		{
			name: "basic pointer fields",
			input: map[string]any{
				"string":      "hello",
				"int":         42,
				"float":       3.14,
				"bool":        true,
				"optionalMap": optMapAny,
			},
			dest: &StructWithPointers{},
			want: &StructWithPointers{
				String:      &str,
				Int:         &num,
				Float:       &flt,
				Bool:        &bl,
				OptionalMap: &optMapInt,
			},
		},
		{
			name: "nil pointer fields",
			input: map[string]any{
				"string": nil,
				"int":    nil,
				"float":  nil,
				"bool":   nil,
			},
			dest: &StructWithPointers{},
			want: &StructWithPointers{
				String: nil,
				Int:    nil,
				Float:  nil,
				Bool:   nil,
			},
		},
		{
			name: "pointer struct field",
			input: map[string]any{
				"struct": map[string]any{
					"string": "world",
					"int":    int64(24),
				},
			},
			dest: &StructWithPointers{},
			want: &StructWithPointers{
				Struct: &SimpleStruct{
					String: "world",
					Int:    24,
				},
			},
		},
		{
			name: "slice of pointers",
			input: map[string]any{
				"intSlice": []any{42, 24},
			},
			dest: &StructWithPointers{},
			want: &StructWithPointers{
				IntSlice: []*int64{&num, &num2},
			},
		},
		{
			name: "map with pointer values",
			input: map[string]any{
				"stringMap": map[string]any{
					"key": "map value",
				},
			},
			dest: &StructWithPointers{},
			want: &StructWithPointers{
				StringMap: map[string]*string{
					"key": &str3,
				},
			},
		},
		{
			name: "mixed pointer and non-pointer fields",
			input: map[string]any{
				"string":    "hello",
				"stringPtr": "world",
			},
			dest: &StructWithPointers{},
			want: &StructWithPointers{
				String:    &str,
				StringPtr: &str2,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := MapToStruct(tt.input, tt.dest)
			if (err != nil) != tt.wantErr {
				t.Errorf("MapToStruct() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if !reflect.DeepEqual(tt.dest, tt.want) {
					t.Errorf("MapToStruct()\ngot  %+v\nwant %+v", tt.dest, tt.want)
				}
			}
		})
	}
}
