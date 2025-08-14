package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/value"
)

func TestMethodImmutability(t *testing.T) {
	testCases := []struct {
		name   string
		method string
		target any
		args   []any
		exp    any
	}{
		{
			name:   "merge arrays",
			method: "merge",
			target: []any{"foo", "bar"},
			args: []any{
				[]any{"baz", "buz"},
			},
			exp: []any{"foo", "bar", "baz", "buz"},
		},
		{
			name:   "merge into an array",
			method: "merge",
			target: []any{"foo", "bar"},
			args: []any{
				map[string]any{"baz": "buz"},
			},
			exp: []any{"foo", "bar", map[string]any{"baz": "buz"}},
		},
		{
			name:   "merge objects",
			method: "merge",
			target: map[string]any{"foo": "bar"},
			args: []any{
				map[string]any{"baz": "buz"},
			},
			exp: map[string]any{
				"foo": "bar",
				"baz": "buz",
			},
		},
		{
			name:   "merge collision",
			method: "merge",
			target: map[string]any{"foo": "bar", "baz": "buz"},
			args: []any{
				map[string]any{"foo": "qux"},
			},
			exp: map[string]any{
				"foo": []any{"bar", "qux"},
				"baz": "buz",
			},
		},

		{
			name:   "assign arrays",
			method: "assign",
			target: []any{"foo", "bar"},
			args: []any{
				[]any{"baz", "buz"},
			},
			exp: []any{"foo", "bar", "baz", "buz"},
		},
		{
			name:   "assign into an array",
			method: "assign",
			target: []any{"foo", "bar"},
			args: []any{
				map[string]any{"baz": "buz"},
			},
			exp: []any{"foo", "bar", map[string]any{"baz": "buz"}},
		},
		{
			name:   "assign objects",
			method: "assign",
			target: map[string]any{"foo": "bar"},
			args: []any{
				map[string]any{"baz": "buz"},
			},
			exp: map[string]any{
				"foo": "bar",
				"baz": "buz",
			},
		},
		{
			name:   "assign collision",
			method: "assign",
			target: map[string]any{"foo": "bar", "baz": "buz"},
			args: []any{
				map[string]any{"foo": "qux"},
			},
			exp: map[string]any{
				"foo": "qux",
				"baz": "buz",
			},
		},

		{
			name:   "contains object positive",
			method: "contains",
			target: []any{
				map[string]any{"foo": "bar"},
			},
			args: []any{
				map[string]any{"foo": "bar"},
			},
			exp: true,
		},
		{
			name:   "contains object negative",
			method: "contains",
			target: []any{
				map[string]any{"foo": "bar"},
			},
			args: []any{
				map[string]any{"baz": "buz"},
			},
			exp: false,
		},
	}

	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			targetClone := value.IClone(test.target)
			argsClone := value.IClone(test.args).([]any)

			fn, err := InitMethodHelper(test.method, NewLiteralFunction("", targetClone), argsClone...)
			require.NoError(t, err)

			res, err := fn.Exec(FunctionContext{
				Maps:     map[string]Function{},
				Index:    0,
				MsgBatch: nil,
			})
			require.NoError(t, err)

			assert.Equal(t, test.exp, res)
			assert.Equal(t, test.target, targetClone)
			assert.Equal(t, test.args, argsClone)
		})
	}
}

func TestSplitMethod(t *testing.T) {
	ctx := FunctionContext{
		Index: 0,
		MsgBatch: message.Batch{
			message.NewPart([]byte("test")),
		},
	}

	tests := []struct {
		name     string
		input    any
		param    any
		expected any
		errMsg   string
	}{
		// String splitting
		{
			name:     "string split with comma",
			input:    "foo,bar,baz",
			param:    ",",
			expected: []any{"foo", "bar", "baz"},
		},
		{
			name:     "string split with space",
			input:    "hello world test",
			param:    " ",
			expected: []any{"hello", "world", "test"},
		},
		{
			name:     "string split empty delimiter",
			input:    "abc",
			param:    "",
			expected: []any{"a", "b", "c"},
		},
		{
			name:     "string split no matches",
			input:    "hello",
			param:    "x",
			expected: []any{"hello"},
		},
		{
			name:     "string split empty string",
			input:    "",
			param:    ",",
			expected: []any{""},
		},
		{
			name:     "string split consecutive delimiters",
			input:    "a,,b",
			param:    ",",
			expected: []any{"a", "", "b"},
		},

		// Array splitting
		{
			name:     "array split with string delimiter",
			input:    []any{"apple", "banana", "SPLIT", "orange", "grape"},
			param:    "SPLIT",
			expected: []any{[]any{"apple", "banana"}, []any{"orange", "grape"}},
		},
		{
			name:     "array split no matches",
			input:    []any{"a", "b", "c"},
			param:    "x",
			expected: []any{[]any{"a", "b", "c"}},
		},
		{
			name:     "array split empty array",
			input:    []any{},
			param:    "x",
			expected: []any{[]any{}},
		},
		{
			name:     "array split multiple consecutive matches",
			input:    []any{1, "x", "x", 2},
			param:    "x",
			expected: []any{[]any{1}, []any{}, []any{2}},
		},
		{
			name:     "array split start and end delimiters",
			input:    []any{"x", 1, 2, "x"},
			param:    "x",
			expected: []any{[]any(nil), []any{1, 2}, []any{}},
		},

		// Byte array splitting
		{
			name:     "bytes split with comma",
			input:    []byte("foo,bar,baz"),
			param:    ",",
			expected: []any{[]byte("foo"), []byte("bar"), []byte("baz")},
		},
		{
			name:     "bytes split empty input",
			input:    []byte(""),
			param:    ",",
			expected: []any{[]byte("")},
		},
		{
			name:     "string split start delimiter",
			input:    ",hello,world",
			param:    ",",
			expected: []any{"", "hello", "world"},
		},
		{
			name:     "string split end delimiter",
			input:    "hello,world,",
			param:    ",",
			expected: []any{"hello", "world", ""},
		},
		{
			name:     "array split all elements match",
			input:    []any{"x", "x", "x"},
			param:    "x",
			expected: []any{[]any(nil), []any{}, []any{}, []any{}},
		},

		// Error cases
		{
			name:   "invalid input type",
			input:  123,
			param:  ",",
			errMsg: "expected string, array or bytes value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn, err := InitMethodHelper("split", NewLiteralFunction("", tt.input), tt.param)
			require.NoError(t, err)

			result, err := fn.Exec(ctx)

			if tt.errMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSplitByMethod(t *testing.T) {
	// Helper functions for creating predicates
	literalFn := func(val any) Function {
		return NewLiteralFunction("", val)
	}

	arithmetic := func(left, right Function, op ArithmeticOperator) Function {
		t.Helper()
		fn, err := NewArithmeticExpression(
			[]Function{left, right},
			[]ArithmeticOperator{op},
		)
		require.NoError(t, err)
		return fn
	}

	methods := func(fn Function, methods ...struct {
		name string
		args []any
	}) Function {
		t.Helper()
		for _, m := range methods {
			var err error
			fn, err = InitMethodHelper(m.name, fn, m.args...)
			require.NoError(t, err)
		}
		return fn
	}

	method := func(name string, args ...any) struct {
		name string
		args []any
	} {
		return struct {
			name string
			args []any
		}{name: name, args: args}
	}

	ctx := FunctionContext{
		Index: 0,
		MsgBatch: message.Batch{
			message.NewPart([]byte("test")),
		},
	}

	tests := []struct {
		name      string
		input     any
		predicate Function
		expected  any
		errMsg    string
	}{
		{
			name:  "string split by space characters",
			input: "hello world test",
			predicate: arithmetic(
				NewFieldFunction(""),
				literalFn(" "),
				ArithmeticEq,
			),
			expected: []any{"hello", "world", "test"},
		},
		{
			name:  "string split by vowels",
			input: "hello",
			predicate: methods(
				literalFn("aeiou"),
				method("contains", NewFieldFunction("")),
			),
			expected: []any{"h", "ll", ""},
		},
		{
			name:  "array split by numbers greater than 8",
			input: []any{1, 2, 10, 3, 4, 20, 7},
			predicate: arithmetic(
				NewFieldFunction(""),
				literalFn(8),
				ArithmeticGt,
			),
			expected: []any{[]any{1, 2}, []any{3, 4}, []any{7}},
		},
		{
			name:  "array split by string contains",
			input: []any{"apple", "banana split", "orange"},
			predicate: methods(
				NewFieldFunction(""),
				method("contains", "split"),
			),
			expected: []any{[]any{"apple"}, []any{"orange"}},
		},
		{
			name:  "bytes split by comma ASCII value",
			input: []byte("foo,bar,baz"),
			predicate: arithmetic(
				NewFieldFunction(""),
				literalFn(44), // ASCII comma
				ArithmeticEq,
			),
			expected: []any{[]byte("foo"), []byte("bar"), []byte("baz")},
		},
		{
			name:  "string split by predicate no matches",
			input: "hello",
			predicate: arithmetic(
				NewFieldFunction(""),
				literalFn("x"),
				ArithmeticEq,
			),
			expected: []any{"hello"},
		},
		{
			name:  "empty string with predicate",
			input: "",
			predicate: arithmetic(
				NewFieldFunction(""),
				literalFn(" "),
				ArithmeticEq,
			),
			expected: []any{""},
		},
		{
			name:  "array consecutive matches",
			input: []any{1, 5, 5, 2},
			predicate: arithmetic(
				NewFieldFunction(""),
				literalFn(5),
				ArithmeticEq,
			),
			expected: []any{[]any{1}, []any{}, []any{2}},
		},
		{
			name:  "string split all matches",
			input: "aaaaa",
			predicate: arithmetic(
				NewFieldFunction(""),
				literalFn("a"),
				ArithmeticEq,
			),
			expected: []any{"", "", "", "", "", ""},
		},
		{
			name:  "bytes split by even values",
			input: []byte{1, 2, 3, 4, 5, 6},
			predicate: arithmetic(
				arithmetic(
					NewFieldFunction(""),
					literalFn(2),
					ArithmeticMod,
				),
				literalFn(0),
				ArithmeticEq,
			),
			expected: []any{[]byte{1}, []byte{3}, []byte{5}, []byte{}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn := methods(
				literalFn(tt.input),
				method("split_by", tt.predicate),
			)

			result, err := fn.Exec(ctx)

			if tt.errMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSplitMethodErrorCases(t *testing.T) {
	ctx := FunctionContext{
		Index: 0,
		MsgBatch: message.Batch{
			message.NewPart([]byte("test")),
		},
	}

	errorTests := []struct {
		name   string
		method string
		input  any
		param  any
		errMsg string
	}{
		{
			name:   "split invalid input type",
			method: "split",
			input:  123,
			param:  ",",
			errMsg: "expected string, array or bytes value",
		},
		{
			name:   "split nil input",
			method: "split",
			input:  nil,
			param:  ",",
			errMsg: "expected string, array or bytes value",
		},
		{
			name:   "split_by invalid input type",
			method: "split_by",
			input:  123,
			param: func() Function {
				fn, _ := NewArithmeticExpression(
					[]Function{NewFieldFunction(""), NewLiteralFunction("", " ")},
					[]ArithmeticOperator{ArithmeticEq},
				)
				return fn
			}(),
			errMsg: "expected string, array or bytes value",
		},
	}

	for _, tt := range errorTests {
		t.Run(tt.name, func(t *testing.T) {
			fn, err := InitMethodHelper(tt.method, NewLiteralFunction("", tt.input), tt.param)
			require.NoError(t, err)

			_, err = fn.Exec(ctx)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errMsg)
		})
	}
}
