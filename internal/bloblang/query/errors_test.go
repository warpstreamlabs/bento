package query

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/warpstreamlabs/bento/v1/internal/value"
)

func TestTypeError(t *testing.T) {
	tests := map[string]struct {
		from   string
		actual any
		types  []value.Type
		exp    string
	}{
		"want nothing get str": {
			actual: "hello world",
			types:  []value.Type{},
			exp:    `unexpected value, got string ("hello world")`,
		},
		"want num get str": {
			actual: "hello world",
			types:  []value.Type{value.TNumber},
			exp:    `expected number value, got string ("hello world")`,
		},
		"want num get str from": {
			from:   "method foo",
			actual: "hello world",
			types:  []value.Type{value.TNumber},
			exp:    `expected number value, got string from method foo ("hello world")`,
		},
		"want num or bool get str": {
			actual: "hello world",
			types:  []value.Type{value.TNumber, value.TBool},
			exp:    `expected number or bool value, got string ("hello world")`,
		},
		"want num, bool or array get str": {
			actual: "hello world",
			types:  []value.Type{value.TNumber, value.TBool, value.TArray},
			exp:    `expected number, bool or array value, got string ("hello world")`,
		},
		"want num get bytes": {
			actual: []byte("foo"),
			types:  []value.Type{value.TNumber},
			exp:    `expected number value, got bytes`,
		},
		"want num get bool": {
			actual: false,
			types:  []value.Type{value.TNumber},
			exp:    `expected number value, got bool (false)`,
		},
		"want num get array": {
			actual: []any{"foo"},
			types:  []value.Type{value.TNumber},
			exp:    `expected number value, got array`,
		},
		"want num get object": {
			actual: map[string]any{"foo": "bar"},
			types:  []value.Type{value.TNumber},
			exp:    `expected number value, got object`,
		},
		"want num get null": {
			actual: nil,
			types:  []value.Type{value.TNumber},
			exp:    `expected number value, got null`,
		},
		"want num get delete": {
			actual: value.Delete(nil),
			types:  []value.Type{value.TNumber},
			exp:    `expected number value, got delete`,
		},
		"want num get nothing": {
			actual: value.Nothing(nil),
			types:  []value.Type{value.TNumber},
			exp:    `expected number value, got nothing`,
		},
		"want num get unknown": {
			actual: []string{"unknown"},
			types:  []value.Type{value.TNumber},
			exp:    `expected number value, got unknown`,
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.exp, value.NewTypeErrorFrom(test.from, test.actual, test.types...).Error())
		})
	}
}

func TestErrorFromError(t *testing.T) {
	err := ErrFrom(errors.New("foo"), NewLiteralFunction("bar", nil))
	assert.EqualError(t, err, "bar: foo")

	err = ErrFrom(err, NewLiteralFunction("baz", nil))
	assert.EqualError(t, err, "bar: foo")

	err = ErrFrom(fmt.Errorf("wat: %w", err), NewLiteralFunction("baz", nil))
	assert.EqualError(t, err, "wat: bar: foo")

	err = ErrFrom(fmt.Errorf("wat: %w", value.NewTypeError("hello", value.TBool)), NewLiteralFunction("baz", nil))
	assert.EqualError(t, err, "baz: wat: expected bool value, got string (\"hello\")")
}

func TestTypeMismatchError(t *testing.T) {
	tests := map[string]struct {
		operator string
		left     any
		right    any
		exp      string
	}{
		"string to number": {
			operator: "compare",
			left:     "foo",
			right:    10.0,
			exp:      `cannot compare types string (from left thing) and number (from right thing)`,
		},
		"bool to array": {
			operator: "compare",
			left:     false,
			right:    []any{"foo"},
			exp:      `cannot compare types bool (from left thing) and array (from right thing)`,
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.exp, NewTypeMismatch(
				test.operator,
				NewLiteralFunction("left thing", nil),
				NewLiteralFunction("right thing", nil),
				test.left, test.right,
			).Error())
		})
	}
}
