package query

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/warpstreamlabs/bento/internal/value"
)

var _ = registerSimpleMethod(
	NewMethodSpec("ceil", "Returns the least integer value greater than or equal to a number. If the resulting value fits within a 64-bit integer then that is returned, otherwise a new floating point number is returned.").InCategory(
		MethodCategoryNumbers, "",
		NewExampleSpec("",
			`root.new_value = this.value.ceil()`,
			`{"value":5.3}`,
			`{"new_value":6}`,
			`{"value":-5.9}`,
			`{"new_value":-5}`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return numberMethod(func(f *float64, i *int64, ui *uint64) (any, error) {
			if f != nil {
				ceiled := math.Ceil(*f)
				if i, err := value.IToInt(ceiled); err == nil {
					return i, nil
				}
				return ceiled, nil
			}
			if i != nil {
				return *i, nil
			}
			return *ui, nil
		}), nil
	},
)

var _ = registerSimpleMethod(
	NewMethodSpec(
		"floor", "Returns the greatest integer value less than or equal to the target number. If the resulting value fits within a 64-bit integer then that is returned, otherwise a new floating point number is returned.",
	).InCategory(
		MethodCategoryNumbers,
		"",
		NewExampleSpec("",
			`root.new_value = this.value.floor()`,
			`{"value":5.7}`,
			`{"new_value":5}`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return numberMethod(func(f *float64, i *int64, ui *uint64) (any, error) {
			if f != nil {
				floored := math.Floor(*f)
				if i, err := value.IToInt(floored); err == nil {
					return i, nil
				}
				return floored, nil
			}
			if i != nil {
				return *i, nil
			}
			return *ui, nil
		}), nil
	},
)

var _ = registerSimpleMethod(
	NewMethodSpec("log", "Returns the natural logarithm of a number.").InCategory(
		MethodCategoryNumbers, "",
		NewExampleSpec("",
			`root.new_value = this.value.log().round()`,
			`{"value":1}`,
			`{"new_value":0}`,
			`{"value":2.7183}`,
			`{"new_value":1}`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return numberMethod(func(f *float64, i *int64, ui *uint64) (any, error) {
			var v float64
			if f != nil {
				v = *f
			} else if i != nil {
				v = float64(*i)
			} else {
				v = float64(*ui)
			}
			return math.Log(v), nil
		}), nil
	},
)

var _ = registerSimpleMethod(
	NewMethodSpec("log10", "Returns the decimal logarithm of a number.").InCategory(
		MethodCategoryNumbers, "",
		NewExampleSpec("",
			`root.new_value = this.value.log10()`,
			`{"value":100}`,
			`{"new_value":2}`,
			`{"value":1000}`,
			`{"new_value":3}`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return numberMethod(func(f *float64, i *int64, ui *uint64) (any, error) {
			var v float64
			if f != nil {
				v = *f
			} else if i != nil {
				v = float64(*i)
			} else {
				v = float64(*ui)
			}
			return math.Log10(v), nil
		}), nil
	},
)

var _ = registerSimpleMethod(
	NewMethodSpec(
		"max",
		"Returns the largest numerical value found within an array. All values must be numerical and the array must not be empty, otherwise an error is returned.",
	).InCategory(
		MethodCategoryNumbers, "",
		NewExampleSpec("",
			`root.biggest = this.values.max()`,
			`{"values":[0,3,2.5,7,5]}`,
			`{"biggest":7}`,
		),
		NewExampleSpec("",
			`root.new_value = [0,this.value].max()`,
			`{"value":-1}`,
			`{"new_value":0}`,
			`{"value":7}`,
			`{"new_value":7}`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return func(v any, ctx FunctionContext) (any, error) {
			arr, ok := v.([]any)
			if !ok {
				return nil, value.NewTypeError(v, value.TArray)
			}
			if len(arr) == 0 {
				return nil, errors.New("the array was empty")
			}
			var maximum float64
			for i, n := range arr {
				f, err := value.IGetNumber(n)
				if err != nil {
					return nil, fmt.Errorf("index %v of array: %w", i, err)
				}
				if i == 0 || f > maximum {
					maximum = f
				}
			}
			return maximum, nil
		}, nil
	},
)

var _ = registerSimpleMethod(
	NewMethodSpec(
		"min",
		"Returns the smallest numerical value found within an array. All values must be numerical and the array must not be empty, otherwise an error is returned.",
	).InCategory(
		MethodCategoryNumbers, "",
		NewExampleSpec("",
			`root.smallest = this.values.min()`,
			`{"values":[0,3,-2.5,7,5]}`,
			`{"smallest":-2.5}`,
		),
		NewExampleSpec("",
			`root.new_value = [10,this.value].min()`,
			`{"value":2}`,
			`{"new_value":2}`,
			`{"value":23}`,
			`{"new_value":10}`,
		),
	),
	func(*ParsedParams) (simpleMethod, error) {
		return func(v any, ctx FunctionContext) (any, error) {
			arr, ok := v.([]any)
			if !ok {
				return nil, value.NewTypeError(v, value.TArray)
			}
			if len(arr) == 0 {
				return nil, errors.New("the array was empty")
			}
			var maximum float64
			for i, n := range arr {
				f, err := value.IGetNumber(n)
				if err != nil {
					return nil, fmt.Errorf("index %v of array: %w", i, err)
				}
				if i == 0 || f < maximum {
					maximum = f
				}
			}
			return maximum, nil
		}, nil
	},
)

var _ = registerSimpleMethod(
	NewMethodSpec(
		"round", "Rounds numbers to the nearest value with a given number of decimal places, defaulting to the nearest integer. The rounding style for ties can be specified and defaults to half away from zero. If the resulting value fits within a 64-bit integer then that is returned, otherwise a new floating point number is returned.",
	).InCategory(
		MethodCategoryNumbers,
		"",
		NewExampleSpec("",
			`root.new_value = this.value.round()`,
			`{"value":5.3}`,
			`{"new_value":5}`,
			`{"value":5.9}`,
			`{"new_value":6}`,
		),
		NewExampleSpec("",
			`root.new_value = this.value.round(2)`,
			`{"value":2.675}`,
			`{"new_value":2.68}`,
		),
		NewExampleSpec("",
			`root.new_value = this.value.round(precision: 2, style: "half_even")`,
			`{"value":0.125}`,
			`{"new_value":0.12}`,
		),
	).
		Param(ParamInt64("precision", "The number of decimal places to round to. Negative values round to tens, hundreds, and so on.").Optional()).
		Param(ParamString("style", "The rounding style to use when the value lies exactly halfway between two candidates: `half_up` rounds ties away from zero, `half_even` rounds ties to the nearest even digit (banker's rounding), and `truncate` always rounds towards zero.").Default("half_up")),
	func(args *ParsedParams) (simpleMethod, error) {
		precision, err := args.FieldOptionalInt64("precision")
		if err != nil {
			return nil, err
		}
		style, err := args.FieldString("style")
		if err != nil {
			return nil, err
		}
		p := int64(0)
		if precision != nil {
			p = *precision
		}
		if style != "half_up" && style != "half_even" && style != "truncate" {
			return nil, fmt.Errorf("unknown rounding style %q: must be one of half_up, half_even, truncate", style)
		}
		roundAndCoerce := func(v float64) (any, error) {
			rounded := roundToPrecision(v, p, style)
			if i, err := value.IToInt(rounded); err == nil {
				return i, nil
			}
			return rounded, nil
		}
		return numberMethod(func(f *float64, i *int64, ui *uint64) (any, error) {
			if f != nil {
				return roundAndCoerce(*f)
			}
			if i != nil {
				if p >= 0 {
					return *i, nil
				}
				return roundAndCoerce(float64(*i))
			}
			if p >= 0 {
				return *ui, nil
			}
			return roundAndCoerce(float64(*ui))
		}), nil
	},
)

// roundToPrecision rounds v to the given number of decimal places using the
// given style. The value is rounded as its shortest decimal representation,
// so 2.675 rounds to 2.68 at a precision of 2 under half_up, free of float
// shift artefacts.
func roundToPrecision(v float64, precision int64, style string) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return v
	}
	// Beyond these bounds rounding is determined by float64's range alone:
	// no finite float64 has more than ~330 significant decimal digits, and
	// every finite float64 rounds to zero at a precision of -309 or below.
	if precision > 400 {
		return v
	}
	if precision <= -309 {
		return 0
	}

	s := strconv.FormatFloat(v, 'f', -1, 64)
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")

	intPart, fracPart, _ := strings.Cut(s, ".")
	digits := intPart + fracPart
	// Position of the rounding digit within digits, counting from the decimal
	// point: digits[:point] is the kept part.
	point := len(intPart) + int(precision)
	switch {
	case point >= len(digits):
		return v
	case point <= 0:
		digits = strings.Repeat("0", -point) + digits
		point = 0
	}

	kept, discarded := digits[:point], digits[point:]
	roundUp := false
	switch style {
	case "half_up":
		roundUp = discarded[0] >= '5'
	case "half_even":
		switch {
		case discarded[0] > '5':
			roundUp = true
		case discarded[0] == '5':
			roundUp = strings.TrimLeft(discarded[1:], "0") != "" || (kept != "" && kept[len(kept)-1]%2 == 1)
		}
	}

	if roundUp {
		b := []byte(kept)
		carry := true
		for i := len(b) - 1; i >= 0 && carry; i-- {
			if b[i] == '9' {
				b[i] = '0'
			} else {
				b[i]++
				carry = false
			}
		}
		kept = string(b)
		if carry {
			kept = "1" + kept
		}
	}

	var out string
	if precision > 0 {
		split := len(kept) - int(precision)
		out = kept[:split] + "." + kept[split:]
	} else if precision == 0 {
		out = kept
	} else {
		out = kept + strings.Repeat("0", int(-precision))
	}
	if neg {
		out = "-" + out
	}
	r, err := strconv.ParseFloat(out, 64)
	if err != nil {
		return v
	}
	return r
}
