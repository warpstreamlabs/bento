package value

import (
	"bytes"
	"fmt"
	"strings"
)

// TypeError represents an error where a value of a type was required for a
// function, method or operator but instead a different type was found.
type TypeError struct {
	From     string
	Expected []Type
	Actual   Type
	Value    string
}

// Error implements the standard error interface for TypeError.
func (t *TypeError) Error() string {
	var errStr bytes.Buffer
	if len(t.Expected) > 0 {
		errStr.WriteString("expected ")
		for i, exp := range t.Expected {
			if i > 0 {
				if len(t.Expected) > 2 && i < (len(t.Expected)-1) {
					errStr.WriteString(", ")
				} else {
					errStr.WriteString(" or ")
				}
			}
			errStr.WriteString(string(exp))
		}
		errStr.WriteString(" value")
	} else {
		errStr.WriteString("unexpected value")
	}

	fmt.Fprintf(&errStr, ", got %v", t.Actual)

	if t.From != "" {
		fmt.Fprintf(&errStr, " from %v", t.From)
	}

	if t.Value != "" {
		fmt.Fprintf(&errStr, " (%v)", t.Value)
	}

	return errStr.String()
}

// NewTypeError creates a new type error.
func NewTypeError(value any, exp ...Type) *TypeError {
	return NewTypeErrorFrom("", value, exp...)
}

// NewTypeErrorFrom creates a new type error with an annotation of the query
// that provided the wrong type.
func NewTypeErrorFrom(from string, value any, exp ...Type) *TypeError {
	valueStr := ""
	valueType := ITypeOf(value)
	switch valueType {
	case TString:
		valueStr = fmt.Sprintf(`"%v"`, value)
	case TBool, TNumber:
		valueStr = fmt.Sprintf("%v", value)
	}
	return &TypeError{
		From:     from,
		Expected: exp,
		Actual:   valueType,
		Value:    valueStr,
	}
}

//------------------------------------------------------------------------------

// NewDetailedError decorates an error with Type, Label, and Path metadata from the
// component that caused it.
func NewDetailedError(err error, typ, lbl string, path ...string) *DetailedError {
	// TODO(gregfurman): Introduce a StackTrace (likely via a StackFrame approach for messages)
	// that allows us to track exactly where a message has traversed.
	//
	// For now, however, a DetailedError should be recursively unwrapped until we reach
	// the original error. Otherwise, we're deeply nesting data without much benefit.
	// i.e if a message is retried 1000x and continually errors, this will give us a 1000-level
	// nested error whereas we only care about the most recent one.
	if de, ok := err.(*DetailedError); ok {
		err = de.Unwrap()
	}

	return &DetailedError{
		err:   err,
		typ:   typ,
		label: lbl,
		path:  strings.Join(path, "."),
	}
}

// DetailedError wraps an error with component metadata.
type DetailedError struct {
	err   error
	typ   string
	label string
	path  string
}

func (d *DetailedError) Error() string {
	return d.err.Error()
}

func (d *DetailedError) Path() string {
	return d.path
}

func (d *DetailedError) Type() string {
	return d.typ
}

func (d *DetailedError) Label() string {
	return d.label
}

func (d *DetailedError) Unwrap() error {
	// TODO: Either handle the case where a DetailedError is wrapped
	// by another error type OR consider falling back to errors.Unwrap
	err, ok := d.err.(*DetailedError)
	if !ok {
		return d.err
	}

	return err.Unwrap()
}
