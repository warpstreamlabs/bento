package field

import (
	"bytes"
	"strings"

	"github.com/warpstreamlabs/bento/internal/message"
)

// Message is an interface type to be given to a function interpolator, it
// allows the function to resolve fields and metadata from a message.
type Message interface {
	Get(p int) *message.Part
	Len() int
}

//------------------------------------------------------------------------------

// NewExpression creates a field expression from a slice of resolvers.
func NewExpression(resolvers ...Resolver) *Expression {
	e := &Expression{
		resolvers: resolvers,
	}
	var staticBuf bytes.Buffer
	staticLen := 0
	for _, r := range resolvers {
		if s, is := r.(StaticResolver); is {
			_, _ = staticBuf.WriteString(string(s))
			staticLen += len(s)
		} else {
			e.dynamicExpressions++
		}
	}
	// The static segments are known exactly, the dynamic ones are not, so
	// allow a modest amount for each of them rather than growing from zero.
	e.sizeHint = staticLen + e.dynamicExpressions*dynamicSizeHint
	if e.dynamicExpressions > 0 || staticBuf.Len() == 0 {
		return e
	}
	return &Expression{
		static: staticBuf.String(),
	}
}

// dynamicSizeHint is the number of bytes assumed for each dynamic segment of
// an expression when presizing concatenation buffers.
const dynamicSizeHint = 16

//------------------------------------------------------------------------------

// Expression represents a Bento dynamic field expression, used to configure
// string fields where the contents should change based on the contents of
// messages and other factors.
//
// Each function here resolves the expression for a particular message of a
// batch, this is why an index is expected.
type Expression struct {
	static             string
	resolvers          []Resolver
	dynamicExpressions int

	// sizeHint is the total length of the static segments of the expression
	// plus a small allowance per dynamic segment. It is calculated once at
	// construction and is only used to presize the buffers used when
	// concatenating multiple resolvers, so a rough value is fine.
	sizeHint int
}

func (e *Expression) resolve(index int, msg Message, escaped bool) ([]byte, error) {
	if len(e.resolvers) == 1 {
		return e.resolvers[0].ResolveBytes(index, msg, escaped)
	}
	var buf bytes.Buffer
	buf.Grow(e.sizeHint)
	for _, r := range e.resolvers {
		b, err := r.ResolveBytes(index, msg, escaped)
		if err != nil {
			return nil, err
		}
		_, _ = buf.Write(b)
	}
	return buf.Bytes(), nil
}

// resolveString mirrors resolve but keeps the intermediate values as strings
// throughout, so that resolvers which already hold a string (static segments,
// and queries evaluating to a string) do not pay for a round trip through
// []byte and back.
func (e *Expression) resolveString(index int, msg Message, escaped bool) (string, error) {
	if len(e.resolvers) == 1 {
		return e.resolvers[0].ResolveString(index, msg, escaped)
	}
	var buf strings.Builder
	buf.Grow(e.sizeHint)
	for _, r := range e.resolvers {
		s, err := r.ResolveString(index, msg, escaped)
		if err != nil {
			return "", err
		}
		_, _ = buf.WriteString(s)
	}
	return buf.String(), nil
}

// NumDynamicExpressions returns the number of dynamic interpolation functions
// within the expression.
func (e *Expression) NumDynamicExpressions() int {
	return e.dynamicExpressions
}

// Bytes returns a byte slice representing the expression resolved for a message
// of a batch.
func (e *Expression) Bytes(index int, msg Message) ([]byte, error) {
	if len(e.resolvers) == 0 {
		return []byte(e.static), nil
	}
	return e.resolve(index, msg, false)
}

// String returns a string representing the expression resolved for a message of
// a batch.
func (e *Expression) String(index int, msg Message) (string, error) {
	if len(e.resolvers) == 0 {
		return e.static, nil
	}
	return e.resolveString(index, msg, false)
}
