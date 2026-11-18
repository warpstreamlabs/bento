package editor

import (
	"errors"
	"fmt"

	"github.com/Jeffail/gabs/v2"

	"github.com/warpstreamlabs/bento/internal/bloblang"
	"github.com/warpstreamlabs/bento/internal/bloblang/mapping"
	"github.com/warpstreamlabs/bento/internal/bloblang/parser"
	"github.com/warpstreamlabs/bento/internal/bloblang/query"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/value"
)

// Runner runs compiled mappings, reusing its message and variables between runs.
type Runner struct {
	msg  message.Batch
	vars map[string]any
}

func NewRunner() *Runner {
	return &Runner{
		msg:  message.QuickBatch([][]byte{[]byte(nil)}),
		vars: map[string]any{},
	}
}

// Run runs a compiled mapping against input, which is treated as structured
// unless rawInput is set, and optionally pretty-prints the output.
func (r *Runner) Run(exec *mapping.Executor, rawInput, prettyOutput bool, input []byte) (string, error) {
	r.msg.Get(0).SetBytes(input)

	var valuePtr *any
	var parseErr error

	// parse input as structured data if needed
	lazyValue := func() *any {
		if valuePtr == nil && parseErr == nil {
			if rawInput {
				var value any = input
				valuePtr = &value
			} else {
				if jObj, err := r.msg.Get(0).AsStructured(); err == nil {
					valuePtr = &jObj
				} else {
					if errors.Is(err, message.ErrMessagePartNotExist) {
						parseErr = errors.New("message is empty")
					} else {
						parseErr = fmt.Errorf("parse as json: %w", err)
					}
				}
			}
		}
		return valuePtr
	}

	for k := range r.vars {
		delete(r.vars, k)
	}

	var result any = value.Nothing(nil)
	err := exec.ExecOnto(query.FunctionContext{
		Maps:     exec.Maps(),
		Vars:     r.vars,
		MsgBatch: r.msg,
		NewMeta:  r.msg.Get(0),
		NewValue: &result,
	}.WithValueFunc(lazyValue), mapping.AssignmentContext{
		Vars:  r.vars,
		Meta:  r.msg.Get(0),
		Value: &result,
	})

	if err != nil {
		var ctxErr query.ErrNoContext
		if parseErr != nil && errors.As(err, &ctxErr) {
			if ctxErr.FieldName != "" {
				err = fmt.Errorf("unable to reference message as structured (with 'this.%v'): %w", ctxErr.FieldName, parseErr)
			} else {
				err = fmt.Errorf("unable to reference message as structured (with 'this'): %w", parseErr)
			}
		}
		return "", err
	}

	var resultStr string
	switch t := result.(type) {
	case string:
		resultStr = t
	case []byte:
		resultStr = string(t)
	case value.Delete:
		return "", nil
	case value.Nothing:
		// Do not change the original contents
		if v := lazyValue(); v != nil {
			gObj := gabs.Wrap(v)
			if prettyOutput {
				resultStr = gObj.StringIndent("", "  ")
			} else {
				resultStr = gObj.String()
			}
		} else {
			resultStr = string(input)
		}
	default:
		gObj := gabs.Wrap(result)
		if prettyOutput {
			resultStr = gObj.StringIndent("", "  ")
		} else {
			resultStr = gObj.String()
		}
	}

	return resultStr, nil
}

// executeMapping compiles and executes a Bloblang mapping against JSON input.
func executeMapping(env *bloblang.Environment, input, mapping string) *ExecutionResult {
	result := &ExecutionResult{}

	if input == "" {
		s := "Input JSON string cannot be empty"
		result.MappingError = &s
		return result
	}

	if mapping == "" {
		s := "Mapping string cannot be empty"
		result.ParseError = &s
		return result
	}

	exec, err := env.NewMapping(mapping)
	if err != nil {
		var s string
		if perr, ok := err.(*parser.Error); ok {
			s = fmt.Sprintf("failed to parse mapping: %v", perr.ErrorAtPositionStructured("", []rune(mapping)))
		} else {
			s = fmt.Sprintf("mapping error: %v", err.Error())
		}
		result.ParseError = &s
		return result
	}

	output, err := NewRunner().Run(exec, false, true, []byte(input))
	if err != nil {
		s := fmt.Sprintf("execution error: %v", err.Error())
		result.MappingError = &s
	} else {
		result.Result = output
	}

	return result
}
