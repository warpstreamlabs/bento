package blobl

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Jeffail/gabs/v2"
	"github.com/warpstreamlabs/bento/internal/bloblang"
	"github.com/warpstreamlabs/bento/internal/bloblang/mapping"
	"github.com/warpstreamlabs/bento/internal/bloblang/parser"
	"github.com/warpstreamlabs/bento/internal/bloblang/query"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/value"
)

// executionResult represents the result of executing a Bloblang mapping.
type executionResult struct {
	Result       any `json:"result"`
	ParseError   any `json:"parse_error"`
	MappingError any `json:"mapping_error"`
}

// execCache is used to execute Bloblang mappings with cached state.
type execCache struct {
	msg  message.Batch
	vars map[string]any
}

// newExecCache creates a new execution cache for running mappings.
func newExecCache() *execCache {
	return &execCache{
		msg:  message.QuickBatch([][]byte{[]byte(nil)}),
		vars: map[string]any{},
	}
}

// highlightRule represents a token type and regex pattern for syntax highlighting.
// Token is a semantic class used to apply styling.
// Regex is a JS-compatible regex pattern used to match code tokens.
type highlightRule struct {
	Token string `json:"token"`
	Regex string `json:"regex"`
}

// bloblangSyntax contains syntax metadata for the Bloblang language,
// including function/method specs and highlighting rules.
type bloblangSyntax struct {
	// Rich function and method data
	Functions map[string]query.FunctionSpec `json:"functions"`
	Methods   map[string]query.MethodSpec   `json:"methods"`

	// Minimal syntax highlighting rules that extends CoffeeScript
	Rules []highlightRule `json:"rules"`

	// Quick lookup arrays for regex generation
	FunctionNames []string `json:"function_names"`
	MethodNames   []string `json:"method_names"`
}

// Walker abstracts the ability to walk over registered functions and methods.
type Walker interface {
	WalkFunctions(fn func(name string, spec query.FunctionSpec))
	WalkMethods(fn func(name string, spec query.MethodSpec))
}

// executeBloblangMapping runs a compiled Bloblang mapping executor against input data.
// It supports both raw and structured input, and optionally pretty-prints output.
func (e *execCache) executeBloblangMapping(exec *mapping.Executor, rawInput, prettyOutput bool, input []byte) (string, error) {
	e.msg.Get(0).SetBytes(input)

	var valuePtr *any
	var parseErr error

	// parse input as structured data if needed
	lazyValue := func() *any {
		if valuePtr == nil && parseErr == nil {
			if rawInput {
				var value any = input
				valuePtr = &value
			} else {
				if jObj, err := e.msg.Get(0).AsStructured(); err == nil {
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

	for k := range e.vars {
		delete(e.vars, k)
	}

	var result any = value.Nothing(nil)
	err := exec.ExecOnto(query.FunctionContext{
		Maps:     exec.Maps(),
		Vars:     e.vars,
		MsgBatch: e.msg,
		NewMeta:  e.msg.Get(0),
		NewValue: &result,
	}.WithValueFunc(lazyValue), mapping.AssignmentContext{
		Vars:  e.vars,
		Meta:  e.msg.Get(0),
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

// evaluateMapping compiles and executes a Bloblang mapping string against a JSON input string.
// Returns an executionResult containing with the output or error details.
func evaluateMapping(env *bloblang.Environment, input, mapping string) *executionResult {
	result := &executionResult{
		Result:       nil,
		ParseError:   nil,
		MappingError: nil,
	}

	if input == "" {
		result.MappingError = "Input JSON string cannot be empty"
		return result
	}

	if mapping == "" {
		result.ParseError = "Mapping string cannot be empty"
		return result
	}

	exec, err := env.WithoutFunctions("env", "file").NewMapping(mapping)
	if err != nil {
		if perr, ok := err.(*parser.Error); ok {
			result.ParseError = fmt.Sprintf("failed to parse mapping: %v", perr.ErrorAtPositionStructured("", []rune(mapping)))
		} else {
			result.ParseError = fmt.Sprintf("mapping error: %v", err.Error())
		}
		return result
	}

	execCache := newExecCache()
	output, err := execCache.executeBloblangMapping(exec, false, true, []byte(input))
	if err != nil {
		result.MappingError = fmt.Sprintf("execution error: %v", err.Error())
	} else {
		result.Result = output
	}

	return result
}

// generateBloblangSyntax returns Bloblang syntax metadata for editor tooling.
func generateBloblangSyntax(env *bloblang.Environment) (bloblangSyntax, error) {
	var functionNames, methodNames []string
	functions := make(map[string]query.FunctionSpec)
	methods := make(map[string]query.MethodSpec)

	env.WithoutFunctions("env", "file").WalkFunctions(func(name string, spec query.FunctionSpec) {
		functions[name] = spec
		functionNames = append(functionNames, name)
	})

	env.WalkMethods(func(name string, spec query.MethodSpec) {
		methods[name] = spec
		methodNames = append(methodNames, name)
	})

	// Generate syntax highlighting rules
	rules := []highlightRule{
		// Matches `root` (highest priority)
		{Token: "bloblang_root", Regex: `\broot\b`},
		// Matches `this` (highest priority)
		{Token: "bloblang_this", Regex: `\bthis\b`},
		// Matches `foo()`
		{Token: "support.function", Regex: `(?<![\.\w])(` + strings.Join(functionNames, "|") + `)(?=\s*\()`},
		// Matches `.bar()`
		{Token: "support.method", Regex: `\.(` + strings.Join(methodNames, "|") + `)(?=\s*\()`},
	}

	syntax := bloblangSyntax{
		Functions:     functions,
		Methods:       methods,
		Rules:         rules,
		FunctionNames: functionNames,
		MethodNames:   methodNames,
	}

	return syntax, nil
}
