// Package blobl provides core functionality for the Bloblang language playground,
// including code execution, syntax highlighting, autocompletion, and formatting.
package blobl

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/Jeffail/gabs/v2"
	"github.com/warpstreamlabs/bento/internal/bloblang"
	"github.com/warpstreamlabs/bento/internal/bloblang/mapping"
	"github.com/warpstreamlabs/bento/internal/bloblang/parser"
	"github.com/warpstreamlabs/bento/internal/bloblang/query"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/value"
)

// Configuration constants
const (
	// Autocompletion limits
	maxBeforeCursorLength = 1000

	// Formatting configuration
	indentSize = 2

	// Placeholder prefixes for string protection during formatting
	stringLiteralPlaceholder = "BLOBLANG_STRING_LITERAL_"
	lambdaPlaceholder        = "BLOBLANG_LAMBDA_"

	// Documentation base URL
	docsBaseURL = "https://warpstreamlabs.github.io/bento/docs/guides/bloblang"
)

// Compiled regex patterns for formatting (compiled once at package initialization)
var (
	// Whitespace cleanup
	multipleSpacesRegex = regexp.MustCompile(`\s{2,}`)

	// Logical operators
	logicalAndRegex = regexp.MustCompile(`\s*&&\s*`)
	logicalOrRegex  = regexp.MustCompile(`\s*\|\|\s*`)

	// Comparison operators
	equalityRegex     = regexp.MustCompile(`\s*==\s*`)
	inequalityRegex   = regexp.MustCompile(`\s*!=\s*`)
	greaterEqualRegex = regexp.MustCompile(`\s*>=\s*`)
	lessEqualRegex    = regexp.MustCompile(`\s*<=\s*`)
	greaterThanRegex  = regexp.MustCompile(`\s*>\s*`)
	lessThanRegex     = regexp.MustCompile(`\s*<\s*`)
)

// newExecCache creates a new execution cache for running mappings.
func newExecCache() *execCache {
	return &execCache{
		msg:  message.QuickBatch([][]byte{[]byte(nil)}),
		vars: map[string]any{},
	}
}

// runBloblangExecutor runs a compiled Bloblang mapping executor against input data.
// It supports both raw and structured input, and optionally pretty-prints output.
func (e *execCache) runBloblangExecutor(exec *mapping.Executor, rawInput, prettyOutput bool, input []byte) (string, error) {
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

// executeBloblangMapping compiles and executes a Bloblang mapping against JSON input.
// Returns an executionResult with parsed result or error details.
func executeBloblangMapping(env *bloblang.Environment, input, mapping string) *executionResult {
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

	exec, err := env.NewMapping(mapping)
	if err != nil {
		if perr, ok := err.(*parser.Error); ok {
			result.ParseError = fmt.Sprintf("failed to parse mapping: %v", perr.ErrorAtPositionStructured("", []rune(mapping)))
		} else {
			result.ParseError = fmt.Sprintf("mapping error: %v", err.Error())
		}
		return result
	}

	execCache := newExecCache()
	output, err := execCache.runBloblangExecutor(exec, false, true, []byte(input))
	if err != nil {
		result.MappingError = fmt.Sprintf("execution error: %v", err.Error())
	} else {
		result.Result = output
	}

	return result
}

// validateBloblangMapping validates a Bloblang mapping without executing it.
// Note: Not currently used by the playground UI ('execute' already validates), but exposed
// for future integrations / consumers
func validateBloblangMapping(env *bloblang.Environment, mapping string) ValidationResponse {
	if mapping == "" {
		return ValidationResponse{
			Valid: false,
			Error: "Mapping cannot be empty",
		}
	}

	_, err := env.NewMapping(mapping)
	if err != nil {
		return ValidationResponse{
			Valid: false,
			Error: err.Error(),
		}
	}

	return ValidationResponse{
		Valid: true,
	}
}

// generateBloblangSyntax builds metadata for the ACE editor (autocompletion, syntax highlighting, tooltips).
// Iterates through all functions/methods in the environment and pre-generates HTML documentation.
func generateBloblangSyntax(env *bloblang.Environment) (bloblangSyntax, error) {
	var functionNames, methodNames []string
	functions := make(map[string]functionSpecWithHTML)
	methods := make(map[string]methodSpecWithHTML)

	// Walk all functions: uuid(), timestamp(), etc.
	env.WalkFunctions(func(name string, spec query.FunctionSpec) {
		wrapper := FunctionSpecWrapper{spec}
		functions[name] = functionSpecWithHTML{
			FunctionSpec: spec,
			DocHTML:      createSpecDocHTML(name, wrapper, false),
		}
		functionNames = append(functionNames, name)
	})

	// Walk all methods: .uppercase(), .split(), etc.
	env.WalkMethods(func(name string, spec query.MethodSpec) {
		wrapper := MethodSpecWrapper{spec}
		methods[name] = methodSpecWithHTML{
			MethodSpec: spec,
			DocHTML:    createSpecDocHTML(name, wrapper, true),
		}
		methodNames = append(methodNames, name)
	})

	return bloblangSyntax{
		Functions:     functions,
		Methods:       methods,
		Rules:         buildSyntaxHighlightingRules(functionNames, methodNames),
		FunctionNames: functionNames,
		MethodNames:   methodNames,
	}, nil
}

// formatBloblangMapping formats Bloblang mappings
func formatBloblangMapping(env *bloblang.Environment, mapping string) FormatMappingResponse {
	if mapping == "" {
		return FormatMappingResponse{
			Success:   false,
			Error:     "Mapping cannot be empty",
			Formatted: "",
		}
	}

	// Parse the mapping to validate it
	_, err := env.NewMapping(mapping)
	if err != nil {
		return FormatMappingResponse{
			Success:   false,
			Error:     fmt.Sprintf("Parse error: %v", err),
			Formatted: mapping, // Return original on error
		}
	}

	// Format using AST structure
	formatted, formatErr := formatBloblang(mapping)
	if formatErr != nil {
		return FormatMappingResponse{
			Success:   false,
			Error:     formatErr.Error(),
			Formatted: mapping, // Return original on error
		}
	}

	return FormatMappingResponse{
		Success:   true,
		Formatted: formatted,
		Error:     "",
	}
}

// generateAutocompletion provides context-aware autocompletion for Bloblang
func generateAutocompletion(env *bloblang.Environment, req AutocompletionRequest) AutocompletionResponse {
	// Validate input
	if err := validateAutocompletionRequest(req); err != nil {
		return AutocompletionResponse{
			Completions: []CompletionItem{},
			Success:     false,
			Error:       err.Error(),
		}
	}

	// Don't suggest completions inside comments
	if strings.Contains(req.BeforeCursor, "#") {
		return AutocompletionResponse{
			Completions: []CompletionItem{},
			Success:     true,
		}
	}

	// Get cached syntax data
	syntaxData, err := getOrGenerateSyntax(env)
	if err != nil {
		return AutocompletionResponse{
			Completions: []CompletionItem{},
			Success:     false,
			Error:       fmt.Sprintf("Failed to get syntax data: %v", err),
		}
	}

	var completions []CompletionItem

	// Determine context: method vs function/keyword context
	isMethodContext := regexp.MustCompile(`\.\w*$`).MatchString(req.BeforeCursor)
	if isMethodContext {
		completions = append(completions, getCompletions(syntaxData.Methods)...)
	} else {
		completions = append(completions, getCompletions(syntaxData.Functions)...)
		completions = append(completions, getKeywordCompletions()...)
	}

	return AutocompletionResponse{
		Completions: completions,
		Success:     true,
	}
}
