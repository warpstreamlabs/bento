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

func formatBloblangMapping(originalMapping string) (string, error) {
	lines := strings.Split(originalMapping, "\n")
	formatted := make([]string, 0, len(lines))
	indentLevel := 0
	const indentSize = 2
	inMapBlock := false

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Handle empty lines
		if trimmed == "" {
			formatted = append(formatted, "")
			continue
		}

		// Handle comments
		if strings.HasPrefix(trimmed, "#") {
			indent := strings.Repeat(" ", indentLevel*indentSize)
			formatted = append(formatted, indent+trimmed)
			continue
		}

		// Handle map blocks (map create_foo {...)
		if strings.HasPrefix(trimmed, "map ") && strings.HasSuffix(trimmed, " {") {
			inMapBlock = true
		}

		// Handle closing braces
		if countClosingBraces(trimmed) > countOpeningBraces(trimmed) {
			indentLevel -= (countClosingBraces(trimmed) - countOpeningBraces(trimmed))
			if indentLevel < 0 {
				indentLevel = 0
			}
			if trimmed == "}" && inMapBlock {
				inMapBlock = false
			}
		}

		// Apply indentation and format content
		indent := strings.Repeat(" ", indentLevel*indentSize)
		formattedLine := formatLineContent(trimmed)
		formatted = append(formatted, indent+formattedLine)

		// Handle opening braces
		if countOpeningBraces(trimmed) > countClosingBraces(trimmed) {
			indentLevel += (countOpeningBraces(trimmed) - countClosingBraces(trimmed))
		}
	}

	return strings.Join(formatted, "\n"), nil
}

func formatLineContent(line string) string {
	// Protect string literals first to prevent formatting inside strings
	protected, literals := protectStringLiterals(line)

	// Format operators with proper spacing
	protected = formatOperators(protected)

	// Format lambda expressions correctly (preserve ->)
	protected = formatLambdaExpressions(protected)

	// Format function calls and method chains
	protected = formatFunctionCalls(protected)

	// Handle named parameters in function calls (min:10, max:20)
	protected = formatNamedParameters(protected)

	// Clean up spacing
	protected = regexp.MustCompile(`\s{2,}`).ReplaceAllString(protected, " ")
	protected = strings.TrimSpace(protected)

	// Restore string literals
	return restoreStringLiterals(protected, literals)
}

// formatLambdaExpressions preserves lambda arrow spacing
func formatLambdaExpressions(content string) string {
	// Preserve lambda arrows: item -> item.trim()
	lambdaRegex := regexp.MustCompile(`(\w+)\s*-\s*>\s*`)
	content = lambdaRegex.ReplaceAllString(content, "$1 -> ")
	return content
}

// countOpeningBraces counts opening braces and parentheses
func countOpeningBraces(line string) int {
	count := 0
	for _, char := range line {
		if char == '{' || char == '(' || char == '[' {
			count++
		}
	}
	return count
}

// countClosingBraces counts closing braces and parentheses
func countClosingBraces(line string) int {
	count := 0
	for _, char := range line {
		if char == '}' || char == ')' || char == ']' {
			count++
		}
	}
	return count
}

// protectStringLiterals replaces string literals with placeholders to prevent formatting inside strings
func protectStringLiterals(content string) (string, []string) {
	var literals []string
	placeholder := "BLOBLANG_STRING_LITERAL_"

	// Match double-quoted strings (escaped quotes allowed)
	stringRegex := regexp.MustCompile(`"(?:[^"\\]|\\.)*"`)

	protected := stringRegex.ReplaceAllStringFunc(content, func(match string) string {
		index := len(literals)
		literals = append(literals, match)
		return placeholder + fmt.Sprintf("%d", index)
	})

	return protected, literals
}

// restoreStringLiterals restores protected string literals
func restoreStringLiterals(content string, literals []string) string {
	placeholder := "BLOBLANG_STRING_LITERAL_"

	for i, literal := range literals {
		placeholderStr := placeholder + fmt.Sprintf("%d", i)
		content = strings.ReplaceAll(content, placeholderStr, literal)
	}

	return content
}

// formatOperators adds proper spacing around operators
func formatOperators(content string) string {
	// Handle multi-character operators first to avoid conflicts

	// Logical operators
	content = regexp.MustCompile(`\s*&&\s*`).ReplaceAllString(content, " && ")
	content = regexp.MustCompile(`\s*\|\|\s*`).ReplaceAllString(content, " || ")

	// Match arrows (handle before = operator)
	content = regexp.MustCompile(`\s*=>\s*`).ReplaceAllString(content, " => ")

	// Comparison operators
	content = regexp.MustCompile(`\s*==\s*`).ReplaceAllString(content, " == ")
	content = regexp.MustCompile(`\s*!=\s*`).ReplaceAllString(content, " != ")
	content = regexp.MustCompile(`\s*>=\s*`).ReplaceAllString(content, " >= ")
	content = regexp.MustCompile(`\s*<=\s*`).ReplaceAllString(content, " <= ")

	// Pipe assignment (handle before single | and =)
	content = regexp.MustCompile(`\s*\|=\s*`).ReplaceAllString(content, " |= ")

	// Single character operators
	content = regexp.MustCompile(`\s*>\s*`).ReplaceAllString(content, " > ")
	content = regexp.MustCompile(`\s*<\s*`).ReplaceAllString(content, " < ")

	// Assignment operator
	content = regexp.MustCompile(`\s*=\s*`).ReplaceAllString(content, " = ")

	// Arithmetic operators
	content = regexp.MustCompile(`\s*\+\s*`).ReplaceAllString(content, " + ")
	content = regexp.MustCompile(`\s*-\s*`).ReplaceAllString(content, " - ")
	content = regexp.MustCompile(`\s*\*\s*`).ReplaceAllString(content, " * ")
	content = regexp.MustCompile(`\s*/\s*`).ReplaceAllString(content, " / ")
	content = regexp.MustCompile(`\s*%\s*`).ReplaceAllString(content, " % ")

	// Pipe operator (for method chaining and data flow)
	content = regexp.MustCompile(`\s*\|\s*`).ReplaceAllString(content, " | ")

	// Lambda arrows
	content = regexp.MustCompile(`\s*-\s*>\s*`).ReplaceAllString(content, " -> ")

	// Match arrows
	content = regexp.MustCompile(`\s*=\s*>\s*`).ReplaceAllString(content, " => ")

	return content
}

// formatFunctionCalls formats function calls and method chains
func formatFunctionCalls(content string) string {
	// Add space after comma in function arguments
	content = regexp.MustCompile(`,\s*`).ReplaceAllString(content, ", ")

	// Remove space before opening parentheses of function calls
	content = regexp.MustCompile(`\s+\(`).ReplaceAllString(content, "(")

	// Remove space after opening parentheses and before closing parentheses
	content = regexp.MustCompile(`\(\s+`).ReplaceAllString(content, "(")
	content = regexp.MustCompile(`\s+\)`).ReplaceAllString(content, ")")

	// Format method chains with proper spacing around dots
	content = regexp.MustCompile(`\s*\.\s*`).ReplaceAllString(content, ".")

	return content
}

// formatNamedParameters handles named parameters in function calls
func formatNamedParameters(content string) string {
	// Handle named parameters like min:10, max:20
	content = regexp.MustCompile(`(\w+)\s*:\s*`).ReplaceAllString(content, "$1: ")

	// Handle named parameters with no value (just the name)
	content = regexp.MustCompile(`(\w+)\s*:\s*([^,\s)]+)`).ReplaceAllString(content, "$1: $2")

	return content
}

// formatMappingResponse represents the standardized response for mapping formatting
type formatMappingResponse struct {
	Success   bool   `json:"success"`
	Formatted string `json:"formatted"`
	Error     string `json:"error,omitempty"`
}

// FormatBloblangMapping formats Bloblang mappings
func FormatBloblangMapping(mapping string) formatMappingResponse {
	if mapping == "" {
		return formatMappingResponse{
			Success:   false,
			Error:     "Mapping cannot be empty",
			Formatted: "",
		}
	}

	// Parse the mapping to validate it
	env := bloblang.GlobalEnvironment().WithoutFunctions("env", "file")
	_, err := env.NewMapping(mapping)
	if err != nil {
		return formatMappingResponse{
			Success:   false,
			Error:     fmt.Sprintf("Parse error: %v", err),
			Formatted: mapping, // Return original on error
		}
	}

	// Format using AST structure
	formatted, formatErr := formatBloblangMapping(mapping)
	if formatErr != nil {
		return formatMappingResponse{
			Success:   false,
			Error:     formatErr.Error(),
			Formatted: mapping, // Return original on error
		}
	}

	return formatMappingResponse{
		Success:   true,
		Formatted: formatted,
		Error:     "",
	}
}
