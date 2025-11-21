// Package blobl provides core functionality for the Bloblang language playground,
// including code execution, syntax highlighting, autocompletion, and formatting.
package blobl

import (
	"encoding/json"
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

// functionSpecWithHTML extends FunctionSpec with pre-generated HTML documentation
type functionSpecWithHTML struct {
	query.FunctionSpec
	DocHTML string `json:"docHTML"`
}

// methodSpecWithHTML extends MethodSpec with pre-generated HTML documentation
type methodSpecWithHTML struct {
	query.MethodSpec
	DocHTML string `json:"docHTML"`
}

// bloblangSyntax contains syntax metadata for the Bloblang language,
// including function/method specs and highlighting rules.
type bloblangSyntax struct {
	// Rich function and method data with pre-generated documentation HTML
	Functions map[string]functionSpecWithHTML `json:"functions"`
	Methods   map[string]methodSpecWithHTML   `json:"methods"`

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
	functions := make(map[string]functionSpecWithHTML)
	methods := make(map[string]methodSpecWithHTML)

	env.WithoutFunctions("env", "file").WalkFunctions(func(name string, spec query.FunctionSpec) {
		// Pre-generate documentation HTML for this function
		wrapper := FunctionSpecWrapper{spec}
		docHTML := createSpecDocHTML(name, wrapper, false)

		functions[name] = functionSpecWithHTML{
			FunctionSpec: spec,
			DocHTML:      docHTML,
		}
		functionNames = append(functionNames, name)
	})

	env.WalkMethods(func(name string, spec query.MethodSpec) {
		// Pre-generate documentation HTML for this method
		wrapper := MethodSpecWrapper{spec}
		docHTML := createSpecDocHTML(name, wrapper, true)

		methods[name] = methodSpecWithHTML{
			MethodSpec: spec,
			DocHTML:    docHTML,
		}
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

// CompletionItem represents an autocompletion suggestion
type CompletionItem struct {
	Caption     string `json:"caption"`     // Display name
	Value       string `json:"value"`       // Insert value
	Snippet     string `json:"snippet"`     // Insert with cursor positioning
	Meta        string `json:"meta"`        // Category/type description
	Type        string `json:"type"`        // "function", "method", "keyword"
	Score       int    `json:"score"`       // Priority score (higher = more important)
	DocHTML     string `json:"docHTML"`     // Documentation HTML
	Description string `json:"description"` // Simple description
}

// AutocompletionRequest represents a request for autocompletion
type AutocompletionRequest struct {
	Line         string `json:"line"`         // The current line text
	Column       int    `json:"column"`       // Cursor column position
	BeforeCursor string `json:"beforeCursor"` // Text before cursor position
}

// AutocompletionResponse represents the response with completion suggestions
type AutocompletionResponse struct {
	Completions []CompletionItem `json:"completions"`
	Success     bool             `json:"success"`
	Error       string           `json:"error,omitempty"`
}

// Spec represents a unified interface for function and method specs
type Spec interface {
	GetDescription() string
	GetStatus() query.Status
	GetVersion() string
	GetParams() query.Params
}

// FunctionSpecWrapper wraps query.FunctionSpec to implement Spec interface
type FunctionSpecWrapper struct {
	query.FunctionSpec
}

func (f FunctionSpecWrapper) GetDescription() string  { return f.Description }
func (f FunctionSpecWrapper) GetStatus() query.Status { return f.Status }
func (f FunctionSpecWrapper) GetVersion() string      { return f.Version }
func (f FunctionSpecWrapper) GetParams() query.Params { return f.Params }

// MethodSpecWrapper wraps query.MethodSpec to implement Spec interface
type MethodSpecWrapper struct {
	query.MethodSpec
}

func (m MethodSpecWrapper) GetDescription() string  { return m.Description }
func (m MethodSpecWrapper) GetStatus() query.Status { return m.Status }
func (m MethodSpecWrapper) GetVersion() string      { return m.Version }
func (m MethodSpecWrapper) GetParams() query.Params { return m.Params }

// completionCache holds cached syntax data to avoid repeated environment walks
type completionCache struct {
	syntax *bloblangSyntax
	env    *bloblang.Environment
}

var globalCompletionCache *completionCache

// getSyntaxData returns cached syntax data or generates it if needed
func getSyntaxData(env *bloblang.Environment) (*bloblangSyntax, error) {
	if globalCompletionCache == nil || globalCompletionCache.env != env {
		syntax, err := generateBloblangSyntax(env)
		if err != nil {
			return nil, err
		}
		globalCompletionCache = &completionCache{
			syntax: &syntax,
			env:    env,
		}
	}
	return globalCompletionCache.syntax, nil
}

// generateAutocompletion provides context-aware autocompletion for Bloblang
func GenerateAutocompletion(env *bloblang.Environment, req AutocompletionRequest) AutocompletionResponse {
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
	syntaxData, err := getSyntaxData(env)
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
		// Add method completions
		completions = append(completions, getCompletions(syntaxData.Methods)...)
	} else {
		// Add function and keyword completions
		completions = append(completions, getCompletions(syntaxData.Functions)...)
		completions = append(completions, getKeywordCompletions()...)
	}

	return AutocompletionResponse{
		Completions: completions,
		Success:     true,
	}
}

// validateAutocompletionRequest validates the autocompletion request
func validateAutocompletionRequest(req AutocompletionRequest) error {
	if req.Column < 0 {
		return fmt.Errorf("column position cannot be negative: %d", req.Column)
	}
	if len(req.Line) > 0 && req.Column > len(req.Line) {
		return fmt.Errorf("column position %d exceeds line length %d", req.Column, len(req.Line))
	}
	if len(req.BeforeCursor) > 1000 {
		return fmt.Errorf("before cursor text too long: %d characters (max 1000)", len(req.BeforeCursor))
	}
	return nil
}

// getKeywordCompletions returns Bloblang keyword completions
func getKeywordCompletions() []CompletionItem {
	keywords := []struct {
		name        string
		description string
		score       int
		category    string
	}{
		{"root", "The root of the output document", 950, "core"},
		{"this", "The current context value", 950, "core"},
		{"if", "Conditional expression", 900, "control"},
		{"match", "Pattern matching expression", 900, "control"},
		{"let", "Variable assignment", 880, "variable"},
		{"map", "Create named mapping", 870, "mapping"},
		{"else", "Alternative branch", 850, "control"},
		{"import", "Import external mapping", 800, "mapping"},
		{"meta", "Access message metadata", 820, "metadata"},
		{"deleted", "Delete the current field", 750, "mutation"},
	}

	var completions []CompletionItem

	for _, keyword := range keywords {
		docHTML := fmt.Sprintf(`
			<div class="ace-doc">
				<div class="ace-doc-header">
					<div class="ace-doc-signature">
						<strong>%s</strong>
						<span class="ace-keyword-category">%s</span>
					</div>
				</div>
				<div class="ace-doc-description">%s</div>
			</div>`, keyword.name, keyword.category, keyword.description)

		completions = append(completions, CompletionItem{
			Caption:     keyword.name,
			Value:       keyword.name,
			Meta:        keyword.category,
			Type:        "keyword",
			Score:       keyword.score,
			Description: keyword.description,
			DocHTML:     docHTML,
		})
	}

	return completions
}

// getCompletions returns completions for functions or methods
func getCompletions(specs any) []CompletionItem {
	var completions []CompletionItem

	switch s := specs.(type) {
	case map[string]query.FunctionSpec:
		for name, spec := range s {
			wrapper := FunctionSpecWrapper{spec}
			completion := CompletionItem{
				Caption:     name,
				Meta:        spec.Category,
				Type:        "function",
				Score:       getSpecScore(wrapper),
				Description: wrapper.GetDescription(),
				DocHTML:     createSpecDocHTML(name, wrapper, false),
			}

			// Check if spec has parameters for snippet vs value
			if hasSpecParameters(wrapper) {
				completion.Snippet = fmt.Sprintf("%s($1)", name)
			} else {
				completion.Value = fmt.Sprintf("%s()", name)
			}

			completions = append(completions, completion)
		}

	case map[string]query.MethodSpec:
		for name, spec := range s {
			wrapper := MethodSpecWrapper{spec}
			category := "general"
			if len(spec.Categories) > 0 {
				category = spec.Categories[0].Category
			}

			completion := CompletionItem{
				Caption:     name,
				Meta:        category,
				Type:        "method",
				Score:       getSpecScore(wrapper),
				Description: wrapper.GetDescription(),
				DocHTML:     createSpecDocHTML(name, wrapper, true),
			}

			// Check if spec has parameters for snippet vs value
			if hasSpecParameters(wrapper) {
				completion.Snippet = fmt.Sprintf("%s($1)", name)
			} else {
				completion.Value = fmt.Sprintf("%s()", name)
			}

			completions = append(completions, completion)
		}
	}

	return completions
}

func getSpecScore(spec Spec) int {
	// Prioritize by status (stable > beta > experimental)
	switch spec.GetStatus() {
	case query.StatusStable:
		return 1000
	case query.StatusBeta:
		return 800
	case query.StatusExperimental:
		return 600
	}
	return 500
}

func hasSpecParameters(spec Spec) bool {
	params := spec.GetParams()
	return len(params.Definitions) > 0 || params.Variadic
}

// createSpecDocHTML creates HTML documentation for Bloblang functions and methods
func createSpecDocHTML(name string, spec Spec, isMethod bool) string {
	// Build signature with parameters
	signature := buildSpecSignature(name, spec, isMethod)
	status := strings.ToLower(string(spec.GetStatus()))

	// Generate documentation URL
	docUrl := generateDocumentationLink(name, isMethod)

	var html strings.Builder
	html.WriteString(fmt.Sprintf(`<div class="ace-doc" data-docs-url="%s" data-function-name="%s" data-is-method="%t">`, docUrl, name, isMethod))

	// Header with signature and status
	html.WriteString(fmt.Sprintf(`
		<div class="ace-doc-header clickable-header" title="Click to view documentation">
			<div class="ace-doc-signature">
				<strong>%s</strong>
				<span class="ace-status-%s">%s</span>
			</div>
		</div>`, signature, status, status))

	// Version information
	if spec.GetVersion() != "" {
		html.WriteString(fmt.Sprintf(`
		<div class="ace-doc-version">Since: v%s</div>`, spec.GetVersion()))
	}

	// Parameters section
	params := spec.GetParams()
	if len(params.Definitions) > 0 {
		html.WriteString(`
		<div class="ace-doc-parameters">
			<strong>Parameters:</strong>`)

		for _, param := range params.Definitions {
			paramType := getParamTypeString(param)
			html.WriteString(fmt.Sprintf(`
			<div class="ace-doc-param">
				<code>%s [%s]</code><br/>`, param.Name, paramType))

			if param.Description != "" {
				processedDesc := processMarkdownDescription(param.Description)
				html.WriteString(fmt.Sprintf(`
				<span class="ace-doc-param-desc">%s</span>`, processedDesc))
			}
			html.WriteString(`</div>`)
		}
		html.WriteString(`</div>`)
	}

	html.WriteString(`</div>`)
	return html.String()
}

// buildSpecSignature creates the signature with parameters for functions and methods
func buildSpecSignature(name string, spec Spec, isMethod bool) string {
	var sig strings.Builder

	if isMethod {
		sig.WriteString(".")
	}
	sig.WriteString(name)

	params := spec.GetParams()
	if len(params.Definitions) > 0 {
		sig.WriteString("(")
		for i, param := range params.Definitions {
			if i > 0 {
				sig.WriteString(", ")
			}
			paramType := getParamTypeString(param)
			sig.WriteString(fmt.Sprintf("%s: %s", param.Name, paramType))
		}
		if params.Variadic {
			if len(params.Definitions) > 0 {
				sig.WriteString(", ")
			}
			sig.WriteString("...")
		}
		sig.WriteString(")")
	} else if params.Variadic {
		sig.WriteString("(...)")
	} else {
		sig.WriteString("()")
	}

	return sig.String()
}

// getParamTypeString converts Bloblang parameter type to string
func getParamTypeString(param query.ParamDefinition) string {
	switch param.ValueType {
	case value.TString:
		return "string"
	case value.TInt:
		return "integer"
	case value.TFloat:
		return "number"
	case value.TBool:
		return "boolean"
	case value.TArray:
		return "array"
	case value.TObject:
		return "object"
	case value.TTimestamp:
		return "timestamp"
	default:
		return "any"
	}
}

// processMarkdownDescription processes markdown in descriptions
func processMarkdownDescription(description string) string {
	if description == "" {
		return ""
	}

	// Strip admonitions (:::warning:::, etc.)
	re := regexp.MustCompile(`:::([a-zA-Z]+)[\s\S]*?:::`)
	processed := re.ReplaceAllString(description, "")

	// Process inline code (backticks)
	re = regexp.MustCompile("`([^`]+)`")
	processed = re.ReplaceAllString(processed, "<code>$1</code>")

	// Process markdown links [text](url)
	re = regexp.MustCompile(`\[([^\]]+)\]\(([^)]+)\)`)
	processed = re.ReplaceAllString(processed, `<a href="$2" target="_blank">$1</a>`)

	return strings.TrimSpace(processed)
}

// generateDocumentationLink creates the documentation URL
func generateDocumentationLink(name string, isMethod bool) string {
	baseURL := "https://warpstreamlabs.github.io/bento/docs/guides/bloblang"
	if isMethod {
		return fmt.Sprintf("%s/methods#%s", baseURL, name)
	}
	return fmt.Sprintf("%s/functions#%s", baseURL, name)
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

	// Protect lambda expressions to prevent operator formatting inside them
	protected, lambdas := protectLambdaExpressions(protected)

	// Format operators with proper spacing
	protected = formatOperators(protected)

	// Format function calls and method chains
	protected = formatFunctionCalls(protected)

	// Handle named parameters in function calls (min:10, max:20)
	protected = formatNamedParameters(protected)

	// Clean up spacing
	protected = regexp.MustCompile(`\s{2,}`).ReplaceAllString(protected, " ")
	protected = strings.TrimSpace(protected)

	// Restore lambda expressions with proper formatting
	protected = restoreLambdaExpressions(protected, lambdas)

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

// formatLambdaOperators formats operators specifically within lambda expressions
func formatLambdaOperators(content string) string {
	// Logical operators
	content = regexp.MustCompile(`\s*&&\s*`).ReplaceAllString(content, " && ")
	content = regexp.MustCompile(`\s*\|\|\s*`).ReplaceAllString(content, " || ")

	// Comparison operators
	content = regexp.MustCompile(`\s*==\s*`).ReplaceAllString(content, " == ")
	content = regexp.MustCompile(`\s*!=\s*`).ReplaceAllString(content, " != ")
	content = regexp.MustCompile(`\s*>=\s*`).ReplaceAllString(content, " >= ")
	content = regexp.MustCompile(`\s*<=\s*`).ReplaceAllString(content, " <= ")
	content = regexp.MustCompile(`\s*>\s*`).ReplaceAllString(content, " > ")
	content = regexp.MustCompile(`\s*<\s*`).ReplaceAllString(content, " < ")

	return content
}

// protectLambdaExpressions replaces lambda expressions with placeholders to prevent operator formatting inside them
func protectLambdaExpressions(content string) (string, []string) {
	placeholder := "BLOBLANG_LAMBDA_"
	var lambdas []string

	// Match lambda expressions like: c -> c == "," || c == " "
	// This captures the parameter(s), arrow, and expression
	lambdaRegex := regexp.MustCompile(`(\w+)\s*->\s*([^,)}\]]+(?:\([^)]*\)[^,)}\]]*)*)\s*`)

	content = lambdaRegex.ReplaceAllStringFunc(content, func(match string) string {
		formattedLambda := match
		// Apply lambda-specific operator formatting
		formattedLambda = formatLambdaOperators(formattedLambda)
		// Ensure proper lambda arrow spacing
		formattedLambda = formatLambdaExpressions(formattedLambda)
		// Remove excessive whitespace
		formattedLambda = regexp.MustCompile(`\s{2,}`).ReplaceAllString(formattedLambda, " ")
		formattedLambda = strings.TrimSpace(formattedLambda)

		lambdas = append(lambdas, formattedLambda)
		return placeholder + fmt.Sprintf("%d", len(lambdas)-1)
	})

	return content, lambdas
}

// restoreLambdaExpressions restores lambda expressions (already formatted during protection)
func restoreLambdaExpressions(content string, lambdas []string) string {
	placeholder := "BLOBLANG_LAMBDA_"

	for i, lambda := range lambdas {
		placeholderStr := placeholder + fmt.Sprintf("%d", i)
		// Lambda is already properly formatted, just restore it
		content = strings.ReplaceAll(content, placeholderStr, lambda)
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

// FormatMappingResponse represents the standardized response for mapping formatting
type FormatMappingResponse struct {
	Success   bool   `json:"success"`
	Formatted string `json:"formatted"`
	Error     string `json:"error,omitempty"`
}

// ValidationResponse represents the response from validating a Bloblang mapping
type ValidationResponse struct {
	Valid bool   `json:"valid"`
	Error string `json:"error,omitempty"`
}

// JSONResponse represents the response from JSON formatting operations
type JSONResponse struct {
	Success bool   `json:"success"`
	Result  string `json:"result"`
	Error   string `json:"error,omitempty"`
}

// ValidateBloblangMapping validates a Bloblang mapping without executing it.
// Note: Not currently used by the playground UI ('execute' already validates), but exposed
// for future integrations / consumers
func ValidateBloblangMapping(env *bloblang.Environment, mapping string) ValidationResponse {
	if mapping == "" {
		return ValidationResponse{
			Valid: false,
			Error: "Mapping cannot be empty",
		}
	}

	// Parse the mapping to validate it
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

// FormatJSON formats a JSON string with 2-space indentation
func FormatJSON(jsonString string) JSONResponse {
	if jsonString == "" {
		return JSONResponse{
			Success: false,
			Result:  "",
			Error:   "JSON string cannot be empty",
		}
	}

	var parsed any
	if err := json.Unmarshal([]byte(jsonString), &parsed); err != nil {
		return JSONResponse{
			Success: false,
			Result:  jsonString, // Return original on error
			Error:   fmt.Sprintf("Invalid JSON: %v", err),
		}
	}

	formatted, err := json.MarshalIndent(parsed, "", "  ")
	if err != nil {
		return JSONResponse{
			Success: false,
			Result:  jsonString,
			Error:   fmt.Sprintf("Failed to format JSON: %v", err),
		}
	}

	return JSONResponse{
		Success: true,
		Result:  string(formatted),
	}
}

// MinifyJSON compacts a JSON string by removing whitespace
func MinifyJSON(jsonString string) JSONResponse {
	if jsonString == "" {
		return JSONResponse{
			Success: false,
			Result:  "",
			Error:   "JSON string cannot be empty",
		}
	}

	var parsed any
	if err := json.Unmarshal([]byte(jsonString), &parsed); err != nil {
		return JSONResponse{
			Success: false,
			Result:  jsonString, // Return original on error
			Error:   fmt.Sprintf("Invalid JSON: %v", err),
		}
	}

	minified, err := json.Marshal(parsed)
	if err != nil {
		return JSONResponse{
			Success: false,
			Result:  jsonString,
			Error:   fmt.Sprintf("Failed to minify JSON: %v", err),
		}
	}

	return JSONResponse{
		Success: true,
		Result:  string(minified),
	}
}

// ValidateJSON checks if a string is valid JSON
func ValidateJSON(jsonString string) ValidationResponse {
	if jsonString == "" {
		return ValidationResponse{
			Valid: false,
			Error: "JSON string cannot be empty",
		}
	}

	var parsed any
	if err := json.Unmarshal([]byte(jsonString), &parsed); err != nil {
		return ValidationResponse{
			Valid: false,
			Error: fmt.Sprintf("Invalid JSON: %v", err),
		}
	}

	return ValidationResponse{
		Valid: true,
	}
}

// FormatBloblangMapping formats Bloblang mappings
func FormatBloblangMapping(mapping string) FormatMappingResponse {
	if mapping == "" {
		return FormatMappingResponse{
			Success:   false,
			Error:     "Mapping cannot be empty",
			Formatted: "",
		}
	}

	// Parse the mapping to validate it
	env := bloblang.GlobalEnvironment().WithoutFunctions("env", "file")
	_, err := env.NewMapping(mapping)
	if err != nil {
		return FormatMappingResponse{
			Success:   false,
			Error:     fmt.Sprintf("Parse error: %v", err),
			Formatted: mapping, // Return original on error
		}
	}

	// Format using AST structure
	formatted, formatErr := formatBloblangMapping(mapping)
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
