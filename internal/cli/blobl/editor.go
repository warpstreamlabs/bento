package blobl

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/warpstreamlabs/bento/internal/bloblang"
	"github.com/warpstreamlabs/bento/internal/bloblang/query"
	"github.com/warpstreamlabs/bento/internal/value"
)

var globalCompletionCache *completionCache

// buildSyntaxHighlightingRules creates regex patterns for ACE editor syntax highlighting.
// Examples: "root" → bloblang_root, "uuid()" → support.function, ".uppercase()" → support.method
func buildSyntaxHighlightingRules(functionNames, methodNames []string) []highlightRule {
	return []highlightRule{
		{Token: "bloblang_root", Regex: `\broot\b`},                                                          // Matches: root
		{Token: "bloblang_this", Regex: `\bthis\b`},                                                          // Matches: this
		{Token: "support.function", Regex: `(?<![\.\w])(` + strings.Join(functionNames, "|") + `)(?=\s*\()`}, // Matches: uuid(), not .uuid()
		{Token: "support.method", Regex: `\.(` + strings.Join(methodNames, "|") + `)(?=\s*\()`},              // Matches: .uppercase()
	}
}

// getOrGenerateSyntax returns cached syntax data, or generates and caches it on first call.
// Caching prevents expensive regeneration (100+ functions/methods with HTML docs) on every keystroke.
func getOrGenerateSyntax(env *bloblang.Environment) (*bloblangSyntax, error) {
	if globalCompletionCache == nil || globalCompletionCache.env != env {
		syntax, err := generateBloblangSyntax(env)
		if err != nil {
			return nil, fmt.Errorf("failed to generate Bloblang syntax: %w", err)
		}
		globalCompletionCache = &completionCache{
			syntax: &syntax,
			env:    env,
		}
	}
	return globalCompletionCache.syntax, nil
}

// validateAutocompletionRequest validates the autocompletion request
func validateAutocompletionRequest(req AutocompletionRequest) error {
	if req.Column < 0 {
		return fmt.Errorf("column position cannot be negative: %d", req.Column)
	}
	if len(req.Line) > 0 && req.Column > len(req.Line) {
		return fmt.Errorf("column position %d exceeds line length %d", req.Column, len(req.Line))
	}
	if len(req.BeforeCursor) > maxBeforeCursorLength {
		return fmt.Errorf("before cursor text too long: %d characters (max %d)", len(req.BeforeCursor), maxBeforeCursorLength)
	}
	return nil
}

// getKeywordCompletions returns Bloblang keyword completions
func getKeywordCompletions() []CompletionItem {
	keywords := []struct {
		name        string
		description string
		score       int
	}{
		{"root", "The root of the output document", 950},
		{"this", "The current context value", 950},
		{"if", "Conditional expression", 900},
		{"match", "Pattern matching expression", 900},
		{"let", "Variable assignment", 880},
		{"map", "Create named mapping", 870},
		{"else", "Alternative branch", 850},
		{"import", "Import external mapping", 800},
		{"meta", "Access message metadata", 820},
		{"deleted", "Delete the current field", 750},
	}

	var completions []CompletionItem

	for _, keyword := range keywords {
		docHTML := fmt.Sprintf(`
			<div class="ace-doc">
				<div class="ace-doc-header">
					<div class="ace-doc-signature">
						<strong>%s</strong>
					</div>
				</div>
				<div class="ace-doc-description">%s</div>
			</div>`, keyword.name, keyword.description)

		completions = append(completions, CompletionItem{
			Caption:     keyword.name,
			Value:       keyword.name,
			Meta:        "keyword",
			Type:        "keyword",
			Score:       keyword.score,
			Description: keyword.description,
			DocHTML:     docHTML,
		})
	}

	return completions
}

// getCompletions converts function/method specs with HTML into autocompletion items.
func getCompletions(specs any) []CompletionItem {
	var completions []CompletionItem

	switch s := specs.(type) {
	case map[string]functionSpecWithHTML:
		for name, spec := range s {
			completions = append(completions, buildCompletionItem(
				name,
				FunctionSpecWrapper{spec.FunctionSpec},
				spec.Category,
				"function",
				false, // isMethod
			))
		}

	case map[string]methodSpecWithHTML:
		for name, spec := range s {
			category := "general"
			if len(spec.Categories) > 0 {
				category = spec.Categories[0].Category
			}
			completions = append(completions, buildCompletionItem(
				name,
				MethodSpecWrapper{spec.MethodSpec},
				category,
				"method",
				true, // isMethod
			))
		}
	}

	return completions
}

// buildCompletionItem creates a CompletionItem from a spec wrapper.
func buildCompletionItem(name string, spec Spec, category, itemType string, isMethod bool) CompletionItem {
	// Use "general" as default if no category provided
	if category == "" {
		category = "general"
	}

	item := CompletionItem{
		Caption:     name,
		Meta:        category,
		Type:        itemType,
		Score:       getSpecScore(spec),
		Description: spec.GetDescription(),
		DocHTML:     createSpecDocHTML(name, spec, isMethod),
	}

	// Use snippet for functions with params (enables cursor positioning), otherwise plain value
	if hasSpecParameters(spec) {
		item.Snippet = name + "($1)"
	} else {
		item.Value = name + "()"
	}

	return item
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
	if isMethod {
		return fmt.Sprintf("%s/methods#%s", docsBaseURL, name)
	}
	return fmt.Sprintf("%s/functions#%s", docsBaseURL, name)
}

// formatBloblang formats Bloblang code with indentation and consistent spacing
func formatBloblang(originalMapping string) (string, error) {
	lines := strings.Split(originalMapping, "\n")
	formatted := make([]string, 0, len(lines))
	indentLevel := 0
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
	protected = multipleSpacesRegex.ReplaceAllString(protected, " ")
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

	// Match double-quoted strings (escaped quotes allowed)
	stringRegex := regexp.MustCompile(`"(?:[^"\\]|\\.)*"`)

	protected := stringRegex.ReplaceAllStringFunc(content, func(match string) string {
		index := len(literals)
		literals = append(literals, match)
		return stringLiteralPlaceholder + strconv.Itoa(index)
	})

	return protected, literals
}

// restoreStringLiterals restores protected string literals
func restoreStringLiterals(content string, literals []string) string {
	for i, literal := range literals {
		placeholderStr := stringLiteralPlaceholder + strconv.Itoa(i)
		content = strings.ReplaceAll(content, placeholderStr, literal)
	}

	return content
}

// formatLambdaOperators formats operators specifically within lambda expressions
func formatLambdaOperators(content string) string {
	// Logical operators
	content = logicalAndRegex.ReplaceAllString(content, " && ")
	content = logicalOrRegex.ReplaceAllString(content, " || ")

	// Comparison operators
	content = equalityRegex.ReplaceAllString(content, " == ")
	content = inequalityRegex.ReplaceAllString(content, " != ")
	content = greaterEqualRegex.ReplaceAllString(content, " >= ")
	content = lessEqualRegex.ReplaceAllString(content, " <= ")
	content = greaterThanRegex.ReplaceAllString(content, " > ")
	content = lessThanRegex.ReplaceAllString(content, " < ")

	return content
}

// protectLambdaExpressions replaces lambda expressions with placeholders to prevent operator formatting inside them
func protectLambdaExpressions(content string) (string, []string) {
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
		formattedLambda = multipleSpacesRegex.ReplaceAllString(formattedLambda, " ")
		formattedLambda = strings.TrimSpace(formattedLambda)

		lambdas = append(lambdas, formattedLambda)
		return lambdaPlaceholder + strconv.Itoa(len(lambdas)-1)
	})

	return content, lambdas
}

// restoreLambdaExpressions restores lambda expressions (already formatted during protection)
func restoreLambdaExpressions(content string, lambdas []string) string {

	for i, lambda := range lambdas {
		placeholderStr := lambdaPlaceholder + strconv.Itoa(i)
		// Lambda is already properly formatted, just restore it
		content = strings.ReplaceAll(content, placeholderStr, lambda)
	}

	return content
}

// formatOperators adds proper spacing around operators
func formatOperators(content string) string {
	// Handle multi-character operators first to avoid conflicts

	// Logical operators
	content = logicalAndRegex.ReplaceAllString(content, " && ")
	content = logicalOrRegex.ReplaceAllString(content, " || ")

	// Match arrows (handle before = operator)
	content = regexp.MustCompile(`\s*=>\s*`).ReplaceAllString(content, " => ")

	// Comparison operators
	content = equalityRegex.ReplaceAllString(content, " == ")
	content = inequalityRegex.ReplaceAllString(content, " != ")
	content = greaterEqualRegex.ReplaceAllString(content, " >= ")
	content = lessEqualRegex.ReplaceAllString(content, " <= ")

	// Pipe assignment (handle before single | and =)
	content = regexp.MustCompile(`\s*\|=\s*`).ReplaceAllString(content, " |= ")

	// Single character operators
	content = greaterThanRegex.ReplaceAllString(content, " > ")
	content = lessThanRegex.ReplaceAllString(content, " < ")

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
