package blobl

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/warpstreamlabs/bento/internal/bloblang/query"
)

type SpecKind int

const (
	SpecFunction SpecKind = iota
	SpecMethod
)

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

// validateAutocompletionRequest validates the autocompletion request
func validateAutocompletionRequest(req AutocompletionRequest) error {
	if req.Column < 0 {
		return fmt.Errorf("column position cannot be negative: %d", req.Column)
	}
	if len(req.Line) > 0 && req.Column > len(req.Line) {
		return fmt.Errorf("column position %d exceeds line length %d", req.Column, len(req.Line))
	}
	return nil
}

// getKeywordCompletions returns Bloblang keyword completions
func getKeywordCompletions() []CompletionItem {
	keywords := []struct {
		name        string
		description string
		score       statusPriority
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

	const itemHTMLFormat = `<div class="ace-doc">
	<div class="ace-doc-header">
		<div class="ace-doc-signature">
			<strong>%s</strong>
		</div>
	</div>
	<div class="ace-doc-description">%s</div>
</div>`

	for _, keyword := range keywords {
		docHTML := fmt.Sprintf(itemHTMLFormat, keyword.name, keyword.description)
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

func getFunctionCompletions(specs map[string]functionSpecWithHTML) []CompletionItem {
	completions := make([]CompletionItem, 0, len(specs))
	for name, spec := range specs {
		completions = append(completions, buildCompletionItem(
			name, spec.BaseSpec, spec.Category, "function", SpecFunction,
		))
	}
	return completions
}

func getMethodCompletions(specs map[string]methodSpecWithHTML) []CompletionItem {
	completions := make([]CompletionItem, 0, len(specs))
	for name, spec := range specs {
		category := "general"
		if len(spec.Categories) > 0 {
			category = spec.Categories[0].Category
		}
		completions = append(completions, buildCompletionItem(
			name, spec.BaseSpec, category, "method", SpecMethod,
		))
	}
	return completions
}

// buildCompletionItem creates a CompletionItem from a spec wrapper.
func buildCompletionItem(
	name string,
	spec query.BaseSpec,
	category string,
	itemType string,
	kind SpecKind,
) CompletionItem {
	// Use "general" as default if no category provided
	if category == "" {
		category = "general"
	}

	item := CompletionItem{
		Caption:     name,
		Meta:        category,
		Type:        itemType,
		Score:       getSpecScore(spec.Status),
		Description: spec.Description,
		DocHTML:     createSpecDocHTML(name, spec, kind),
	}

	// Use snippet for functions with params (enables cursor positioning), otherwise plain value
	if hasSpecParameters(spec) {
		item.Snippet = name + "($1)"
	} else {
		item.Value = name + "()"
	}

	return item
}

type statusPriority int

const (
	Unknown statusPriority = iota
	Experimental
	Beta
	Stable
)

func getSpecScore(status query.Status) statusPriority {
	// Prioritize by status (stable > beta > experimental)
	switch status {
	case query.StatusStable:
		return Stable
	case query.StatusBeta:
		return Beta
	case query.StatusExperimental:
		return Experimental
	}
	return Unknown
}

func hasSpecParameters(spec query.BaseSpec) bool {
	return len(spec.Params.Definitions) > 0 || spec.Params.Variadic
}

// createSpecDocHTML creates HTML documentation for Bloblang functions and methods
func createSpecDocHTML(name string, spec query.BaseSpec, kind SpecKind) string {
	signature := buildSpecSignature(name, spec, kind)
	status := strings.ToLower(string(spec.Status))
	docURL := documentationURL(name, kind)

	var b strings.Builder

	fmt.Fprintf(
		&b,
		`<div class="ace-doc" data-docs-url="%s" data-function-name="%s" data-kind="%d">`,
		docURL,
		name,
		kind,
	)

	fmt.Fprintf(&b, `
		<div class="ace-doc-header clickable-header" title="Click to view documentation">
			<div class="ace-doc-signature">
				<strong>%s</strong>
				<span class="ace-status-%s">%s</span>
			</div>
		</div>`,
		signature,
		status,
		status,
	)

	if spec.Version != "" {
		fmt.Fprintf(&b, `<div class="ace-doc-version">Since: v%s</div>`, spec.Version)
	}

	writeParamsSection(&b, spec)

	b.WriteString(`</div>`)
	return b.String()
}

func writeParamsSection(b *strings.Builder, spec query.BaseSpec) {
	params := spec.Params
	if len(params.Definitions) == 0 {
		return
	}

	b.WriteString(`
		<div class="ace-doc-parameters">
			<strong>Parameters:</strong>`)

	for _, p := range params.Definitions {
		fmt.Fprintf(b, `
			<div class="ace-doc-param">
				<code>%s [%s]</code><br/>`,
			p.Name,
			getParamTypeString(p),
		)

		if p.Description != "" {
			desc := processMarkdownDescription(p.Description)
			fmt.Fprintf(b, `<span class="ace-doc-param-desc">%s</span>`, desc)
		}

		b.WriteString(`</div>`)
	}

	b.WriteString(`</div>`)
}

// buildSpecSignature creates the signature with parameters for functions and methods.
func buildSpecSignature(name string, spec query.BaseSpec, kind SpecKind) string {
	var b strings.Builder

	if kind == SpecMethod {
		b.WriteString(".")
	}

	b.WriteString(name)
	writeParamsSignature(&b, spec.Params)

	return b.String()
}

func writeParamsSignature(b *strings.Builder, params query.Params) {
	defs := params.Definitions

	if len(defs) == 0 && !params.Variadic {
		b.WriteString("()")
		return
	}

	b.WriteString("(")

	for i, p := range defs {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(b, "%s: %s", p.Name, getParamTypeString(p))
	}

	if params.Variadic {
		if len(defs) > 0 {
			b.WriteString(", ")
		}
		b.WriteString("...")
	}

	b.WriteString(")")
}

func getParamTypeString(param query.ParamDefinition) string {
	return string(param.ValueType)
}

// processMarkdownDescription processes markdown in descriptions
func processMarkdownDescription(description string) string {
	if description == "" {
		return ""
	}

	processed := admonitionRegex.ReplaceAllString(description, "")
	processed = inlineCodeRegex.ReplaceAllString(processed, "<code>$1</code>")
	processed = markdownLinkRegex.ReplaceAllString(processed, `<a href="$2" target="_blank">$1</a>`)

	return strings.TrimSpace(processed)
}

func documentationURL(name string, kind SpecKind) string {
	switch kind {
	case SpecMethod:
		return fmt.Sprintf("%s/methods#%s", docsBaseURL, name)
	default:
		return fmt.Sprintf("%s/functions#%s", docsBaseURL, name)
	}
}

// formatBloblang formats Bloblang code with indentation and consistent spacing
func formatBloblang(originalMapping string) (string, error) {
	lines := strings.Split(originalMapping, "\n")
	formatted := make([]string, 0, len(lines))
	indentLevel := 0

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Handle empty lines - only preserve if content exists
		if trimmed == "" {
			if len(formatted) > 0 {
				formatted = append(formatted, "")
			}
			continue
		}

		// Handle comments
		if strings.HasPrefix(trimmed, "#") {
			indent := strings.Repeat(" ", indentLevel*indentSize)
			formatted = append(formatted, indent+trimmed)
			continue
		}

		// Protect string literals before counting braces to avoid counting braces inside strings.
		protected, _ := protectStringLiterals(trimmed)

		if countClosingBraces(protected) > countOpeningBraces(protected) {
			indentLevel -= countClosingBraces(protected) - countOpeningBraces(protected)
			if indentLevel < 0 {
				indentLevel = 0
			}
		}

		indent := strings.Repeat(" ", indentLevel*indentSize)
		formatted = append(formatted, indent+formatLineContent(trimmed))

		if countOpeningBraces(protected) > countClosingBraces(protected) {
			indentLevel += countOpeningBraces(protected) - countClosingBraces(protected)
		}
	}

	// Trim trailing empty lines
	for len(formatted) > 0 && formatted[len(formatted)-1] == "" {
		formatted = formatted[:len(formatted)-1]
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
	return lambdaArrowRegex.ReplaceAllString(content, "$1 -> ")
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
	protected := stringLiteralRegex.ReplaceAllStringFunc(content, func(match string) string {
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
	content = lambdaExprRegex.ReplaceAllStringFunc(content, func(match string) string {
		formatted := formatLambdaOperators(match)
		formatted = formatLambdaExpressions(formatted)
		formatted = strings.TrimSpace(multipleSpacesRegex.ReplaceAllString(formatted, " "))
		lambdas = append(lambdas, formatted)
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
	content = logicalAndRegex.ReplaceAllString(content, " && ")
	content = logicalOrRegex.ReplaceAllString(content, " || ")
	content = matchArrowRegex.ReplaceAllString(content, " => ")
	content = equalityRegex.ReplaceAllString(content, " == ")
	content = inequalityRegex.ReplaceAllString(content, " != ")
	content = greaterEqualRegex.ReplaceAllString(content, " >= ")
	content = lessEqualRegex.ReplaceAllString(content, " <= ")
	content = pipeAssignRegex.ReplaceAllString(content, " |= ")
	content = greaterThanRegex.ReplaceAllString(content, " > ")
	content = lessThanRegex.ReplaceAllString(content, " < ")
	content = assignmentRegex.ReplaceAllString(content, " = ")
	content = addRegex.ReplaceAllString(content, " + ")
	content = subRegex.ReplaceAllString(content, " - ")
	content = mulRegex.ReplaceAllString(content, " * ")
	content = divRegex.ReplaceAllString(content, " / ")
	content = modRegex.ReplaceAllString(content, " % ")
	content = pipeRegex.ReplaceAllString(content, " | ")
	content = lambdaArrowOpRegex.ReplaceAllString(content, " -> ")
	content = matchArrowOpRegex.ReplaceAllString(content, " => ")
	return content
}

// formatFunctionCalls formats function calls and method chains
func formatFunctionCalls(content string) string {
	content = commaSpaceRegex.ReplaceAllString(content, ", ")
	content = spaceBeforeParenRegex.ReplaceAllString(content, "(")
	content = spaceAfterOpenParenRegex.ReplaceAllString(content, "(")
	content = spaceBeforeCloseRegex.ReplaceAllString(content, ")")
	content = dotSpaceRegex.ReplaceAllString(content, ".")
	return content
}

// formatNamedParameters handles named parameters in function calls
func formatNamedParameters(content string) string {
	content = namedParamColonRegex.ReplaceAllString(content, "$1: ")
	content = namedParamValueRegex.ReplaceAllString(content, "$1: $2")
	return content
}
