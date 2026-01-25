package blobl

import (
	"github.com/warpstreamlabs/bento/internal/bloblang"
	"github.com/warpstreamlabs/bento/internal/bloblang/query"
	"github.com/warpstreamlabs/bento/internal/message"
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

// completionCache holds cached syntax data to avoid repeated environment walks.
type completionCache struct {
	syntax *bloblangSyntax
	env    *bloblang.Environment
}

// highlightRule represents a token type and regex pattern for syntax highlighting.
// Token is a semantic class used to apply styling.
// Regex is a JS-compatible regex pattern used to match code tokens.
type highlightRule struct {
	Token string `json:"token"`
	Regex string `json:"regex"`
}

// functionSpecWithHTML extends FunctionSpec with pre-generated HTML documentation.
type functionSpecWithHTML struct {
	query.FunctionSpec
	DocHTML string `json:"docHTML"`
}

// methodSpecWithHTML extends MethodSpec with pre-generated HTML documentation.
type methodSpecWithHTML struct {
	query.MethodSpec
	DocHTML string `json:"docHTML"`
}

// bloblangSyntax contains syntax metadata and highlighting rules.
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

// CompletionItem represents an autocompletion suggestion.
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

// AutocompletionRequest represents a request for autocompletion.
type AutocompletionRequest struct {
	Line         string `json:"line"`         // The current line text
	Column       int    `json:"column"`       // Cursor column position
	BeforeCursor string `json:"beforeCursor"` // Text before cursor position
}
