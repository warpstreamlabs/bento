package editor

import (
	"github.com/warpstreamlabs/bento/internal/bloblang/query"
)

// ExecutionResult represents the result of executing a Bloblang mapping.
type ExecutionResult struct {
	Result       any     `json:"result"`
	ParseError   *string `json:"parse_error"`
	MappingError *string `json:"mapping_error"`
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

// Syntax contains syntax metadata and highlighting rules.
type Syntax struct {
	// Rich function and method data with pre-generated documentation HTML
	Functions map[string]functionSpecWithHTML `json:"functions"`
	Methods   map[string]methodSpecWithHTML   `json:"methods"`

	// Minimal syntax highlighting rules that extends CoffeeScript
	Rules []highlightRule `json:"rules"`
}

// AutocompletionItem represents an autocompletion suggestion.
type AutocompletionItem struct {
	Caption string         `json:"caption"`           // Display name
	Value   string         `json:"value"`             // Insert value
	Snippet string         `json:"snippet"`           // Insert with cursor positioning
	Meta    string         `json:"meta"`              // Category/type description
	Type    string         `json:"type"`              // "function", "method", "keyword"
	Score   statusPriority `json:"score"`             // Priority score (higher = more important)
	DocHTML string         `json:"docHTML,omitempty"` // Documentation HTML, for keywords only
}

type statusPriority int

const (
	unknown statusPriority = iota
	experimental
	beta
	stable
)

// AutocompletionRequest represents a request for autocompletion.
type AutocompletionRequest struct {
	IsMethodContext bool `json:"isMethodContext"` // Whether cursor follows a dot (method context)
}
