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
