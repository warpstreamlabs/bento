package syntax

import (
	"encoding/json"
	"strings"

	"github.com/warpstreamlabs/bento/internal/bloblang"
	"github.com/warpstreamlabs/bento/internal/bloblang/query"
)

// Token → semantic class used to apply styling.
// Regex → JavaScript-compatible regex pattern used to match code tokens.
type HighlightRule struct {
	Token string `json:"token"`
	Regex string `json:"regex"`
}

// BloblangSyntax contains everything needed for Bloblang syntax highlighting and autocompletion
type BloblangSyntax struct {
	// Rich function and method data with descriptions
	Functions map[string]query.FunctionSpec `json:"functions"`
	Methods   map[string]query.MethodSpec   `json:"methods"`

	// Minimal syntax highlighting rules that extend CoffeeScript
	Rules []HighlightRule `json:"rules"`

	// Quick lookup arrays for regex generation (internal use)
	FunctionNames []string `json:"function_names"`
	MethodNames   []string `json:"method_names"`
}

func GenerateBloblangSyntax() ([]byte, error) {
	env := bloblang.GlobalEnvironment()

	functions := make(map[string]query.FunctionSpec)
	methods := make(map[string]query.MethodSpec)
	var functionNames, methodNames []string

	env.WalkFunctions(func(name string, spec query.FunctionSpec) {
		functions[name] = spec
		functionNames = append(functionNames, name)
	})

	env.WalkMethods(func(name string, spec query.MethodSpec) {
		methods[name] = spec
		methodNames = append(methodNames, name)
	})

	rules := createHighlightRules(functionNames, methodNames)

	syntax := BloblangSyntax{
		Functions:     functions,
		Methods:       methods,
		Rules:         rules,
		FunctionNames: functionNames,
		MethodNames:   methodNames,
	}

	return json.Marshal(syntax)
}

func createHighlightRules(functionNames, methodNames []string) []HighlightRule {
	// Generate syntax highlighting rules
	rules := []HighlightRule{
		// Matches `root` (highest priority)
		{Token: "bloblang_root", Regex: `\broot\b`},
		// Matches `this` (highest priority)
		{Token: "bloblang_this", Regex: `\bthis\b`},
		// Matches `foo()`
		{Token: "support.function", Regex: `(?<![\.\w])(` + strings.Join(functionNames, "|") + `)(?=\s*\()`},
		// Matches `.bar()`
		{Token: "support.method", Regex: `\.(` + strings.Join(methodNames, "|") + `)(?=\s*\()`},
	}

	return rules
}
