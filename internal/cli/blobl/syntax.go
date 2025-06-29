package blobl

import (
	"strings"

	"github.com/warpstreamlabs/bento/internal/bloblang/query"
)

// Token → semantic class used to apply styling.
// Regex → JavaScript-compatible regex pattern used to match code tokens.
type highlightRule struct {
	Token string `json:"token"`
	Regex string `json:"regex"`
}

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

type Walker interface {
	WalkFunctions(fn func(name string, spec query.FunctionSpec))
	WalkMethods(fn func(name string, spec query.MethodSpec))
}

func GenerateBloblangSyntax(env Walker) (bloblangSyntax, error) {
	var functionNames, methodNames []string
	functions := make(map[string]query.FunctionSpec)
	methods := make(map[string]query.MethodSpec)

	env.WalkFunctions(func(name string, spec query.FunctionSpec) {
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
