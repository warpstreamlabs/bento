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

type BloblangSyntax struct {
	// For autocompletion
	Functions []string `json:"functions"`
	Methods   []string `json:"methods"`

	// For syntax highlighting (minimal rules that extend CoffeeScript)
	Rules []HighlightRule `json:"rules"`
}

func GenerateBloblangSyntax() ([]byte, error) {
	env := bloblang.GlobalEnvironment()

	var functions, methods []string
	env.WalkFunctions(func(name string, _ query.FunctionSpec) {
		functions = append(functions, name)
	})
	env.WalkMethods(func(name string, _ query.MethodSpec) {
		methods = append(methods, name)
	})

	// Bloblang-specific identifiers
	rules := []HighlightRule{
		// Matches `root`
		{Token: "bloblang_root", Regex: `\broot\b`},
		// Matches `this`
		{Token: "bloblang_this", Regex: `\bthis\b`},
		// Matches `.foo()`
		{Token: "support.method", Regex: `\.(` + strings.Join(methods, "|") + `)(?=\s*\()`},
		// Matches `bar()`
		{Token: "support.function", Regex: `(?<![\.\w])(` + strings.Join(functions, "|") + `)(?=\s*\()`},
	}

	syntax := BloblangSyntax{
		Functions: functions,
		Methods:   methods,
		Rules:     rules,
	}

	return json.Marshal(syntax)
}
