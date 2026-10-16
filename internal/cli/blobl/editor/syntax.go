package editor

import (
	"strings"

	"github.com/warpstreamlabs/bento/internal/bloblang/query"
)

// GenerateSyntax builds metadata for the ACE editor (autocompletion, syntax highlighting, tooltips).
func GenerateSyntax(walker Walker, r *Renderer) Syntax {
	var functionNames, methodNames []string
	functions := make(map[string]functionSpecWithHTML)
	methods := make(map[string]methodSpecWithHTML)

	walker.WalkFunctions(func(name string, spec query.FunctionSpec) {
		functions[name] = functionSpecWithHTML{
			FunctionSpec: spec,
			DocHTML:      r.Spec(functionDoc(name, spec)),
		}
		functionNames = append(functionNames, name)
	})

	walker.WalkMethods(func(name string, spec query.MethodSpec) {
		methods[name] = methodSpecWithHTML{
			MethodSpec: spec,
			DocHTML:    r.Spec(methodDoc(name, spec)),
		}
		methodNames = append(methodNames, name)
	})

	return Syntax{
		Functions: functions,
		Methods:   methods,
		Rules:     buildSyntaxHighlightingRules(functionNames, methodNames),
	}
}

// -----------------------------------------------------------------

type highlightRule struct {
	Token string `json:"token"`
	Regex string `json:"regex"` // JS-compatible, not Go regexp syntax
}

func buildSyntaxHighlightingRules(functionNames, methodNames []string) []highlightRule {
	return []highlightRule{
		{Token: "bloblang_root", Regex: `\broot\b`},                                                          // Matches: root
		{Token: "bloblang_this", Regex: `\bthis\b`},                                                          // Matches: this
		{Token: "support.function", Regex: `(?<![\.\w])(` + strings.Join(functionNames, "|") + `)(?=\s*\()`}, // Matches: uuid(), not .uuid()
		{Token: "support.method", Regex: `\.\s*(` + strings.Join(methodNames, "|") + `)(?=\s*\()`},           // Matches: .uppercase(), . uppercase()
	}
}
