package editor

import (
	"strings"
	"testing"

	"github.com/warpstreamlabs/bento/internal/bloblang/query"
)

var testRenderer = NewRenderer("https://example.com/docs/guides/bloblang")

type testWalker struct {
	functions []query.FunctionSpec
	methods   []query.MethodSpec
}

func (w *testWalker) WalkFunctions(fn func(string, query.FunctionSpec)) {
	for _, spec := range w.functions {
		fn(spec.Name, spec)
	}
}

func (w *testWalker) WalkMethods(fn func(string, query.MethodSpec)) {
	for _, spec := range w.methods {
		fn(spec.Name, spec)
	}
}

func checkDocHTML(t *testing.T, docHTML string, want, notWant []string) {
	t.Helper()
	for _, w := range want {
		if !strings.Contains(docHTML, w) {
			t.Errorf("DocHTML missing %q\ngot:\n%s", w, docHTML)
		}
	}
	for _, w := range notWant {
		if strings.Contains(docHTML, w) {
			t.Errorf("DocHTML must not contain %q\ngot:\n%s", w, docHTML)
		}
	}
}

func TestGenerateSyntax_Empty(t *testing.T) {
	syntax := GenerateSyntax(&testWalker{}, testRenderer)
	if len(syntax.Functions) != 0 {
		t.Errorf("expected 0 functions, got %d", len(syntax.Functions))
	}
	if len(syntax.Methods) != 0 {
		t.Errorf("expected 0 methods, got %d", len(syntax.Methods))
	}
	if syntax.Rules == nil {
		t.Error("Rules must be non-nil even for empty environment")
	}
}

func TestGenerateSyntax_FunctionDocHTML(t *testing.T) {
	tests := []struct {
		name    string
		spec    query.FunctionSpec
		want    []string
		notWant []string
	}{
		{
			name: "stable function no params",
			spec: query.NewFunctionSpec(query.FunctionCategoryGeneral, "test_fn", "A test function."),
			want: []string{
				`data-function-name="test_fn"`,
				`data-docs-url="https://example.com/docs/guides/bloblang/functions#test_fn"`,
				`<strong>test_fn()</strong>`,
				`ace-status-stable`,
			},
			notWant: []string{`methods#test_fn`},
		},
		{
			name: "function with params renders signature and param list",
			spec: query.NewFunctionSpec(query.FunctionCategoryGeneral, "range_fn", "Generates a range.").
				Param(query.ParamInt64("count", "Number of elements")),
			want: []string{
				`<strong>range_fn(count: integer)</strong>`,
				`ace-doc-parameters`,
				`count [integer]`,
				`Number of elements`,
			},
		},
		{
			name:    "experimental function reflects status",
			spec:    query.NewFunctionSpec(query.FunctionCategoryGeneral, "exp_fn", "An experimental function.").Experimental(),
			want:    []string{`ace-status-experimental`},
			notWant: []string{`ace-status-stable`},
		},
		{
			name: "beta function reflects status",
			spec: query.NewFunctionSpec(query.FunctionCategoryGeneral, "beta_fn", "A beta function.").Beta(),
			want: []string{`ace-status-beta`},
		},
		{
			name: "function with version renders Since line",
			spec: query.NewFunctionSpec(query.FunctionCategoryGeneral, "new_fn", "A newer function.").AtVersion("1.2.0"),
			want: []string{`Since: v1.2.0`},
		},
		{
			name:    "hidden function without description",
			spec:    query.NewHiddenFunctionSpec("hidden_fn"),
			want:    []string{`data-function-name="hidden_fn"`},
			notWant: []string{`ace-doc-description`, `ace-doc-parameters`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			syntax := GenerateSyntax(&testWalker{functions: []query.FunctionSpec{tt.spec}}, testRenderer)
			spec, ok := syntax.Functions[tt.spec.Name]
			if !ok {
				t.Fatalf("function %q not found in output", tt.spec.Name)
			}
			checkDocHTML(t, spec.DocHTML, tt.want, tt.notWant)
		})
	}
}

func TestGenerateSyntax_MethodDocHTML(t *testing.T) {
	tests := []struct {
		name    string
		spec    query.MethodSpec
		want    []string
		notWant []string
	}{
		{
			name: "method signature has leading dot",
			spec: query.NewMethodSpec("test_method", "A test method.").InCategory(query.MethodCategoryStrings, ""),
			want: []string{
				`<strong>.test_method()</strong>`,
				`data-docs-url="https://example.com/docs/guides/bloblang/methods#test_method"`,
			},
			notWant: []string{`functions#test_method`},
		},
		{
			name: "method with param renders param in signature and list",
			spec: query.NewMethodSpec("repeat", "Repeats a string.").
				Param(query.ParamInt64("count", "Number of repetitions")),
			want: []string{
				`<strong>.repeat(count: integer)</strong>`,
				`count [integer]`,
				`ace-doc-parameters`,
			},
		},
		{
			name: "variadic method",
			spec: query.NewMethodSpec("join_all", "Joins things.").VariadicParams(),
			want: []string{`<strong>.join_all(...)</strong>`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			syntax := GenerateSyntax(&testWalker{methods: []query.MethodSpec{tt.spec}}, testRenderer)
			spec, ok := syntax.Methods[tt.spec.Name]
			if !ok {
				t.Fatalf("method %q not found in output", tt.spec.Name)
			}
			checkDocHTML(t, spec.DocHTML, tt.want, tt.notWant)
		})
	}
}

func TestGenerateSyntax_HighlightingRules(t *testing.T) {
	walker := &testWalker{
		functions: []query.FunctionSpec{query.NewFunctionSpec(query.FunctionCategoryGeneral, "uuid", "")},
		methods:   []query.MethodSpec{query.NewMethodSpec("uppercase", "")},
	}
	byToken := map[string]string{}
	for _, rule := range GenerateSyntax(walker, testRenderer).Rules {
		byToken[rule.Token] = rule.Regex
	}

	if r := byToken["support.function"]; !strings.Contains(r, "uuid") {
		t.Errorf("support.function rule missing uuid: %q", r)
	}
	if r := byToken["support.method"]; !strings.Contains(r, "uppercase") {
		t.Errorf("support.method rule missing uppercase: %q", r)
	}
}
