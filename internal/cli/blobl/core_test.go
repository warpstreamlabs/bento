package blobl

import (
	"strings"
	"testing"

	"github.com/warpstreamlabs/bento/internal/bloblang"
	"github.com/warpstreamlabs/bento/internal/bloblang/query"
)

// testWalker implements Walker for tests that only care
// about metadata/HTML generation, not execution.
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

// --- GenerateBloblangSyntax ---

func TestGenerateBloblangSyntax_Empty(t *testing.T) {
	syntax, err := GenerateBloblangSyntax(&testWalker{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
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

func TestGenerateBloblangSyntax_FunctionDocHTML(t *testing.T) {
	tests := []struct {
		name             string
		spec             query.FunctionSpec
		wantInDocHTML    []string
		wantNotInDocHTML []string
	}{
		{
			name: "stable function no params",
			spec: query.NewFunctionSpec(
				query.FunctionCategoryGeneral,
				"test_fn",
				"A test function.",
			),
			wantInDocHTML: []string{
				`data-function-name="test_fn"`,
				`data-kind="0"`,              // SpecFunction == 0
				`functions#test_fn`,          // function URL anchor
				`<strong>test_fn()</strong>`, // no params → ()
				`ace-status-stable`,
			},
			wantNotInDocHTML: []string{
				`methods#test_fn`, // must not use method URL
			},
		},
		{
			name: "function with params renders signature and param list",
			spec: query.NewFunctionSpec(
				query.FunctionCategoryGeneral,
				"range_fn",
				"Generates a range.",
			).Param(query.ParamInt64("count", "Number of elements")),
			wantInDocHTML: []string{
				`<strong>range_fn(count: integer)</strong>`,
				`ace-doc-parameters`,
				`count [integer]`,
			},
		},
		{
			name: "experimental function reflects status",
			spec: query.NewFunctionSpec(
				query.FunctionCategoryGeneral,
				"exp_fn",
				"An experimental function.",
			).Experimental(),
			wantInDocHTML: []string{
				`ace-status-experimental`,
			},
			wantNotInDocHTML: []string{
				`ace-status-stable`,
			},
		},
		{
			name: "beta function reflects status",
			spec: query.NewFunctionSpec(
				query.FunctionCategoryGeneral,
				"beta_fn",
				"A beta function.",
			).Beta(),
			wantInDocHTML: []string{
				`ace-status-beta`,
			},
		},
		{
			name: "function with version renders Since line",
			spec: query.NewFunctionSpec(
				query.FunctionCategoryGeneral,
				"new_fn",
				"A newer function.",
			).AtVersion("1.2.0"),
			wantInDocHTML: []string{
				`Since: v1.2.0`,
			},
		},
		{
			name: "hidden function produces valid HTML",
			spec: query.NewHiddenFunctionSpec("hidden_fn"),
			wantInDocHTML: []string{
				`data-function-name="hidden_fn"`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			walker := &testWalker{functions: []query.FunctionSpec{tt.spec}}
			syntax, err := GenerateBloblangSyntax(walker)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			spec, ok := syntax.Functions[tt.spec.Name]
			if !ok {
				t.Fatalf("function %q not found in output", tt.spec.Name)
			}
			if spec.DocHTML == "" {
				t.Fatal("DocHTML must not be empty")
			}

			for _, want := range tt.wantInDocHTML {
				if !strings.Contains(spec.DocHTML, want) {
					t.Errorf("DocHTML missing %q\ngot:\n%s", want, spec.DocHTML)
				}
			}
			for _, notWant := range tt.wantNotInDocHTML {
				if strings.Contains(spec.DocHTML, notWant) {
					t.Errorf("DocHTML must not contain %q\ngot:\n%s", notWant, spec.DocHTML)
				}
			}
		})
	}
}

func TestGenerateBloblangSyntax_MethodDocHTML(t *testing.T) {
	tests := []struct {
		name             string
		spec             query.MethodSpec
		wantInDocHTML    []string
		wantNotInDocHTML []string
	}{
		{
			name: "method signature has leading dot",
			spec: query.NewMethodSpec("test_method", "A test method.").
				InCategory(query.MethodCategoryStrings, ""),
			wantInDocHTML: []string{
				`<strong>.test_method()</strong>`, // leading dot distinguishes method from function
				`data-kind="1"`,                   // SpecMethod == 1
				`methods#test_method`,             // method URL, not functions URL
			},
			wantNotInDocHTML: []string{
				`functions#test_method`,
			},
		},
		{
			name: "method with param renders param in signature and list",
			spec: query.NewMethodSpec("repeat", "Repeats a string.").
				Param(query.ParamInt64("count", "Number of repetitions")),
			wantInDocHTML: []string{
				`<strong>.repeat(count: integer)</strong>`,
				`count [integer]`,
				`ace-doc-parameters`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			walker := &testWalker{methods: []query.MethodSpec{tt.spec}}
			syntax, err := GenerateBloblangSyntax(walker)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			spec, ok := syntax.Methods[tt.spec.Name]
			if !ok {
				t.Fatalf("method %q not found in output", tt.spec.Name)
			}
			if spec.DocHTML == "" {
				t.Fatal("DocHTML must not be empty")
			}

			for _, want := range tt.wantInDocHTML {
				if !strings.Contains(spec.DocHTML, want) {
					t.Errorf("DocHTML missing %q\ngot:\n%s", want, spec.DocHTML)
				}
			}
			for _, notWant := range tt.wantNotInDocHTML {
				if strings.Contains(spec.DocHTML, notWant) {
					t.Errorf("DocHTML must not contain %q\ngot:\n%s", notWant, spec.DocHTML)
				}
			}
		})
	}
}

func TestGenerateBloblangSyntax_HighlightingRules(t *testing.T) {
	walker := &testWalker{
		functions: []query.FunctionSpec{
			query.NewFunctionSpec(query.FunctionCategoryGeneral, "uuid", ""),
		},
		methods: []query.MethodSpec{
			query.NewMethodSpec("uppercase", ""),
		},
	}

	syntax, err := GenerateBloblangSyntax(walker)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	byToken := map[string]string{}
	for _, rule := range syntax.Rules {
		byToken[rule.Token] = rule.Regex
	}

	if r, ok := byToken["support.function"]; !ok {
		t.Error("missing support.function rule")
	} else if !strings.Contains(r, "uuid") {
		t.Errorf("support.function rule missing uuid: %q", r)
	}

	if r, ok := byToken["support.method"]; !ok {
		t.Error("missing support.method rule")
	} else if !strings.Contains(r, "uppercase") {
		t.Errorf("support.method rule missing uppercase: %q", r)
	}
}

func TestGenerateBloblangSyntax_NameLists(t *testing.T) {
	walker := &testWalker{
		functions: []query.FunctionSpec{
			query.NewFunctionSpec(query.FunctionCategoryGeneral, "alpha", ""),
			query.NewFunctionSpec(query.FunctionCategoryGeneral, "beta", ""),
		},
		methods: []query.MethodSpec{
			query.NewMethodSpec("gamma", ""),
		},
	}

	syntax, err := GenerateBloblangSyntax(walker)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(syntax.FunctionNames) != 2 {
		t.Errorf("expected 2 function names, got %d: %v", len(syntax.FunctionNames), syntax.FunctionNames)
	}
	if len(syntax.MethodNames) != 1 {
		t.Errorf("expected 1 method name, got %d: %v", len(syntax.MethodNames), syntax.MethodNames)
	}
	if len(syntax.Functions) != 2 {
		t.Errorf("expected 2 entries in Functions map, got %d", len(syntax.Functions))
	}
	if len(syntax.Methods) != 1 {
		t.Errorf("expected 1 entry in Methods map, got %d", len(syntax.Methods))
	}
}

// --- ExecuteBloblangMapping ---

func TestExecuteMapping(t *testing.T) {
	env := bloblang.GlobalEnvironment()

	tests := []struct {
		name         string
		input        string
		mapping      string
		wantResult   bool
		wantParseErr bool
		wantExecErr  bool
	}{
		{
			name:       "simple field mapping",
			input:      `{"name":"Alice"}`,
			mapping:    `root.name = this.name`,
			wantResult: true,
		},
		{
			name:        "empty input",
			input:       "",
			mapping:     `root = this`,
			wantExecErr: true,
		},
		{
			name:         "empty mapping",
			input:        `{"test":true}`,
			mapping:      "",
			wantParseErr: true,
		},
		{
			name:         "invalid mapping syntax",
			input:        `{"test":true}`,
			mapping:      `root.bad =`,
			wantParseErr: true,
		},
		{
			name:       "pass-through",
			input:      `{"x":1}`,
			mapping:    `root = this`,
			wantResult: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExecuteBloblangMapping(env, tt.input, tt.mapping)

			if tt.wantParseErr && result.ParseError == nil {
				t.Error("expected ParseError, got nil")
			}
			if !tt.wantParseErr && result.ParseError != nil {
				t.Errorf("unexpected ParseError: %v", result.ParseError)
			}
			if tt.wantExecErr && result.MappingError == nil {
				t.Error("expected MappingError, got nil")
			}
			if tt.wantResult && result.Result == nil {
				t.Error("expected non-nil Result")
			}
		})
	}
}

// --- FormatBloblangMapping ---

func TestFormatBloblangMapping(t *testing.T) {
	env := bloblang.GlobalEnvironment()

	tests := []struct {
		name        string
		mapping     string
		expected    string
		expectError bool
	}{
		{
			name:     "literal assignment",
			mapping:  `root.name = "test"`,
			expected: `root.name = "test"`,
		},
		{
			name:     "pass-through",
			mapping:  `root = this`,
			expected: `root = this`,
		},
		{
			name:        "empty mapping",
			mapping:     ``,
			expectError: true,
		},
		{
			name:        "invalid syntax",
			mapping:     `root.bad =`,
			expectError: true,
		},
		{
			name: "method chain spacing",
			mapping: `root.about = "%s 🍱 is a %s %s".format(
				this.name.capitalize(),
				this.features.join(" & "),
				this.type. split("_").join(" ")
			)`,
			expected: `root.about = "%s 🍱 is a %s %s".format(
  this.name.capitalize(),
  this.features.join(" & "),
  this.type.split("_").join(" ")
)`,
		},
		{
			name: "extra space before method call collapsed",
			mapping: `
				root.stars = "★". repeat((this.stars / 100) )
			`,
			expected: `root.stars = "★".repeat((this.stars / 100))`,
		},
		{
			name: "dot inside string not formatted",
			mapping: `
				root.msg = "this . should not . change"
			`,
			expected: `root.msg = "this . should not . change"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			formatted, err := FormatBloblangMapping(env, tt.mapping)

			if tt.expectError {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if formatted != tt.expected {
				t.Errorf("mismatch\nwant:\n%s\n\ngot:\n%s", tt.expected, formatted)
			}
		})
	}
}

// --- GenerateAutocompletion ---

func TestGenerateAutocompletion(t *testing.T) {
	env := bloblang.GlobalEnvironment()
	syntax, err := GenerateBloblangSyntax(env)
	if err != nil {
		t.Fatalf("failed to generate syntax: %v", err)
	}

	tests := []struct {
		name           string
		req            AutocompletionRequest
		wantErr        bool
		wantEmpty      bool
		wantTypes      []string
		minCompletions int
	}{
		{
			name:           "function context returns functions",
			req:            AutocompletionRequest{Line: "root = ", Column: 7, BeforeCursor: "root = "},
			wantTypes:      []string{"function"},
			minCompletions: 10,
		},
		{
			name:           "method context returns methods",
			req:            AutocompletionRequest{Line: "root = this.u", Column: 13, BeforeCursor: "root = this.u"},
			wantTypes:      []string{"method"},
			minCompletions: 5,
		},
		{
			name:      "inside comment returns empty",
			req:       AutocompletionRequest{Line: "# comment", Column: 5, BeforeCursor: "# com"},
			wantEmpty: true,
		},
		{
			name:    "negative column is error",
			req:     AutocompletionRequest{Line: "root = this", Column: -1},
			wantErr: true,
		},
		{
			name:    "column beyond line length is error",
			req:     AutocompletionRequest{Line: "root", Column: 100, BeforeCursor: "root"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			completions, err := GenerateAutocompletion(&syntax, tt.req)

			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.wantEmpty {
				if len(completions) > 0 {
					t.Errorf("expected no completions, got %d", len(completions))
				}
				return
			}

			if len(completions) < tt.minCompletions {
				t.Errorf("expected >= %d completions, got %d", tt.minCompletions, len(completions))
			}

			for i, c := range completions {
				if c.Caption == "" {
					t.Errorf("completion[%d] missing Caption", i)
				}
				if c.Type == "" {
					t.Errorf("completion[%d] missing Type", i)
				}
				if c.DocHTML == "" {
					t.Errorf("completion[%d] missing DocHTML", i)
				}
				if c.Value == "" && c.Snippet == "" {
					t.Errorf("completion[%d] missing both Value and Snippet", i)
				}
			}

			typeSet := map[string]bool{}
			for _, c := range completions {
				typeSet[c.Type] = true
			}
			for _, want := range tt.wantTypes {
				if !typeSet[want] {
					t.Errorf("expected completion type %q but none found in %v", want, typeSet)
				}
			}
		})
	}
}
