package blobl

import (
	"strings"
	"testing"

	"github.com/warpstreamlabs/bento/internal/bloblang"
)

// Test execute (Bloblang mapping execution)
func TestExecuteMapping(t *testing.T) {
	env := bloblang.GlobalEnvironment()

	tests := []struct {
		name         string
		input        string
		mapping      string
		expectError  bool
		expectResult bool
	}{
		{
			name:         "simple mapping",
			input:        `{"name":"Alice"}`,
			mapping:      `root.name = this.name`,
			expectError:  false,
			expectResult: true,
		},
		{
			name:         "empty input",
			input:        "",
			mapping:      `root = this`,
			expectError:  true,
			expectResult: false,
		},
		{
			name:         "empty mapping",
			input:        `{"test":true}`,
			mapping:      "",
			expectError:  true,
			expectResult: false,
		},
		{
			name:         "invalid mapping syntax",
			input:        `{"test":true}`,
			mapping:      `root.bad =`,
			expectError:  true,
			expectResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExecuteBloblangMapping(env, tt.input, tt.mapping)

			hasError := result.ParseError != nil || result.MappingError != nil
			if hasError != tt.expectError {
				t.Errorf("ExecuteBloblangMapping() error = %v, expectError = %v", hasError, tt.expectError)
			}

			hasResult := result.Result != nil
			if hasResult != tt.expectResult {
				t.Errorf("ExecuteBloblangMapping() hasResult = %v, expectResult = %v", hasResult, tt.expectResult)
			}
		})
	}
}

// Test validate (Bloblang validation)
func TestValidateBloblangMapping(t *testing.T) {
	env := bloblang.GlobalEnvironment()

	tests := []struct {
		name    string
		mapping string
		valid   bool
	}{
		{
			name:    "simple valid mapping",
			mapping: `root.name = "test"`,
			valid:   true,
		},
		{
			name:    "valid with this",
			mapping: `root.name = this.user.name`,
			valid:   true,
		},
		{
			name:    "valid with function",
			mapping: `root = this.uppercase()`,
			valid:   true,
		},
		{
			name:    "empty mapping",
			mapping: "",
			valid:   false,
		},
		{
			name:    "invalid syntax",
			mapping: `root.bad =`,
			valid:   false,
		},
		{
			name:    "unknown function",
			mapping: `root = fake_function()`,
			valid:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid, err := ValidateBloblangMapping(env, tt.mapping)

			if valid != tt.valid {
				t.Errorf(
					"ValidateBloblangMapping() valid = %v, want %v (err=%v)",
					valid,
					tt.valid,
					err,
				)
			}

			if tt.valid && err != nil {
				t.Errorf(
					"ValidateBloblangMapping() unexpected error: %v",
					err,
				)
			}

			if !tt.valid && err == nil {
				t.Errorf(
					"ValidateBloblangMapping() expected error, got nil",
				)
			}
		})
	}
}

// Test syntax (Syntax metadata generation)
func TestGenerateBloblangSyntax(t *testing.T) {
	env := bloblang.GlobalEnvironment()

	syntax, err := GenerateBloblangSyntax(env)
	if err != nil {
		t.Fatalf("GenerateBloblangSyntax() error = %v", err)
	}

	// Check functions exist
	if len(syntax.Functions) == 0 {
		t.Error("GenerateBloblangSyntax() returned no functions")
	}

	// Check methods exist
	if len(syntax.Methods) == 0 {
		t.Error("GenerateBloblangSyntax() returned no methods")
	}

	// Check highlighting rules generated
	if len(syntax.Rules) == 0 {
		t.Error("GenerateBloblangSyntax() returned no syntax rules")
	}

	// Check that DocHTML is pre-generated for functions
	foundDocHTML := false
	for _, fn := range syntax.Functions {
		if fn.DocHTML != "" {
			foundDocHTML = true
			// Verify it contains expected HTML tags
			if !strings.Contains(fn.DocHTML, "ace-doc") {
				t.Error("Function DocHTML missing expected ace-doc class")
			}
			break
		}
	}
	if !foundDocHTML {
		t.Error("GenerateBloblangSyntax() did not pre-generate DocHTML for functions")
	}

	// Check that DocHTML is pre-generated for methods
	foundMethodDocHTML := false
	for _, method := range syntax.Methods {
		if method.DocHTML != "" {
			foundMethodDocHTML = true
			if !strings.Contains(method.DocHTML, "ace-doc") {
				t.Error("Method DocHTML missing expected ace-doc class")
			}
			break
		}
	}
	if !foundMethodDocHTML {
		t.Error("GenerateBloblangSyntax() did not pre-generate DocHTML for methods")
	}
}

// Test format (Bloblang formatting)
func TestFormatBloblangMapping(t *testing.T) {
	tests := []struct {
		name        string
		mapping     string
		expected    string
		expectError bool
	}{
		{
			name:     "valid mapping",
			mapping:  `root.name = "test"`,
			expected: `root.name = "test"`,
		},
		{
			name:     "mapping with this",
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
			name: "method call spacing",
			mapping: `
				root.stars = "★". repeat((this.stars / 100) )
			`,
			expected: `root.stars = "★".repeat((this.stars / 100))`,
		},
		{
			name: "no formatting inside strings",
			mapping: `
				root.msg = "this . should not . change"
			`,
			expected: `root.msg = "this . should not . change"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			formatted, err := FormatBloblangMapping(
				bloblang.GlobalEnvironment(),
				tt.mapping,
			)

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
				t.Errorf(
					"formatted output mismatch\nexpected:\n%s\n\ngot:\n%s",
					tt.expected,
					formatted,
				)
			}
		})
	}
}

// Test autocomplete (Autocompletion)
func TestGenerateAutocompletion(t *testing.T) {
	env := bloblang.GlobalEnvironment()

	tests := []struct {
		name              string
		request           AutocompletionRequest
		expectError       bool
		expectCompletions bool
		expectKeywords    bool
		expectFunctions   bool
		expectMethods     bool
		minCompletions    int
	}{
		{
			name: "function context - should return functions and keywords",
			request: AutocompletionRequest{
				Line:         "root = ",
				Column:       7,
				BeforeCursor: "root = ",
			},
			expectError:       false,
			expectCompletions: true,
			expectKeywords:    false, // Keywords only show when typing keyword prefix
			expectFunctions:   true,
			expectMethods:     false,
			minCompletions:    10, // Should have many function completions
		},
		{
			name: "method context - should return methods only",
			request: AutocompletionRequest{
				Line:         "root = this.u",
				Column:       13,
				BeforeCursor: "root = this.u",
			},
			expectError:       false,
			expectCompletions: true,
			expectKeywords:    false,
			expectFunctions:   false,
			expectMethods:     true,
			minCompletions:    5, // Should have method completions (uppercase, etc.)
		},
		{
			name: "keyword context - should return keywords",
			request: AutocompletionRequest{
				Line:         "ro",
				Column:       2,
				BeforeCursor: "ro",
			},
			expectError:       false,
			expectCompletions: true,
			expectKeywords:    true,
			expectFunctions:   true,
			expectMethods:     false,
			minCompletions:    1,
		},
		{
			name: "invalid column - should fail",
			request: AutocompletionRequest{
				Line:         "root = this",
				Column:       -1,
				BeforeCursor: "",
			},
			expectError:       true,
			expectCompletions: false,
		},
		{
			name: "inside comment - should return empty",
			request: AutocompletionRequest{
				Line:         "# comment",
				Column:       5,
				BeforeCursor: "# com",
			},
			expectError:       false,
			expectCompletions: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			completions, err := GenerateAutocompletion(env, tt.request)

			if tt.expectError && err == nil {
				t.Errorf("GenerateAutocompletion() expected error, got nil")
			}

			if !tt.expectError && err != nil {
				t.Errorf("GenerateAutocompletion() unexpected error: %v", err)
			}

			if tt.expectCompletions {
				if len(completions) < tt.minCompletions {
					t.Errorf(
						"Expected at least %d completions, got %d",
						tt.minCompletions,
						len(completions),
					)
				}

				for _, comp := range completions {
					if comp.Caption == "" {
						t.Error("Completion missing Caption")
					}
					if comp.Type == "" {
						t.Error("Completion missing Type")
					}
					if comp.DocHTML == "" {
						t.Error("Completion missing DocHTML")
					}
					if comp.Meta == "" {
						t.Error("Completion missing Meta")
					}
					if comp.Value == "" && comp.Snippet == "" {
						t.Error("Completion missing both Value and Snippet")
					}
				}

				hasKeyword := false
				hasFunction := false
				hasMethod := false

				for _, comp := range completions {
					switch comp.Type {
					case "keyword":
						hasKeyword = true
					case "function":
						hasFunction = true
					case "method":
						hasMethod = true
					}
				}

				if tt.expectKeywords && !hasKeyword {
					t.Error("Expected keyword completions but found none")
				}
				if tt.expectFunctions && !hasFunction {
					t.Error("Expected function completions but found none")
				}
				if tt.expectMethods && !hasMethod {
					t.Error("Expected method completions but found none")
				}
			} else {
				if len(completions) > 0 {
					t.Errorf(
						"Expected no completions, got %d",
						len(completions),
					)
				}
			}
		})
	}
}
