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
			result := executeBloblangMapping(env, tt.input, tt.mapping)

			hasError := result.ParseError != nil || result.MappingError != nil
			if hasError != tt.expectError {
				t.Errorf("executeBloblangMapping() error = %v, expectError = %v", hasError, tt.expectError)
			}

			hasResult := result.Result != nil
			if hasResult != tt.expectResult {
				t.Errorf("executeBloblangMapping() hasResult = %v, expectResult = %v", hasResult, tt.expectResult)
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
			result := validateBloblangMapping(env, tt.mapping)
			if result.Valid != tt.valid {
				t.Errorf("validateBloblangMapping() = %v, want %v (error: %s)", result.Valid, tt.valid, result.Error)
			}
		})
	}
}

// Test syntax (Syntax metadata generation)
func TestGenerateBloblangSyntax(t *testing.T) {
	env := bloblang.GlobalEnvironment()

	syntax, err := generateBloblangSyntax(env)
	if err != nil {
		t.Fatalf("generateBloblangSyntax() error = %v", err)
	}

	// Check functions exist
	if len(syntax.Functions) == 0 {
		t.Error("generateBloblangSyntax() returned no functions")
	}

	// Check methods exist
	if len(syntax.Methods) == 0 {
		t.Error("generateBloblangSyntax() returned no methods")
	}

	// Check highlighting rules generated
	if len(syntax.Rules) == 0 {
		t.Error("generateBloblangSyntax() returned no syntax rules")
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
		t.Error("generateBloblangSyntax() did not pre-generate DocHTML for functions")
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
		t.Error("generateBloblangSyntax() did not pre-generate DocHTML for methods")
	}
}

// Test format (Bloblang formatting)
func TestFormatBloblangMapping(t *testing.T) {
	tests := []struct {
		name        string
		mapping     string
		expectError bool
	}{
		{
			name:        "valid mapping",
			mapping:     `root.name = "test"`,
			expectError: false,
		},
		{
			name:        "mapping with this",
			mapping:     `root = this`,
			expectError: false,
		},
		{
			name:        "empty mapping",
			mapping:     "",
			expectError: true,
		},
		{
			name:        "invalid syntax",
			mapping:     `root.bad =`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatBloblangMapping(bloblang.GlobalEnvironment(), tt.mapping)

			if result.Success == tt.expectError {
				t.Errorf("formatBloblangMapping() success = %v, expectError = %v", result.Success, tt.expectError)
			}

			if !tt.expectError && result.Formatted == "" {
				t.Error("formatBloblangMapping() returned empty formatted result")
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
			result := generateAutocompletion(env, tt.request)

			if result.Success == tt.expectError {
				t.Errorf("generateAutocompletion() success = %v, expectError = %v", result.Success, tt.expectError)
			}

			if tt.expectCompletions {
				if len(result.Completions) < tt.minCompletions {
					t.Errorf("Expected at least %d completions, got %d", tt.minCompletions, len(result.Completions))
				}

				// Verify completion structure
				for _, comp := range result.Completions {
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
					// Either Value or Snippet should be set
					if comp.Value == "" && comp.Snippet == "" {
						t.Error("Completion missing both Value and Snippet")
					}
				}

				// Verify expected types are present
				hasKeyword := false
				hasFunction := false
				hasMethod := false

				for _, comp := range result.Completions {
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
				if len(result.Completions) > 0 {
					t.Errorf("Expected no completions, got %d", len(result.Completions))
				}
			}
		})
	}
}
