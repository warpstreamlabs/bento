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
		name        string
		request     AutocompletionRequest
		expectError bool
	}{
		{
			name: "function context",
			request: AutocompletionRequest{
				Line:         "root = ",
				Column:       7,
				BeforeCursor: "root = ",
			},
			expectError: false,
		},
		{
			name: "method context with dot",
			request: AutocompletionRequest{
				Line:         "root = this.u",
				Column:       13,
				BeforeCursor: "root = this.u",
			},
			expectError: false,
		},
		{
			name: "invalid column",
			request: AutocompletionRequest{
				Line:         "root = this",
				Column:       -1,
				BeforeCursor: "",
			},
			expectError: true,
		},
		{
			name: "inside comment",
			request: AutocompletionRequest{
				Line:         "# comment",
				Column:       5,
				BeforeCursor: "# com",
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := generateAutocompletion(env, tt.request)

			if result.Success == tt.expectError {
				t.Errorf("generateAutocompletion() success = %v, expectError = %v", result.Success, tt.expectError)
			}

			if !tt.expectError && len(result.Completions) > 0 {
				// Check that completions have DocHTML when present
				for _, comp := range result.Completions {
					if comp.DocHTML == "" {
						t.Error("Completion missing DocHTML field")
					}
				}
			}
		})
	}
}

// Test formatJSON (JSON formatting)
func TestFormatJSON(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantError bool
	}{
		{
			name:      "compact JSON",
			input:     `{"name":"Alice","age":30}`,
			wantError: false,
		},
		{
			name:      "already formatted",
			input:     "{\n  \"name\": \"Alice\"\n}",
			wantError: false,
		},
		{
			name:      "empty string",
			input:     "",
			wantError: true,
		},
		{
			name:      "invalid JSON",
			input:     `{invalid`,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatJSON(tt.input)

			if result.Success == tt.wantError {
				t.Errorf("FormatJSON() success = %v, wantError = %v", result.Success, tt.wantError)
			}

			if !tt.wantError {
				if result.Result == "" {
					t.Error("FormatJSON() returned empty result")
				}
				// Verify it has indentation
				if !strings.Contains(result.Result, "  ") {
					t.Error("FormatJSON() result not properly indented")
				}
			}
		})
	}
}

// Test minifyJSON (JSON minification)
func TestMinifyJSON(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantError bool
	}{
		{
			name:      "formatted JSON",
			input:     "{\n  \"name\": \"Alice\",\n  \"age\": 30\n}",
			wantError: false,
		},
		{
			name:      "already minified",
			input:     `{"name":"Alice"}`,
			wantError: false,
		},
		{
			name:      "empty string",
			input:     "",
			wantError: true,
		},
		{
			name:      "invalid JSON",
			input:     `{bad`,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MinifyJSON(tt.input)

			if result.Success == tt.wantError {
				t.Errorf("MinifyJSON() success = %v, wantError = %v", result.Success, tt.wantError)
			}

			if !tt.wantError {
				// Verify no extra whitespace
				if strings.Contains(result.Result, "\n") || strings.Contains(result.Result, "  ") {
					t.Error("MinifyJSON() result contains whitespace")
				}
			}
		})
	}
}

// Test validateJSON (JSON validation)
func TestValidateJSON(t *testing.T) {
	tests := []struct {
		name  string
		input string
		valid bool
	}{
		{
			name:  "valid JSON object",
			input: `{"name":"Alice","age":30}`,
			valid: true,
		},
		{
			name:  "valid JSON array",
			input: `[1,2,3]`,
			valid: true,
		},
		{
			name:  "valid JSON string",
			input: `"hello"`,
			valid: true,
		},
		{
			name:  "valid JSON number",
			input: `42`,
			valid: true,
		},
		{
			name:  "empty string",
			input: "",
			valid: false,
		},
		{
			name:  "invalid JSON - incomplete object",
			input: `{incomplete`,
			valid: false,
		},
		{
			name:  "invalid JSON - trailing comma",
			input: `{"name":"Alice",}`,
			valid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ValidateJSON(tt.input)
			if result.Valid != tt.valid {
				t.Errorf("ValidateJSON() = %v, want %v (error: %s)", result.Valid, tt.valid, result.Error)
			}
		})
	}
}
