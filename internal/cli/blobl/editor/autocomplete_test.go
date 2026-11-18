package editor

import (
	"testing"

	"github.com/warpstreamlabs/bento/internal/bloblang"
)

func TestGenerateAutocompletion(t *testing.T) {
	syntax := generateSyntax(bloblang.GlobalEnvironment(), testRenderer)
	ac := newAutocompleter(&syntax, testRenderer)

	tests := []struct {
		name           string
		req            AutocompletionRequest
		wantTypes      []string
		minCompletions int
	}{
		{
			name:           "function context returns functions and keywords",
			req:            AutocompletionRequest{IsMethodContext: false},
			wantTypes:      []string{"function", "keyword"},
			minCompletions: 10,
		},
		{
			name:           "method context returns methods",
			req:            AutocompletionRequest{IsMethodContext: true},
			wantTypes:      []string{"method"},
			minCompletions: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			completions := ac.Generate(tt.req)
			if len(completions) < tt.minCompletions {
				t.Errorf("expected >= %d completions, got %d", tt.minCompletions, len(completions))
			}

			types := map[string]bool{}
			for i, c := range completions {
				types[c.Type] = true
				if c.Caption == "" {
					t.Errorf("completion[%d] missing Caption", i)
				}
				if c.Value == "" && c.Snippet == "" {
					t.Errorf("completion[%d] %q missing both Value and Snippet", i, c.Caption)
				}
				// Function and method docs are looked up from the syntax by the editor.
				if (c.Type == "keyword") != (c.DocHTML != "") {
					t.Errorf("completion[%d] %q of type %s: DocHTML set = %v", i, c.Caption, c.Type, c.DocHTML != "")
				}
			}
			if len(types) != len(tt.wantTypes) {
				t.Errorf("got types %v, want %v", types, tt.wantTypes)
			}
			for _, want := range tt.wantTypes {
				if !types[want] {
					t.Errorf("expected completion type %q, got %v", want, types)
				}
			}
		})
	}
}

func TestAutocompletionItemInsertText(t *testing.T) {
	syntax := generateSyntax(bloblang.GlobalEnvironment(), testRenderer)
	byCaption := map[string]AutocompletionItem{}
	for _, c := range newAutocompleter(&syntax, testRenderer).Generate(AutocompletionRequest{IsMethodContext: true}) {
		byCaption[c.Caption] = c
	}

	if c := byCaption["uppercase"]; c.Value != "uppercase()" || c.Snippet != "" {
		t.Errorf("method without params: got value %q snippet %q", c.Value, c.Snippet)
	}
	if c := byCaption["join"]; c.Snippet != "join($1)" || c.Value != "" {
		t.Errorf("method with params: got value %q snippet %q", c.Value, c.Snippet)
	}
}
