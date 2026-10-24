package editor

import (
	"testing"

	"github.com/warpstreamlabs/bento/internal/bloblang"
)

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
			result := executeMapping(env, tt.input, tt.mapping)

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
