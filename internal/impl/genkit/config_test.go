package genkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/public/service"
)

func TestConfigParsing(t *testing.T) {
	tests := []struct {
		name           string
		yamlConf       string
		expectModel    string
		expectTemplate string
		expectTemp     float64
		expectTokens   int
	}{
		{
			name: "basic_config",
			yamlConf: `
address: "http://localhost:11434"
model: "llama3.2"
type: "chat"
config:
  temperature: 0.7
  maxOutputTokens: 100
prompt: "Hello {{name}}, you are in {{location}}"
`,
			expectModel:    "llama3.2",
			expectTemplate: "Hello {{name}}, you are in {{location}}",
			expectTemp:     0.7,
			expectTokens:   100,
		},
		{
			name: "minimal",
			yamlConf: `
address: "http://localhost:11434"
model: "llama3.2"
type: "chat"
prompt: "Simple template"
`,
			expectModel:    "llama3.2",
			expectTemplate: "Simple template",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := ollamaProcessorConfigSpec()
			conf, err := spec.ParseYAML(tt.yamlConf, service.NewEnvironment())
			require.NoError(t, err)

			// Test model parsing
			model, err := conf.FieldString(modelField)
			require.NoError(t, err)
			assert.Equal(t, tt.expectModel, model)

			// Test template parsing
			template, err := conf.FieldInterpolatedString(promptField)
			require.NoError(t, err)
			templateStr, ok := template.Static()
			require.True(t, ok)
			assert.Equal(t, tt.expectTemplate, templateStr)

			// Test config parsing
			config, err := parseModelConfig(conf)
			require.NoError(t, err)
			if tt.expectTemp > 0 {
				require.NotNil(t, config)
				assert.Equal(t, tt.expectTemp, config.Temperature)
				assert.Equal(t, tt.expectTokens, config.MaxOutputTokens)
			}
		})
	}
}
