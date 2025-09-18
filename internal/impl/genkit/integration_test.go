package genkit

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

const (
	testTimeout = 30 * time.Second
)

func TestOllamaEmbedIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	yamlConf := `
address: "http://localhost:11434"
model: "all-minilm"
`

	tests := []struct {
		name     string
		input    string
		validate func(t *testing.T, result [][]float32)
	}{
		{
			name:  "single_text_embedding",
			input: "Hello world",
			validate: func(t *testing.T, result [][]float32) {
				require.Len(t, result, 1)
				require.Greater(t, len(result[0]), 0)
				assert.Greater(t, len(result[0]), 300)
			},
		},
		{
			name:  "json_text_embedding",
			input: `This is a JSON-like text: {"type": "text", "content": "This is a test document"}`,
			validate: func(t *testing.T, result [][]float32) {
				require.Len(t, result, 1)
				require.Greater(t, len(result[0]), 0)
			},
		},
		{
			name:  "structured_content_as_text",
			input: `{"message": "test json content", "type": "example"}`,
			validate: func(t *testing.T, result [][]float32) {
				require.Len(t, result, 1)
				require.Greater(t, len(result[0]), 0)
			},
		},
		{
			name:  "binary_data_embedding",
			input: string([]byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}), // PNG header
			validate: func(t *testing.T, result [][]float32) {
				require.Len(t, result, 1)
				require.Greater(t, len(result[0]), 0)
			},
		},
		{
			name:  "mixed_text_content",
			input: `Document with mixed content: text, data: YmluYXJ5ZGF0YQ==, and metadata: {"version": 1}`,
			validate: func(t *testing.T, result [][]float32) {
				require.Len(t, result, 1)
				require.Greater(t, len(result[0]), 0)
			},
		},
		{
			name:  "empty_string",
			input: "",
			validate: func(t *testing.T, result [][]float32) {
				require.Len(t, result, 0)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := ollamaEmbedProcessorConfigSpec()
			conf, err := spec.ParseYAML(yamlConf, service.NewEnvironment())
			require.NoError(t, err)

			processor, err := newOllamaEmbedder(conf, service.MockResources())
			if err != nil {
				t.Fatalf("Skipping test due to setup error (likely missing Ollama server or model): %v", err)
			}

			msg := service.NewMessage([]byte(tt.input))

			ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
			defer cancel()

			result, err := processor.Process(ctx, msg)
			require.NoError(t, err)
			require.Len(t, result, 1)

			var embeddings [][]float32
			structured, err := result[0].AsStructuredMut()
			require.NoError(t, err)
			err = json.Unmarshal([]byte(fmt.Sprintf("%v", structured)), &embeddings)
			if err != nil {
				embeddings = structured.([][]float32)
			}

			tt.validate(t, embeddings)
		})
	}
}

func TestOllamaGenerationIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	tests := []struct {
		name     string
		yamlConf string
		input    string
		validate func(t *testing.T, output string)
	}{
		{
			name: "simple_generation",
			yamlConf: `
address: "http://localhost:11434"
model: "tinyllama"
type: "generate"
config:
  temperature: 1.0
  maxOutputTokens: 50
prompt: "Complete this sentence: The sky is"
`,
			input: "",
			validate: func(t *testing.T, output string) {
				assert.NotEmpty(t, output)
				assert.Greater(t, len(output), 5)
			},
		},
		{
			name: "templated_generation",
			yamlConf: `
address: "http://localhost:11434"
model: "tinyllama"
type: "generate"
config:
  temperature: 1.0
  maxOutputTokens: 30
prompt: "Answer this question: ${! content() }"
`,
			input: "What is 2+2?",
			validate: func(t *testing.T, output string) {
				assert.NotEmpty(t, output)
			},
		},
		{
			name: "chat_mode",
			yamlConf: `
address: "http://localhost:11434"
model: "tinyllama"
type: "chat"
config:
  temperature: 1.0
  maxOutputTokens: 20
prompt: "You are a helpful assistant. Answer: ${! content() }"
`,
			input: "Hello",
			validate: func(t *testing.T, output string) {
				assert.NotEmpty(t, output)
			},
		},
		{
			name: "bloblang_json_mapping",
			yamlConf: `address: "http://localhost:11434"
model: "tinyllama"
type: "generate"
config:
  temperature: 1.0
  maxOutputTokens: 30
prompt: 'Describe: ${! json("name") } is ${! json("age") } years old'`,
			input: `{"name": "Alice", "age": 25}`,
			validate: func(t *testing.T, output string) {
				assert.NotEmpty(t, output)
			},
		},
		{
			name: "bloblang_content_transformation",
			yamlConf: `
address: "http://localhost:11434"
model: "tinyllama"
type: "generate"
config:
  temperature: 1.0
  maxOutputTokens: 40
prompt: "Transform this: ${! content().uppercase() }"
`,
			input: "hello world",
			validate: func(t *testing.T, output string) {
				assert.NotEmpty(t, output)
			},
		},
		{
			name: "bloblang_array_access",
			yamlConf: `address: "http://localhost:11434"
model: "tinyllama"
type: "generate"
config:
  temperature: 1.0
  maxOutputTokens: 25
prompt: 'First item: ${! json("items").index(0) }'`,
			input: `{"items": ["apple", "banana", "cherry"]}`,
			validate: func(t *testing.T, output string) {
				assert.NotEmpty(t, output)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := ollamaProcessorConfigSpec()

			conf, err := spec.ParseYAML(tt.yamlConf, service.NewEnvironment())
			require.NoError(t, err)

			processor, err := newOllamaProcessor(conf, service.MockResources())
			if err != nil {
				t.Skipf("Skipping test due to setup error (likely missing Ollama server or model): %v", err)
			}

			msg := service.NewMessage([]byte(tt.input))

			ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
			defer cancel()

			result, err := processor.Process(ctx, msg)
			require.NoError(t, err)
			require.Len(t, result, 1)

			output, err := result[0].AsBytes()
			require.NoError(t, err)

			tt.validate(t, string(output))
		})
	}
}

func TestMultiPartDocumentEmbedding(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	input := `This is a multipart document with:
- Main text content: This is the main text content
- JSON metadata: {"metadata": {"source": "test", "importance": "high"}}
- Additional context: Additional context information
- Raw data: raw_data_content`

	yamlConf := `
address: "http://localhost:11434"
model: "all-minilm"
`
	spec := ollamaEmbedProcessorConfigSpec()
	config, err := spec.ParseYAML(yamlConf, service.NewEnvironment())
	require.NoError(t, err)

	processor, err := newOllamaEmbedder(config, service.MockResources())
	if err != nil {
		t.Fatalf("Skipping test due to setup error (likely missing Ollama server or model): %v", err)
	}

	msg := service.NewMessage([]byte(input))
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	result, err := processor.Process(ctx, msg)
	require.NoError(t, err)

	structured, err := result[0].AsStructuredMut()
	require.NoError(t, err)
	embeddings := structured.([][]float32)
	require.Len(t, embeddings, 1)
	require.Greater(t, len(embeddings[0]), 0)
}
