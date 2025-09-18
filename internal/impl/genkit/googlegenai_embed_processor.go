package genkit

import (
	"fmt"

	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/plugins/googlegenai"
	"github.com/warpstreamlabs/bento/public/service"
)

func googleGenAIEmbedProcessorConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("AI", "Genkit").
		Summary("Generates embeddings using Google AI models.").
		Description("Generates text embeddings using Google AI's embedding models through the Genkit framework. For more information about Genkit, see https://genkit.dev and https://github.com/firebase/genkit.").
		Fields(commonEmbedFields()...).
		Field(service.NewStringField(apiKeyField).
			Description("Google AI API key.").
			Secret().
			Examples("${GOOGLE_API_KEY}", "${GEMINI_API_KEY}")).
		Example("Text Embedding with Google AI", `
Generate embeddings using Google AI embedding models:

`+"```yaml"+`
pipeline:
  processors:
    - genkit_googlegenai_embed:
        api_key: "${GOOGLE_AI_API_KEY}"
        model: "text-embedding-004"
`+"```"+``, "")
}

func init() {
	err := service.RegisterProcessor(
		"genkit_googlegenai_embed", googleGenAIEmbedProcessorConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newGoogleGenAIEmbedder(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

func newGoogleGenAIEmbedder(pconf *service.ParsedConfig, res *service.Resources) (service.Processor, error) {
	apiKey, err := pconf.FieldString(apiKeyField)
	if err != nil {
		return nil, err
	}

	if apiKey == "" {
		return nil, fmt.Errorf("field '%s' cannot be empty", apiKeyField)
	}

	googleAIPlugin := &googlegenai.GoogleAI{
		APIKey: apiKey,
	}

	plugin, err := InitPlugin(res, googleAIPlugin)
	if err != nil {
		return nil, err
	}

	modelName, err := pconf.FieldString(modelField)
	if err != nil {
		return nil, err
	}

	g, err := LoadInstance(res)
	if err != nil {
		return nil, err
	}

	embedder := googlegenai.GoogleAIEmbedder(g, modelName)
	if embedder == nil {
		embedder, err = plugin.DefineEmbedder(g, modelName, &ai.EmbedderOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to define embedder for model %s: %w", modelName, err)
		}
	}

	if embedder == nil {
		return nil, fmt.Errorf("embedder not found for plugin %s with model %s - ensure the model is available", plugin.Name(), modelName)
	}

	return newEmbedderProcessor(pconf, nil, embedder)
}
