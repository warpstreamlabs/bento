package genkit

import (
	"fmt"

	"github.com/firebase/genkit/go/plugins/googlegenai"
	"github.com/warpstreamlabs/bento/public/service"
	"google.golang.org/genai"
)

func init() {
	err := service.RegisterProcessor(
		"genkit_googlegenai", googlegenaiCompatibleSpec(),
		func(conf *service.ParsedConfig, res *service.Resources) (service.Processor, error) {
			plugin, err := newGoogleGenAIPlugin(conf)
			if err != nil {
				return nil, err
			}

			return newGoogleGenAIProcessor(conf, res, plugin)
		})
	if err != nil {
		panic(err)
	}
}

func googlegenaiCompatibleSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("AI", "Genkit").
		Summary("Generates content using Google AI models.").
		Description("Generates text content using Google AI models through the Genkit framework. For more information about Genkit, see https://genkit.dev and https://github.com/firebase/genkit.").
		Fields(
			service.NewStringField(apiKeyField).
				Description("Google AI API key.").
				Secret().
				Examples("${GOOGLE_API_KEY}", "${GEMINI_API_KEY}"),
		).Fields(
		commonGenerateFields()...,
	).
		Example("Chat Completion with Google AI", `
Generate text using Google AI models:

`+"```yaml"+`
pipeline:
  processors:
    - genkit_googlegenai:
        api_key: "${GOOGLE_AI_API_KEY}"
        model: "gemini-1.5-flash"
        config:
          temperature: 0.7
          max_output_tokens: 256
        prompt: |
          {{role "system"}}
          You are a helpful assistant.
          {{role "user"}}
          ${! content() }
`+"```"+``, "")
}

//------------------------------------------------------------------------------

func newGoogleGenAIPlugin(pconf *service.ParsedConfig) (*googlegenai.GoogleAI, error) {
	apiKey, err := pconf.FieldString(apiKeyField)
	if err != nil {
		return nil, err
	}
	if apiKey == "" {
		return nil, fmt.Errorf("field '%s' cannot be empty", apiKeyField)
	}

	return &googlegenai.GoogleAI{
		APIKey: apiKey,
	}, nil
}

func newGoogleGenAIProcessor(pconf *service.ParsedConfig, res *service.Resources, plugin *googlegenai.GoogleAI) (service.Processor, error) {
	modelName, err := pconf.FieldString(modelField)
	if err != nil {
		return nil, err
	}

	genkitConf, err := parseModelConfig(pconf)
	if err != nil {
		return nil, err
	}

	config := &genai.GenerateContentConfig{
		Temperature:     genai.Ptr(float32(genkitConf.Temperature)),
		TopP:            genai.Ptr(float32(genkitConf.TopP)),
		TopK:            genai.Ptr(float32(genkitConf.TopK)),
		MaxOutputTokens: int32(genkitConf.MaxOutputTokens),
		StopSequences:   genkitConf.StopSequences,
	}

	if _, err = InitPlugin(res, plugin); err != nil {
		return nil, err
	}

	g, err := LoadInstance(res)
	if err != nil {
		return nil, err
	}

	model := googlegenai.GoogleAIModel(g, modelName)
	if model == nil {
		return nil, fmt.Errorf("could not find model %s", modelName)
	}

	return newGenkitProcessor(pconf, res, config, model)
}
