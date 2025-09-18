package genkit

import (
	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/genkit"
	oai "github.com/firebase/genkit/go/plugins/compat_oai/openai"

	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"

	"github.com/warpstreamlabs/bento/public/service"
)

func init() {
	err := service.RegisterProcessor(
		"genkit_openai", openaiCompatibleSpec(),
		func(conf *service.ParsedConfig, res *service.Resources) (service.Processor, error) {
			plugin, err := newOpenAIPlugin(conf)
			if err != nil {
				return nil, err
			}

			return newOpenAIProcessor(conf, res, plugin)
		})
	if err != nil {
		panic(err)
	}
}

func openaiCompatibleSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("AI", "Genkit").
		Summary("Generates content using OpenAI models.").
		Description("Generates text content using OpenAI models through the Genkit framework. For more information about Genkit, see https://genkit.dev and https://github.com/firebase/genkit.").
		Fields(
			service.NewURLField(addressField).Description("Server address of OpenAI API.").Optional(),
			service.NewStringField(apiKeyField).Description("OpenAI API key.").Secret().Examples("${OPENAI_API_KEY}"),
		).Fields(
		commonGenerateFields()...,
	).
		Example("Chat Completion with OpenAI", `
Generate text using OpenAI models:

`+"```yaml"+`
pipeline:
  processors:
    - genkit_openai:
        api_key: "${OPENAI_API_KEY}"
        model: "gpt-4o-mini"
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

func newOpenAIPlugin(pconf *service.ParsedConfig) (*oai.OpenAI, error) {
	addr, err := pconf.FieldURL(addressField)
	if err != nil {
		return nil, err
	}

	apiKey, err := pconf.FieldString(apiKeyField)
	if err != nil {
		return nil, err
	}

	return &oai.OpenAI{
		APIKey: apiKey,
		Opts: []option.RequestOption{
			option.WithBaseURL(addr.Host),
		},
	}, nil
}

type ModelPlugin interface {
	Plugin
	Model(g *genkit.Genkit, name string) ai.Model
}

func newOpenAIProcessor(pconf *service.ParsedConfig, res *service.Resources, plugin ModelPlugin) (service.Processor, error) {
	model, err := pconf.FieldString(modelField)
	if err != nil {
		return nil, err
	}

	genkitConf, err := parseModelConfig(pconf)
	if err != nil {
		return nil, err
	}

	config := openai.ChatCompletionNewParams{
		MaxCompletionTokens: openai.Int(int64(genkitConf.MaxOutputTokens)),
		Temperature:         openai.Float(genkitConf.Temperature),
		TopP:                openai.Float(genkitConf.TopP),
		// TODO(gregfurman): Add validations (i.e can be up to 4 tokens)
		Stop: openai.ChatCompletionNewParamsStopUnion{OfStringArray: genkitConf.StopSequences},
	}

	p, err := InitPlugin(res, plugin)
	if err != nil {
		return nil, err
	}

	g, err := LoadInstance(res)
	if err != nil {
		return nil, err
	}

	return newGenkitProcessor(pconf, res, config, p.Model(g, model))
}
