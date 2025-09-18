package genkit

import (
	oai "github.com/firebase/genkit/go/plugins/compat_oai/anthropic"
	"github.com/openai/openai-go/option"
	"github.com/warpstreamlabs/bento/public/service"
)

func anthropicCompatibleSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("AI", "Genkit").
		Summary("Generates content using Anthropic models.").
		Description("Generates text content using Anthropic models through the Genkit framework. Supports Claude models with advanced reasoning capabilities. For more information about Genkit, see https://genkit.dev and https://github.com/firebase/genkit.").
		Fields(
			service.NewURLField(addressField).Description("Server address of Anthropic API.").Optional(),
			service.NewStringField(apiKeyField).Description("Anthropic API key.").Secret().Examples("${ANTHROPIC_API_KEY}"),
		).Fields(
		commonGenerateFields()...,
	).
		Example("Chat Completion with Anthropic", `
Generate text using Anthropic's Claude models:

`+"```yaml"+`
pipeline:
  processors:
    - genkit_anthropic:
        api_key: "${ANTHROPIC_API_KEY}"
        model: "claude-3-5-haiku-20241022"
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

func init() {
	err := service.RegisterProcessor(
		"genkit_anthropic", anthropicCompatibleSpec(),
		func(conf *service.ParsedConfig, res *service.Resources) (service.Processor, error) {
			plugin, err := newAnthropicPlugin(conf)
			if err != nil {
				return nil, err
			}

			return newOpenAIProcessor(conf, res, plugin)
		})
	if err != nil {
		panic(err)
	}
}

func newAnthropicPlugin(pconf *service.ParsedConfig) (*oai.Anthropic, error) {
	var opts []option.RequestOption
	if pconf.Contains(addressField) {
		addr, err := pconf.FieldURL(addressField)
		if err != nil {
			return nil, err
		}
		opts = append(opts, option.WithBaseURL(addr.Host))
	}

	apiKey, err := pconf.FieldString(apiKeyField)
	if err != nil {
		return nil, err
	}
	opts = append(opts, option.WithAPIKey(apiKey))

	return &oai.Anthropic{
		Opts: opts,
	}, nil
}
