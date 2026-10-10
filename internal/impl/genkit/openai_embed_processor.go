package genkit

import (
	"fmt"

	oai "github.com/firebase/genkit/go/plugins/compat_oai/openai"
	"github.com/openai/openai-go/option"
	"github.com/warpstreamlabs/bento/public/service"
)

func openAIEmbedProcessorConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("AI", "Genkit").
		Summary("Generates embeddings using OpenAI models.").
		Description("Generates text embeddings using OpenAI's embedding models through the Genkit framework. For more information about Genkit, see https://genkit.dev and https://github.com/firebase/genkit.").
		Fields(commonEmbedFields()...).
		Field(service.NewURLField(addressField).
			Description("Server address of OpenAI API.").
			Optional()).
		Field(service.NewStringField(apiKeyField).
			Description("OpenAI API key.").
			Secret().
			Examples("${OPENAI_API_KEY}")).
		Example("Text Embedding with OpenAI", `
Generate embeddings using OpenAI's embedding models:

`+"```yaml"+`
pipeline:
  processors:
    - genkit_openai_embed:
        api_key: "${OPENAI_API_KEY}"
        model: "text-embedding-3-small"
`+"```"+``, "")
}

func init() {
	err := service.RegisterProcessor(
		"genkit_openai_embed", openAIEmbedProcessorConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			plugin, err := newOpenAIEmbedPlugin(conf)
			if err != nil {
				return nil, err
			}

			return newOpenAIEmbedder(conf, mgr, plugin)
		})
	if err != nil {
		panic(err)
	}
}

func newOpenAIEmbedPlugin(pconf *service.ParsedConfig) (*oai.OpenAI, error) {
	var opts []option.RequestOption

	if pconf.Contains(addressField) {
		addr, err := pconf.FieldURL(addressField)
		if err != nil {
			return nil, err
		}
		opts = append(opts, option.WithBaseURL(addr.String()))
	}

	apiKey, err := pconf.FieldString(apiKeyField)
	if err != nil {
		return nil, err
	}

	if apiKey == "" {
		return nil, fmt.Errorf("field '%s' cannot be empty", apiKeyField)
	}

	return &oai.OpenAI{
		APIKey: apiKey,
		Opts:   opts,
	}, nil
}

func newOpenAIEmbedder(pconf *service.ParsedConfig, res *service.Resources, plugin *oai.OpenAI) (service.Processor, error) {
	_, err := InitPlugin(res, plugin)
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

	embedder := plugin.Embedder(g, modelName)
	if embedder == nil {
		return nil, fmt.Errorf("embedder not found for plugin %s with model %s - ensure the model is available", plugin.Name(), modelName)
	}

	return newEmbedderProcessor(pconf, nil, embedder)
}