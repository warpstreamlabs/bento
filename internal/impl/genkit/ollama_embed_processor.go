package genkit

import (
	"fmt"

	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/plugins/ollama"
	"github.com/ollama/ollama/api"
	"github.com/warpstreamlabs/bento/public/service"
)

func ollamaEmbedProcessorConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("AI", "Genkit").
		Summary("Generates embeddings using Ollama models.").
		Description("Generates text embeddings using Ollama models through the Genkit framework. For more information about Genkit, see https://genkit.dev and https://github.com/firebase/genkit.").
		Fields(commonEmbedFields()...).
		Field(service.NewURLField(addressField).
			Description("Server address of Ollama instance.").
			Default("http://localhost:11434")).
		Field(service.NewStringField(modelField).
			Description("Ollama embedding model to use (e.g., 'nomic-embed-text', 'all-minilm').")).
		Example("Basic Text Embedding", `
Generate embeddings for text content using a local Ollama instance:

`+"```yaml"+`
pipeline:
  processors:
    - genkit_ollama_embed:
        address: "http://localhost:11434"
        model: "nomic-embed-text"
`+"```"+``, "")
}

func init() {
	err := service.RegisterProcessor(
		"genkit_ollama_embed", ollamaEmbedProcessorConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newOllamaEmbedder(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

func newOllamaEmbedder(pconf *service.ParsedConfig, res *service.Resources) (service.Processor, error) {
	ollamaPlugin, err := newOllamaPlugin(pconf)
	if err != nil {
		return nil, err
	}

	plugin, err := InitPlugin(res, ollamaPlugin)
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

	genkitConf, err := parseModelConfig(pconf)
	if err != nil {
		return nil, err
	}

	// TODO(gregfurman: Implement the remaining options in github.com/ollama/ollama/api
	// TODO(gregfurma): Should we be using api.DefaultOptions()?
	config := api.Options{
		NumPredict:  genkitConf.MaxOutputTokens,
		TopK:        genkitConf.TopK,
		TopP:        float32(genkitConf.TopP),
		Stop:        genkitConf.StopSequences,
		Temperature: float32(genkitConf.Temperature),
	}

	embedder := ollama.Embedder(g, ollamaPlugin.ServerAddress)
	if embedder == nil {
		embedder = plugin.DefineEmbedder(g, ollamaPlugin.ServerAddress, modelName, &ai.EmbedderOptions{})
	}

	if embedder == nil {
		return nil, fmt.Errorf("embedder not found for plugin %s with model %s - ensure Ollama is running and the model is available", plugin.Name(), modelName)
	}

	return newEmbedderProcessor(pconf, config, embedder)
}
