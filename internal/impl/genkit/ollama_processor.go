package genkit

import (
	"github.com/firebase/genkit/go/plugins/ollama"
	"github.com/ollama/ollama/api"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	ollamaTypeField = "type"
)

func init() {
	err := service.RegisterProcessor(
		"genkit_ollama", ollamaProcessorConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newOllamaProcessor(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

func ollamaProcessorConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("AI", "Genkit").
		Summary("Generates content using Ollama models.").
		Description("Generates text content using Ollama models through the Genkit framework. For more information about Genkit, see https://genkit.dev and https://github.com/firebase/genkit.").
		Fields(
			service.NewURLField(addressField).Description("Server address of Ollama instance.").Default("http://localhost:11434"),
			service.NewStringEnumField(ollamaTypeField, "chat", "generate").Description("The type of task the model should perform.").Default("generate"),
		).Fields(commonGenerateFields()...).
		Example("Chat Completion with Ollama", `
Generate text using Ollama models in chat mode:

`+"```yaml"+`
pipeline:
  processors:
    - genkit_ollama:
        address: "http://localhost:11434"
        model: "llama3.2"
        type: "chat"
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

func newOllamaPlugin(pconf *service.ParsedConfig) (*ollama.Ollama, error) {
	addr, err := pconf.FieldURL(addressField)
	if err != nil {
		return nil, err
	}

	return &ollama.Ollama{
		ServerAddress: addr.String(),
	}, nil

}

func newOllamaProcessor(pconf *service.ParsedConfig, res *service.Resources) (service.Processor, error) {
	modelType, err := pconf.FieldString(ollamaTypeField)
	if err != nil {
		return nil, err
	}

	modelName, err := pconf.FieldString(modelField)
	if err != nil {
		return nil, err
	}

	plugin, err := newOllamaPlugin(pconf)
	if err != nil {
		return nil, err
	}

	p, err := InitPlugin(res, plugin)
	if err != nil {
		return nil, err
	}

	g, err := LoadInstance(res)
	if err != nil {
		return nil, err
	}

	model := p.DefineModel(g, ollama.ModelDefinition{
		Name: modelName,
		Type: modelType,
	}, nil)

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

	return newGenkitProcessor(pconf, res, config, model)

}
