package genkit

import (
	"fmt"

	"github.com/firebase/genkit/go/plugins/googlegenai"
	"github.com/warpstreamlabs/bento/public/service"
	"google.golang.org/genai"
)

func vertexAICompatibleSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("AI", "Genkit").
		Summary("Generates content using Vertex AI models.").
		Description("Generates text content using Google Cloud Vertex AI models through the Genkit framework. For more information about Genkit, see https://genkit.dev and https://github.com/firebase/genkit.").
		Fields(
			service.NewStringField("project_id").
				Description("Google Cloud project ID. If not provided, will use GOOGLE_CLOUD_PROJECT environment variable.").
				Optional(),
			service.NewStringField("location").
				Description("Google Cloud location/region. If not provided, will use GOOGLE_CLOUD_LOCATION or GOOGLE_CLOUD_REGION environment variables.").
				Optional(),
		).Fields(
		commonGenerateFields()...,
	).
		Example("Chat Completion with Vertex AI", `
Generate text using Google Cloud Vertex AI models:

`+"```yaml"+`
pipeline:
  processors:
    - genkit_vertexai:
        project_id: "my-gcp-project"
        location: "us-central1"
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

func init() {
	err := service.RegisterProcessor(
		"genkit_vertexai", vertexAICompatibleSpec(),
		func(conf *service.ParsedConfig, res *service.Resources) (service.Processor, error) {
			plugin, err := newVertexAIPlugin(conf)
			if err != nil {
				return nil, err
			}

			return newVertexAIProcessor(conf, res, plugin)
		})
	if err != nil {
		panic(err)
	}
}

func newVertexAIPlugin(pconf *service.ParsedConfig) (*googlegenai.VertexAI, error) {
	var projectID, location string
	var err error

	if pconf.Contains("project_id") {
		projectID, err = pconf.FieldString("project_id")
		if err != nil {
			return nil, err
		}
	}

	if pconf.Contains("location") {
		location, err = pconf.FieldString("location")
		if err != nil {
			return nil, err
		}
	}

	return &googlegenai.VertexAI{
		ProjectID: projectID,
		Location:  location,
	}, nil
}

func newVertexAIProcessor(pconf *service.ParsedConfig, res *service.Resources, plugin *googlegenai.VertexAI) (service.Processor, error) {
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

	model := googlegenai.VertexAIModel(g, modelName)
	if model == nil {
		return nil, fmt.Errorf("could not find model %s", modelName)
	}

	return newGenkitProcessor(pconf, res, config, model)
}
