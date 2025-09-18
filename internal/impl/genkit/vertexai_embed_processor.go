package genkit

import (
	"fmt"

	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/plugins/googlegenai"
	"github.com/warpstreamlabs/bento/public/service"
)

func vertexAIEmbedProcessorConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("AI", "Genkit").
		Summary("Generates embeddings using Vertex AI models.").
		Description("Generates text embeddings using Google Cloud Vertex AI embedding models through the Genkit framework. For more information about Genkit, see https://genkit.dev and https://github.com/firebase/genkit.").
		Fields(commonEmbedFields()...).
		Field(service.NewStringField("project_id").
			Description("Google Cloud project ID. If not provided, will use GOOGLE_CLOUD_PROJECT environment variable.").
			Optional()).
		Field(service.NewStringField("location").
			Description("Google Cloud location/region. If not provided, will use GOOGLE_CLOUD_LOCATION or GOOGLE_CLOUD_REGION environment variables.").
			Optional()).
		Example("Text Embedding with Vertex AI", `
Generate embeddings using Google Cloud Vertex AI models:

`+"```yaml"+`
pipeline:
  processors:
    - genkit_vertexai_embed:
        project_id: "my-gcp-project"
        location: "us-central1"
        model: "text-embedding-004"
`+"```"+``, "")
}

func init() {
	err := service.RegisterProcessor(
		"genkit_vertexai_embed", vertexAIEmbedProcessorConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newVertexAIEmbedder(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

func newVertexAIEmbedder(pconf *service.ParsedConfig, res *service.Resources) (service.Processor, error) {
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

	vertexAIPlugin := &googlegenai.VertexAI{
		ProjectID: projectID,
		Location:  location,
	}

	plugin, err := InitPlugin(res, vertexAIPlugin)
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

	embedder := googlegenai.VertexAIEmbedder(g, modelName)
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
