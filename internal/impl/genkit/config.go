package genkit

import (
	"encoding/json"

	"github.com/firebase/genkit/go/ai"
	"github.com/google/dotprompt/go/dotprompt"
	"github.com/invopop/jsonschema"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	modelField   = "model"
	configField  = "config"
	promptField  = "prompt"
	addressField = "address"

	// Config section fields (genkit/dotprompt naming is pascal-case)
	maxOutputTokensField = "max_output_tokens"
	stopSequencesField   = "stop_sequences"
	temperatureField     = "temperature"
	topKField            = "top_k"
	topPField            = "top_p"

	toolsField = "tools"

	outputField            = "output"
	outputSchemaField      = "schema"
	outputFormatField      = "format"
	outputInstructionField = "instructions"

	apiKeyField = "api_key"
)

func commonEmbedFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField(modelField).Description("Model identifier."),
		service.NewObjectField(configField,
			service.NewIntField(maxOutputTokensField).
				Description("Maximum tokens the model can output.").
				Optional(),
			service.NewStringListField(stopSequencesField).
				Description("List of stop sequences.").
				Optional(),
			service.NewFloatField(temperatureField).
				Description("Controls randomness (0.0 to 1.0).").
				Optional(),
			service.NewIntField(topKField).
				Description("Top-K sampling parameter.").
				Optional(),
			service.NewFloatField(topPField).
				Description("Top-P sampling parameter.").
				Optional(),
		).Description("Model configuration matching common Genkit parameters.").Optional(),
		service.NewInterpolatedStringField(promptField).
			Description("Prompt template. use bento bloblang interpolation for injecting in custom data.").
			Optional(),
	}
}

func commonGenerateFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField(modelField).Description("Model identifier."),
		service.NewObjectField(configField,
			service.NewIntField(maxOutputTokensField).
				Description("Maximum tokens the model can output.").
				Optional(),
			service.NewStringListField(stopSequencesField).
				Description("List of stop sequences.").
				Optional(),
			service.NewFloatField(temperatureField).
				Description("Controls randomness (0.0 to 1.0).").
				Optional(),
			service.NewIntField(topKField).
				Description("Top-K sampling parameter.").
				Optional(),
			service.NewFloatField(topPField).
				Description("Top-P sampling parameter.").
				Optional(),
		).Description("Model configuration matching common Genkit parameters.").Optional(),
		service.NewInterpolatedStringField(promptField).
			Description("Prompt template. use bento bloblang interpolation for injecting in custom data."),
		service.NewObjectListField(toolsField,
			service.NewProcessorField("processor"),
			service.NewStringField("name"),
			service.NewStringField("description")).
			Description("A map of processors that the model can utilise as tools.").
			Optional(),
		service.NewObjectField(outputField,
			service.NewAnyField(outputSchemaField).Description("Output schema in Picoschema format.").Optional(),
			service.NewStringEnumField(outputFormatField,
				ai.OutputFormatJSON,
				ai.OutputFormatJSONL,
				ai.OutputFormatText,
				ai.OutputFormatArray,
				ai.OutputFormatEnum,
				// ai.OutputFormatMedia,
			).Description("The format of the response.").Default(ai.OutputFormatText).Optional(),
			service.NewInterpolatedStringField(outputInstructionField).Description("Instructions for the model output.").Optional(),
		).Description("Determines the schema/format of the output.").Optional(),
	}
}

func parseModelConfig(pconf *service.ParsedConfig) (*ai.GenerationCommonConfig, error) {
	conf := pconf.Namespace(configField)
	if conf == nil {
		return nil, nil
	}

	var err error
	genConf := &ai.GenerationCommonConfig{}

	if conf.Contains(maxOutputTokensField) {
		if genConf.MaxOutputTokens, err = conf.FieldInt(maxOutputTokensField); err != nil {
			return nil, err
		}
	}

	if conf.Contains(stopSequencesField) {
		if genConf.StopSequences, err = conf.FieldStringList(stopSequencesField); err != nil {
			return nil, err
		}
	}

	if conf.Contains(temperatureField) {
		if genConf.Temperature, err = conf.FieldFloat(temperatureField); err != nil {
			return nil, err
		}
	}

	if conf.Contains(topKField) {
		if genConf.TopK, err = conf.FieldInt(topKField); err != nil {
			return nil, err
		}
	}

	if conf.Contains(topPField) {
		if genConf.TopP, err = conf.FieldFloat(topPField); err != nil {
			return nil, err
		}
	}

	return genConf, nil
}

func extractSchema(pconf *service.ParsedConfig) (map[string]any, error) {
	var schema map[string]any

	if pconf.Contains(outputField, outputSchemaField) {
		schemaAny, err := pconf.FieldAny(outputField, outputSchemaField)
		if err != nil {
			return nil, err
		}

		schemaDef, err := dotprompt.Picoschema(schemaAny, &dotprompt.PicoschemaOptions{})
		if err != nil {
			return nil, err
		}

		schema, err = schemaAsMap(schemaDef)
		if err != nil {
			return nil, err
		}
	}

	return schema, nil
}

// schemaAsMap converts json schema struct to a map (JSON representation).
// Adapted from github.com/firebase/genkit/go/internal/base since the functionality we need
// is unfortunately private.
func schemaAsMap(s *jsonschema.Schema) (map[string]any, error) {
	jsb, err := s.MarshalJSON()
	if err != nil {
		return nil, err
	}

	// Check if the marshaled JSON is "true" (indicates an empty schema)
	if string(jsb) == "true" {
		return make(map[string]any), nil
	}

	var m map[string]any
	err = json.Unmarshal(jsb, &m)
	if err != nil {
		return nil, err
	}
	return m, nil
}
