package huggingface

import (
	"github.com/knights-analytics/hugot"
	"github.com/knights-analytics/hugot/pipelineBackends"
	"github.com/knights-analytics/hugot/pipelines"

	"github.com/warpstreamlabs/bento/public/service"
)

func HugotTokenClassificationConfigSpec() *service.ConfigSpec {
	tokenClassificationDescription := "### Token Classification" + "\n" +
		"Token classification assigns a label to individual tokens in a sentence." + "\n" +
		"This processor runs token classification inference against batches of text data, returning a set of Entities classification corresponding to each input." + "\n" +
		description

	spec := hugotConfigSpec().
		Summary("Performs token classification using a Hugging Face 🤗 NLP pipeline with an ONNX Runtime model.").
		Description(tokenClassificationDescription).
		Field(service.NewStringEnumField("aggregation_strategy", "SIMPLE", "NONE").
			Description("The aggregation strategy to use for the token classification pipeline.").Default("SIMPLE")).
		Field(service.NewStringListField("ignore_labels").
			Description("Labels to ignore in the token classification pipeline.").
			Default([]string{}).
			Example([]string{"O", "MISC"})).
		Example("Named Entity Recognition", "Extract entities like persons, organizations, and locations from text.",
			`
pipeline:
  processors:
    - nlp_classify_tokens:
        path: "KnightsAnalytics/distilbert-NER"
        aggregation_strategy: "SIMPLE"
        ignore_labels: ["O"]
# In: "John works at Apple Inc. in New York."
# Out: [
#   {"Entity": "PER", "Score": 0.997136, "Index": 0, "Word": "John", "Start": 0, "End": 4, "IsSubword": false},
#   {"Entity": "ORG", "Score": 0.985432, "Index": 3, "Word": "Apple Inc.", "Start": 14, "End": 24, "IsSubword": false},
#   {"Entity": "LOC", "Score": 0.972841, "Index": 6, "Word": "New York", "Start": 28, "End": 36, "IsSubword": false}
# ]
`).
		Example("Custom Entity Extraction", "Extract entities with no aggregation to see individual token classifications.",
			`
pipeline:
  processors:
    - nlp_classify_tokens:
        path: "KnightsAnalytics/distilbert-NER"
        aggregation_strategy: "NONE"
        ignore_labels: ["O", "MISC"]
# In: "Microsoft was founded by Bill Gates."
# Out: [
#   {"Entity": "B-ORG", "Score": 0.991234, "Index": 0, "Word": "Microsoft", "Start": 0, "End": 9, "IsSubword": false},
#   {"Entity": "B-PER", "Score": 0.987654, "Index": 4, "Word": "Bill", "Start": 23, "End": 27, "IsSubword": false},
#   {"Entity": "I-PER", "Score": 0.976543, "Index": 5, "Word": "Gates", "Start": 28, "End": 33, "IsSubword": false}
# ]
`)

	return spec
}

func init() {
	err := service.RegisterBatchProcessor("nlp_classify_tokens", HugotTokenClassificationConfigSpec(), NewTokenClassificationPipeline)
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

func getTokenClassificationOptions(conf *service.ParsedConfig) ([]pipelineBackends.PipelineOption[*pipelines.TokenClassificationPipeline], error) {

	var options []pipelineBackends.PipelineOption[*pipelines.TokenClassificationPipeline]

	aggregationStrategy, err := conf.FieldString("aggregation_strategy")
	if err != nil {
		return nil, err
	}
	switch aggregationStrategy {
	case "SIMPLE":
		options = append(options, pipelines.WithSimpleAggregation())
	case "NONE":
		options = append(options, pipelines.WithoutAggregation())
	}

	ignoreLabels, err := conf.FieldStringList("ignore_labels")
	if err != nil {
		return nil, err
	}
	if len(ignoreLabels) > 0 {
		options = append(options, pipelines.WithIgnoreLabels(ignoreLabels))
	}

	return options, nil
}

//------------------------------------------------------------------------------

func NewTokenClassificationPipeline(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	p, err := newPipelineProcessor(conf, mgr)
	if err != nil {
		return nil, err
	}

	opts, err := getTokenClassificationOptions(conf)
	if err != nil {
		return nil, err
	}

	cfg := hugot.TokenClassificationConfig{
		Name:      p.pipelineName,
		ModelPath: p.modelPath,
		Options:   opts,
	}

	if p.pipeline, err = hugot.NewPipeline(p.session, cfg); err != nil {
		return nil, err
	}

	if err := p.pipeline.Validate(); err != nil {
		return nil, err
	}

	if md := p.pipeline.GetMetadata().OutputsInfo; len(md) > 0 {
		p.metaOutputType = md[0].Name
		p.metaOutputLayerDim = md[0].Dimensions
	}

	return p, nil
}

func convertTokenClassificationOutput(result *pipelines.TokenClassificationOutput) []any {
	out := make([]any, len(result.Entities))
	for i, entityBatch := range result.Entities {
		batchMaps := make([]any, len(entityBatch))
		for j, entity := range entityBatch {
			batchMaps[j] = map[string]any{
				"Entity":    entity.Entity,
				"Score":     entity.Score,
				"Scores":    entity.Scores,
				"Index":     entity.Index,
				"Word":      entity.Word,
				"TokenID":   entity.TokenID,
				"Start":     entity.Start,
				"End":       entity.End,
				"IsSubword": entity.IsSubword,
			}
		}
		out[i] = batchMaps
	}
	return out
}
