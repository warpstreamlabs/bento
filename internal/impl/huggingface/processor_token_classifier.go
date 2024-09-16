//go:build huggingbento

package huggingface

import (
	"github.com/knights-analytics/hugot"
	"github.com/knights-analytics/hugot/pipelines"

	"github.com/warpstreamlabs/bento/public/service"
)

func hugotTokenClassificationConfigSpec() *service.ConfigSpec {
	tokenClassificaitionDescription := "### Token Classification" + "\n" +
		"Token classification assigns a label to individual tokens in a sentence." +
		"This processor runs token classification inference against batches of text data, returning a set of Entities classification corresponding to each input." + "\n" +
		description

	spec := hugotConfigSpec().
		Summary("Performs token classification using a Hugging Face 🤗 NLP pipeline with an ONNX Runtime model.").
		Description(tokenClassificaitionDescription).
		Field(service.NewStringEnumField("aggregation_strategy",
			"SIMPLE",
			"NONE",
		).Description("The aggregation strategy to use for the token classification pipeline.").Default("SIMPLE")).
		Field(service.NewStringListField("ignore_labels").
			Description("Labels to ignore in the token classification pipeline.").
			Default([]string{}).
			Example([]string{"O", "MISC"}))

	return spec
}

func init() {
	err := service.RegisterBatchProcessor("nlp_classify_tokens", hugotTokenClassificationConfigSpec(), newTokenClassificationPipeline)
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

func getTokenClassificationOptions(conf *service.ParsedConfig) ([]pipelines.PipelineOption[*pipelines.TokenClassificationPipeline], error) {
	var options []pipelines.PipelineOption[*pipelines.TokenClassificationPipeline]

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

func newTokenClassificationPipeline(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	p, err := newPipelineProcessor(conf, mgr)
	if err != nil {
		return nil, err
	}

	opts, err := getTokenClassificationOptions(conf)
	if err != nil {
		return nil, err
	}

	cfg := hugot.TokenClassificationConfig{
		Name:         p.pipelineName,
		OnnxFilename: p.onnxFilename,
		ModelPath:    p.modelPath,
		Options:      opts,
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
