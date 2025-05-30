package huggingface

import (
	"github.com/knights-analytics/hugot"
	"github.com/knights-analytics/hugot/pipelineBackends"
	"github.com/knights-analytics/hugot/pipelines"

	"github.com/warpstreamlabs/bento/public/service"
)

func HugotFeatureExtractionConfigSpec() *service.ConfigSpec {
	featureExtractionDescription := "### Feature Extraction" + "\n" +
		"Feature extraction is the task of extracting features learnt in a model." +
		"This processor runs a feature extraction model against batches of text data, returning a model's multidimensional representation of said features" +
		"in tensor/float64 format." + "\n" + description

	spec := hugotConfigSpec().
		Summary("Performs feature extraction using a Hugging Face 🤗 NLP pipeline with an ONNX Runtime model.").
		Description(featureExtractionDescription).
		Field(service.NewBoolField("normalization").
			Description("Whether to apply normalization in the feature extraction pipeline.").
			Default(false))

	return spec
}

func init() {
	err := service.RegisterBatchProcessor("nlp_extract_features", HugotFeatureExtractionConfigSpec(), NewFeatureExtractionPipeline)
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

func getFeatureExtractionOptions(conf *service.ParsedConfig) ([]pipelineBackends.PipelineOption[*pipelines.FeatureExtractionPipeline], error) {
	var options []pipelineBackends.PipelineOption[*pipelines.FeatureExtractionPipeline]

	normalization, err := conf.FieldBool("normalization")
	if err != nil {
		return nil, err
	}
	if normalization {
		options = append(options, pipelines.WithNormalization())
	}

	return options, nil
}

//------------------------------------------------------------------------------

func NewFeatureExtractionPipeline(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	p, err := newPipelineProcessor(conf, mgr)
	if err != nil {
		return nil, err
	}

	opts, err := getFeatureExtractionOptions(conf)
	if err != nil {
		return nil, err
	}

	cfg := hugot.FeatureExtractionConfig{
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

func convertFeatureExtractionOutput(result pipelines.FeatureExtractionOutput) []any {
	return result.GetOutput()
}
