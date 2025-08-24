package huggingface

import (
	"github.com/knights-analytics/hugot"
	"github.com/knights-analytics/hugot/pipelineBackends"
	"github.com/knights-analytics/hugot/pipelines"

	"github.com/warpstreamlabs/bento/public/service"
)

func HugotFeatureExtractionConfigSpec() *service.ConfigSpec {
	featureExtractionDescription := "### Feature Extraction" + "\n" +
		"Feature extraction is the task of extracting features learnt in a model. " +
		"This processor runs a feature extraction model against batches of text data, returning a model's multidimensional representation of said features " +
		"in tensor/float64 format." + "\n" + description

	spec := hugotConfigSpec().
		Summary("Performs feature extraction using a Hugging Face 🤗 NLP pipeline with an ONNX Runtime model.").
		Description(featureExtractionDescription).
		Field(service.NewBoolField("normalization").
			Description("Whether to apply normalization in the feature extraction pipeline.").
			Default(false)).
		Example("Text Embeddings", "Extract normalized embeddings from text using a sentence transformer model stored locally.",
			`
pipeline:
  processors:
    - nlp_extract_features:
        path: "onnx/model.onnx"
        normalization: true
# In: "Hello world"
# Out: [0.1234, -0.5678, 0.9012, ...] (384-dimensional vector)
`).
		Example("Document Embeddings", "Extract raw features from documents using the all-MiniLM-L6-v2 model.",
			`
pipeline:
  processors:
    - nlp_extract_features:
        path: "./models"
        enable_download: true
        download_options:
          repository: "sentence-transformers/all-MiniLM-L6-v2"
        normalization: false
# In: "This is a sample document for feature extraction."
# Out: [0.2341, -0.8765, 1.2345, ...] (384-dimensional vector)
`)

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
