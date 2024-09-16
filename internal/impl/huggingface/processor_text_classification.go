//go:build huggingbento

package huggingface

import (
	"github.com/daulet/tokenizers"
	"github.com/knights-analytics/hugot"
	"github.com/knights-analytics/hugot/pipelines"

	"github.com/warpstreamlabs/bento/public/service"
)

func hugotTextClassificationConfigSpec() *service.ConfigSpec {
	textClassificationDescription := "### Text Classification" + "\n" +
		"Text Classification is the task of assigning a label or class to a given text." +
		"Some use cases are sentiment analysis, natural language inference, and assessing grammatical correctness." + "\n" +
		"This processor runs text-classification inference against batches of text data, returning labelled classification corresponding to each input." + "\n" +
		description

	spec := hugotConfigSpec().
		Summary("Performs text classification using a Hugging Face 🤗 NLP pipeline with an ONNX Runtime model.").
		Description(textClassificationDescription).
		Field(service.NewStringEnumField("aggregation_function",
			"SOFTMAX",
			"SIGMOID",
		).Description("The aggregation function to use for the text classification pipeline.").Default("SOFTMAX")).
		Field(service.NewStringEnumField("problem_type",
			"singleLabel",
			"multiLabel",
		).Description("The problem type for the text classification pipeline.").Default("singleLabel")).
		Example("Emotion Scoring (Local Model)", "Here, we load the [Cohee/distilbert-base-uncased-go-emotions-onnx](https://huggingface.co/Cohee/distilbert-base-uncased-go-emotions-onnx) model from the local directory at `models/coheedistilbert_base_uncased_go_emotions_onnx`."+
			"The processor returns a single-label output with the highest emotion score for the text. ",
			`
pipeline:
  processors:
    - nlp_classify_text:
        pipeline_name: classify-incoming-data
        model_path: "models/coheedistilbert_base_uncased_go_emotions_onnx"

# In: "I'm super excited for my Bento box!"
# Out: [{"Label":"excitement","Score":0.34134513}]
`).Example("Sentiment Analysis (Downloaded Model)", "Here, we retrieve the [KnightsAnalytics/distilbert-base-uncased-finetuned-sst-2-english(https://huggingface.co/KnightsAnalytics/distilbert-base-uncased-finetuned-sst-2-english) model from HuggingFace and store it in a `./models` directory."+
		"The processor returns a multi-label output indicating showing a `POSITIVE` and `NEGATIVE` score some input text-data.",
		`
pipeline:
  processors:
    - nlp_classify_text:
        pipeline_name: classify-multi-label
        model_path: "./models"
        enable_model_download: true
        model_download_options:
          model_repository: "KnightsAnalytics/distilbert-base-uncased-finetuned-sst-2-english"


# In: "This meal tastes like old boots."
# Out: [{"Label":"NEGATIVE","Score":0.9977291},{"Label":"POSITIVE","Score":0.0022708932}]
`)

	return spec
}

func init() {
	err := service.RegisterBatchProcessor("nlp_classify_text", hugotTextClassificationConfigSpec(), newTextClassificationPipeline)
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

func getTextClassificationOptions(conf *service.ParsedConfig) ([]pipelines.PipelineOption[*pipelines.TextClassificationPipeline], error) {
	var options []pipelines.PipelineOption[*pipelines.TextClassificationPipeline]

	aggregationFunction, err := conf.FieldString("aggregation_function")
	if err != nil {
		return nil, err
	}
	switch aggregationFunction {
	case "SOFTMAX":
		options = append(options, pipelines.WithSoftmax())
	case "SIGMOID":
		options = append(options, pipelines.WithSigmoid())
	}

	problemType, err := conf.FieldString("problem_type")
	if err != nil {
		return nil, err
	}
	switch problemType {
	case "singleLabel":
		options = append(options, pipelines.WithSingleLabel())
	case "multiLabel":
		options = append(options, pipelines.WithMultiLabel())
	}

	return options, nil
}

//------------------------------------------------------------------------------

func newTextClassificationPipeline(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	p, err := newPipelineProcessor(conf, mgr)
	if err != nil {
		return nil, err
	}

	opts, err := getTextClassificationOptions(conf)
	if err != nil {
		return nil, err
	}

	cfg := hugot.TextClassificationConfig{
		Name:         p.pipelineName,
		OnnxFilename: p.onnxFilename,
		ModelPath:    p.modelPath,
		Options:      opts,
	}

	pipeline, err := hugot.NewPipeline(p.session, cfg)
	if err != nil {
		return nil, err
	}

	pipeline.TokenizerOptions = []tokenizers.EncodeOption{
		tokenizers.WithReturnAttentionMask(),
		tokenizers.WithReturnTypeIDs(),
	}

	p.pipeline = pipeline

	if err := p.pipeline.Validate(); err != nil {
		return nil, err
	}

	if md := p.pipeline.GetMetadata().OutputsInfo; len(md) > 0 {
		p.metaOutputType = md[0].Name
		p.metaOutputLayerDim = md[0].Dimensions
	}

	return p, nil
}
