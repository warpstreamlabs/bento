package huggingface

import (
	"github.com/knights-analytics/hugot"
	"github.com/knights-analytics/hugot/pipelineBackends"
	"github.com/knights-analytics/hugot/pipelines"

	"github.com/warpstreamlabs/bento/public/service"
)

func HugotTextClassificationConfigSpec() *service.ConfigSpec {
	textClassificationDescription := "### Text Classification" + "\n" +
		"Text Classification is the task of assigning a label or class to a given text. " +
		"Some use cases are sentiment analysis, natural language inference, and assessing grammatical correctness." + "\n" +
		"This processor runs text-classification inference against batches of text data, returning labelled classification corresponding to each input." + "\n" +
		description

	spec := hugotConfigSpec().
		Summary("Performs text classification using a Hugging Face 🤗 NLP pipeline with an ONNX Runtime model.").
		Description(textClassificationDescription).
		Field(service.NewStringEnumField("aggregation_function", "SOFTMAX", "SIGMOID").
			Description("The aggregation function to use for the text classification pipeline.").Default("SOFTMAX")).
		Field(service.NewBoolField("multi_label").Description("Whether a text classification pipeline should return multiple labels. If false, only the label-pair with the highest score is returned.").Default(false)).
		Example("Emotion Scoring (Local Model)", "Here, we load the [Cohee/distilbert-base-uncased-go-emotions-onnx](https://huggingface.co/Cohee/distilbert-base-uncased-go-emotions-onnx) model from the local directory at `models/coheedistilbert_base_uncased_go_emotions_onnx`."+
			"The processor returns a single-label output with the highest emotion score for the text. ",
			`
pipeline:
  processors:
    - nlp_classify_text:
        name: classify-incoming-data
        path: "models/coheedistilbert_base_uncased_go_emotions_onnx"

# In: "I'm super excited for my Bento box!"
# Out: [{"Label":"excitement","Score":0.34134513}]
`).Example("Sentiment Analysis (Downloaded Model)", "Here, we retrieve the [KnightsAnalytics/distilbert-base-uncased-finetuned-sst-2-english](https://huggingface.co/KnightsAnalytics/distilbert-base-uncased-finetuned-sst-2-english) model from HuggingFace and store it in a `./models` directory."+
		"The processor returns a multi-label output indicating showing a `POSITIVE` and `NEGATIVE` score some input text-data.",
		`
pipeline:
  processors:
    - nlp_classify_text:
        name: classify-multi-label
        path: "./models"
        multi_label: true
        enable_download: true
        download_options:
          repository: "KnightsAnalytics/distilbert-base-uncased-finetuned-sst-2-english"


# In: "This meal tastes like old boots."
# Out: [{"Label":"NEGATIVE","Score":0.9977291},{"Label":"POSITIVE","Score":0.0022708932}]
`)

	return spec
}

func init() {
	err := service.RegisterBatchProcessor("nlp_classify_text", HugotTextClassificationConfigSpec(), NewTextClassificationPipeline)
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

func getTextClassificationOptions(conf *service.ParsedConfig) ([]pipelineBackends.PipelineOption[*pipelines.TextClassificationPipeline], error) {
	var options []pipelineBackends.PipelineOption[*pipelines.TextClassificationPipeline]

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

	isMultiLabel, err := conf.FieldBool("multi_label")
	if err != nil {
		return nil, err
	}

	if isMultiLabel {
		options = append(options, pipelines.WithMultiLabel())
	} else {
		options = append(options, pipelines.WithSingleLabel())
	}

	return options, nil
}

//------------------------------------------------------------------------------

func NewTextClassificationPipeline(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	p, err := newPipelineProcessor(conf, mgr)
	if err != nil {
		return nil, err
	}

	opts, err := getTextClassificationOptions(conf)
	if err != nil {
		return nil, err
	}

	cfg := hugot.TextClassificationConfig{
		Name:      p.pipelineName,
		ModelPath: p.modelPath,
		Options:   opts,
	}

	p.pipeline, err = hugot.NewPipeline(p.session, cfg)
	if err != nil {
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

func convertTextClassificationOutput(result *pipelines.TextClassificationOutput) []any {
	out := make([]any, len(result.ClassificationOutputs))
	for i, classificationBatch := range result.ClassificationOutputs {
		batchMaps := make([]any, len(classificationBatch))
		for j, classification := range classificationBatch {
			batchMaps[j] = map[string]any{
				"Label": classification.Label,
				"Score": classification.Score,
			}
		}
		out[i] = batchMaps
	}
	return out
}
