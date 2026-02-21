package huggingface

import (
	"github.com/knights-analytics/hugot"
	"github.com/knights-analytics/hugot/pipelineBackends"
	"github.com/knights-analytics/hugot/pipelines"

	"github.com/warpstreamlabs/bento/public/service"
)

func HugotZeroShotTextClassificationConfigSpec() *service.ConfigSpec {
	textClassificationDescription := "### Zero Shot Text Classification" + "\n" +
		"Zero-shot text classification allows you to classify text into any labels without training on those specific labels. " +
		"It uses Natural Language Inference (NLI) models to determine if a text entails each candidate label. " +
		"This is more flexible than regular text classification as labels can be chosen at runtime, but may be slower." + "\n" +
		"Common use cases include sentiment analysis, topic classification, intent detection, and content moderation." + "\n" +
		description

	spec := hugotConfigSpec().
		Summary("Performs zero-shot text classification using a Hugging Face 🤗 NLP pipeline with an ONNX Runtime model.").
		Description(textClassificationDescription).
		Fields(
			service.NewStringListField("labels").Description("The set of possible class labels to classify each sequence into.").Example([]string{"positive", "negative", "neutral"}),
			service.NewBoolField("multi_label").Description("Whether multiple labels can be true. If false, scores sum to 1. If true, each label is scored independently.").Default(false),
			service.NewStringField("hypothesis_template").Description("Template to turn each label into an NLI-style hypothesis. Must include {} where the label will be inserted.").Default("This example is {}."),
		).Example("Emotion Classification", "Classify text emotions using zero-shot approach with any custom labels.",
		`
pipeline:
  processors:
    - nlp_zero_shot_classify:
        path: "KnightsAnalytics/deberta-v3-base-zeroshot-v1"
        labels: ["fun", "dangerous", "boring"]
        multi_label: false
# In: "I am going to the park"
# Out: {"sequence": "I am going to the park", "labels": ["fun", "boring", "dangerous"], "scores": [0.77, 0.15, 0.08]}`).
		Example("Multi-Label State Classification", "Classify emotional states with multiple labels enabled.",
			`
pipeline:
  processors:
    - nlp_zero_shot_classify:
        path: "KnightsAnalytics/deberta-v3-base-zeroshot-v1"
        labels: ["busy", "relaxed", "stressed"]
        multi_label: true
        hypothesis_template: "This person is {}."
# In: "Please don't bother me, I'm in a rush"
# Out: {"sequence": "Please don't bother me, I'm in a rush", "labels": ["stressed", "busy", "relaxed"], "scores": [0.89, 0.11, 0.007]}
`)

	return spec
}

func init() {
	err := service.RegisterBatchProcessor("nlp_zero_shot_classify", HugotZeroShotTextClassificationConfigSpec(), NewZeroShotTextClassificationPipeline)
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

func getZeroShotTextClassificationOptions(conf *service.ParsedConfig) ([]pipelineBackends.PipelineOption[*pipelines.ZeroShotClassificationPipeline], error) {
	var options []pipelineBackends.PipelineOption[*pipelines.ZeroShotClassificationPipeline]

	if conf.Contains("hypothesis_template") {
		hypothesisTemplate, err := conf.FieldString("hypothesis_template")
		if err != nil {
			return nil, err
		}
		options = append(options, pipelines.WithHypothesisTemplate(hypothesisTemplate))
	}

	candidateLabels, err := conf.FieldStringList("labels")
	if err != nil {
		return nil, err
	}
	options = append(options, pipelines.WithLabels(candidateLabels))

	multiLabel, err := conf.FieldBool("multi_label")
	if err != nil {
		return nil, err
	}
	options = append(options, pipelines.WithMultilabel(multiLabel))

	return options, nil
}

//------------------------------------------------------------------------------

func NewZeroShotTextClassificationPipeline(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	p, err := newPipelineProcessor(conf, mgr)
	if err != nil {
		return nil, err
	}

	opts, err := getZeroShotTextClassificationOptions(conf)
	if err != nil {
		return nil, err
	}

	cfg := hugot.ZeroShotClassificationConfig{
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

func convertZeroShotTextClassificationOutput(result *pipelines.ZeroShotOutput) []any {
	out := make([]any, len(result.ClassificationOutputs))
	for i, classificationBatch := range result.ClassificationOutputs {
		batchMaps := make([]any, len(classificationBatch.SortedValues))
		for j, classification := range classificationBatch.SortedValues {
			batchMaps[j] = map[string]any{
				"Key":   classification.Key,
				"Value": classification.Value,
			}
		}
		out[i] = map[string]any{
			"Sequence":     classificationBatch.Sequence,
			"SortedValues": batchMaps,
		}
	}
	return out
}
