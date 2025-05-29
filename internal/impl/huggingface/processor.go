package huggingface

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/knights-analytics/hugot"
	"github.com/knights-analytics/hugot/options"
	"github.com/knights-analytics/hugot/pipelineBackends"

	"github.com/warpstreamlabs/bento/public/service"
)

var description = `This component uses [Hugot](https://github.com/knights-analytics/hugot), a library that provides an interface for running [Open Neural Network Exchange (ONNX) models](https://onnx.ai/onnx/intro/) and transformer pipelines, with a focus on NLP tasks.

Currently, [HuggingBento only implements](https://github.com/knights-analytics/hugot/tree/main?tab=readme-ov-file#implemented-pipelines):
	
- [featureExtraction](https://huggingface.co/docs/transformers/en/main_classes/pipelines#transformers.FeatureExtractionPipeline)
- [textClassification](https://huggingface.co/docs/transformers/en/main_classes/pipelines#transformers.TextClassificationPipeline)
- [tokenClassification](https://huggingface.co/docs/transformers/en/main_classes/pipelines#transformers.TokenClassificationPipeline)

### What is a pipeline?
From [HuggingFace docs](https://huggingface.co/docs/transformers/en/main_classes/pipelines):
> A pipeline in 🤗 Transformers is an abstraction referring to a series of steps that are executed in a specific order to preprocess and transform data and return a prediction from a model. Some example stages found in a pipeline might be data preprocessing, feature extraction, and normalization.

:::warning
While, only models in [ONNX](https://onnx.ai/) format are supported, exporting existing formats to ONNX is both possible and straightforward in most standard ML libraries. For more on this, check out the [ONNX conversion docs](https://onnx.ai/onnx/intro/converters.html). 
Otherwise, check out using [HuggingFace Optimum](https://huggingface.co/docs/optimum/en/exporters/onnx/usage_guides/export_a_model) for easy model conversion.
:::
`

func hugotConfigSpec() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Beta().
		Categories("Machine Learning", "NLP").
		Version("v1.3.0 (huggingbento)").
		Fields(hugotConfigFields()...)

	return spec
}

func hugotConfigFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField("pipeline_name").
			Description("Name of the pipeline. Defaults to uuid_v4() if not set").
			Optional(),
		service.NewStringField("model_path").
			Description("Path to the ONNX model directory. If `enable_model_download` is `true`, the model will be downloaded here.").
			Example("/path/to/models/my_model.onnx").
			Default("/model_repository"),
		service.NewStringField("onnx_library_path").
			Description("The location of the ONNX Runtime dynamic library.").
			Default("/usr/lib/onnxruntime.so").
			Advanced(),
		service.NewStringField("onnx_filename").
			Description("The filename of the model to run. Only necessary to specify when multiple .onnx files are present.").
			Example("model.onnx").
			Default("").
			Advanced(),
		service.NewBoolField("enable_model_download").
			Description("If enabled, attempts to download an ONNX Runtime compatible model from HuggingFace specified in `model_name`.").
			Default(false).
			Advanced(),
		service.NewObjectField("model_download_options",
			service.NewStringField("model_repository").
				Description("The name of the huggingface model repository.").
				Examples(
					"KnightsAnalytics/distilbert-NER",
					"KnightsAnalytics/distilbert-base-uncased-finetuned-sst-2-english",
					"sentence-transformers/all-MiniLM-L6-v2",
				).
				Default("").
				Advanced(),
		),
	}
}

//------------------------------------------------------------------------------

type SessionConstructorKey struct{}

type SessionConstructor func() (*hugot.Session, error)

type pipelineProcessor struct {
	log *service.Logger

	session  *hugot.Session
	pipeline pipelineBackends.Pipeline

	pipelineName string
	modelPath    string
	modelName    string
	onnxFilename string

	metaOutputType     string
	metaOutputLayerDim []int64

	closeOnce sync.Once
}

func newPipelineProcessor(conf *service.ParsedConfig, mgr *service.Resources) (*pipelineProcessor, error) {
	p := &pipelineProcessor{log: mgr.Logger()}

	var modelRepo, modelPath, pipelineName string
	var onnxFileName string
	var shouldDownload bool

	var err error

	ctorValue, _ := mgr.GetOrSetGeneric(SessionConstructorKey{}, hugot.NewGoSession)
	sessCtor, ok := ctorValue.(func(opts ...options.WithOption) (*hugot.Session, error))
	if !ok {
		return nil, fmt.Errorf("failed to create session: %w", err)
	}

	p.session, err = sessCtor()
	if err != nil {
		return nil, fmt.Errorf("failed to create session: %w", err)
	}

	if modelPath, err = conf.FieldString("model_path"); err != nil {
		return nil, err
	}

	if pipelineName, err = conf.FieldString("pipeline_name"); err != nil {
		return nil, err
	}

	if onnxFileName, err = conf.FieldString("onnx_filename"); err != nil {
		return nil, err
	}

	if shouldDownload, err = conf.FieldBool("enable_model_download"); err != nil {
		return nil, err
	}

	if shouldDownload {
		opts := hugot.NewDownloadOptions()
		opts.Verbose = false

		if modelRepo, err = conf.FieldString("model_download_options", "model_repository"); err != nil {
			return nil, err
		}

		start := time.Now()
		if path, err := hugot.DownloadModel(modelRepo, modelPath, opts); err != nil {
			return nil, fmt.Errorf("failed to download model %s from HuggingFace to %s: %w", modelRepo, modelPath, err)
		} else {
			modelPath = path
		}
		p.log.With("model_repository", modelRepo).Infof("Completed download (took %d ms)", time.Since(start).Milliseconds())
	}

	p.onnxFilename = onnxFileName
	p.modelPath = modelPath
	p.pipelineName = pipelineName
	p.modelName = modelRepo

	return p, nil
}

//------------------------------------------------------------------------------

func (p *pipelineProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	messages := make([]string, len(batch))

	batch = batch.Copy()
	for i, msg := range batch {
		msgBytes, err := msg.AsBytes()
		if err != nil {
			msg.SetError(err)
			continue
		}
		messages[i] = string(msgBytes)
		p.log.Debug(string(msgBytes))
	}

	results, err := p.pipeline.Run(messages)
	if err != nil {
		return nil, err
	}

	resultsOut := results.GetOutput()
	for i, msg := range batch {
		if msg.GetError() != nil {
			continue
		}

		msg.SetStructuredMut(resultsOut[i])
		msg.MetaSetMut("pipeline_name", p.pipelineName)
		msg.MetaSetMut("output_type", p.metaOutputType)
		msg.MetaSetMut("output_shape", p.metaOutputLayerDim)
	}

	return []service.MessageBatch{batch}, nil
}

func (p *pipelineProcessor) Close(context.Context) error {
	return p.pipeline.GetModel().Destroy()
}
