package huggingface

import (
	"context"
	"fmt"
	"time"

	"github.com/knights-analytics/hugot"
	"github.com/knights-analytics/hugot/pipelineBackends"
	"github.com/knights-analytics/hugot/pipelines"

	"github.com/warpstreamlabs/bento/public/service"
)

var description = `This component uses [Hugot](https://github.com/knights-analytics/hugot), a library that provides an interface for running [Open Neural Network Exchange (ONNX) models](https://onnx.ai/onnx/intro/) and transformer pipelines, with a focus on NLP tasks.

Currently, [Bento only implements](https://github.com/knights-analytics/hugot/tree/main?tab=readme-ov-file#implemented-pipelines):
	
- [featureExtraction](https://huggingface.co/docs/transformers/en/main_classes/pipelines#transformers.FeatureExtractionPipeline)
- [textClassification](https://huggingface.co/docs/transformers/en/main_classes/pipelines#transformers.TextClassificationPipeline)
- [tokenClassification](https://huggingface.co/docs/transformers/en/main_classes/pipelines#transformers.TokenClassificationPipeline)
- [zeroShotClassification](https://huggingface.co/docs/transformers/en/main_classes/pipelines#transformers.ZeroShotClassificationPipeline)

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
		Version("v1.11.0").
		Fields(hugotConfigFields()...)

	return spec
}

func hugotConfigFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField("name").
			Description("Name of the hugot pipeline. Defaults to a random UUID if not set.").
			Optional(),
		service.NewStringField("path").
			Description("Path to the ONNX model file, or directory containing the model. When downloading (`enable_download: true`), this becomes the destination and must be a directory.").
			Examples(
				"/path/to/models/my_model.onnx",
				"/path/to/models/",
			),
		service.NewBoolField("enable_download").
			Description("When enabled, attempts to download an ONNX Runtime compatible model from HuggingFace specified in `repository`.").
			Default(false).
			Advanced(),
		service.NewObjectField("download_options",
			service.NewStringField("repository").
				Description("The name of the huggingface model repository.").
				Examples(
					"KnightsAnalytics/distilbert-NER",
					"KnightsAnalytics/distilbert-base-uncased-finetuned-sst-2-english",
					"sentence-transformers/all-MiniLM-L6-v2",
				),
			service.NewStringField("onnx_filepath").
				Description("Filepath of the ONNX model within the repository. Only needed when multiple `.onnx` files exist.").
				Examples(
					"onnx/model.onnx",
					"onnx/model_quantized.onnx",
					"onnx/model_fp16.onnx",
				).
				Default("model.onnx").
				Advanced(),
		).Description(`Options used to download a model directly from HuggingFace. Before the model is downloaded, validation occurs to ensure the remote repository contains both an` + "`.onnx` and " + "`tokenizers.json` file.").
			Optional().
			Advanced(),
	}
}

//------------------------------------------------------------------------------

type sessionConstructorKey struct{}

type pipelineProcessor struct {
	log *service.Logger

	session  *hugot.Session
	pipeline pipelineBackends.Pipeline

	pipelineName string
	modelPath    string

	metaOutputType     string
	metaOutputLayerDim []int64
}

func newPipelineProcessor(conf *service.ParsedConfig, mgr *service.Resources) (*pipelineProcessor, error) {
	p := &pipelineProcessor{log: mgr.Logger()}

	var modelRepo, modelPath string
	var shouldDownload bool

	var err error

	// To prevent multiple sessions being created at once, rather store the constructor and allow it
	// to be atomically retrieved.
	ctorValue, _ := mgr.GetOrSetGeneric(sessionConstructorKey{}, func() (*hugot.Session, error) {
		return hugot.NewGoSession()
	})

	sessCtor, ok := ctorValue.(func() (*hugot.Session, error))
	if !ok {
		return nil, fmt.Errorf("failed to create session: %w", err)
	}

	p.session, err = sessCtor()
	if err != nil {
		return nil, fmt.Errorf("failed to create session: %w", err)
	}

	if p.modelPath, err = conf.FieldString("path"); err != nil {
		return nil, err
	}

	if p.pipelineName, err = conf.FieldString("name"); err != nil {
		return nil, err
	}

	if shouldDownload, err = conf.FieldBool("enable_download"); err != nil {
		return nil, err
	}

	if shouldDownload {
		opts := hugot.NewDownloadOptions()
		opts.Verbose = false

		if modelRepo, err = conf.FieldString("download_options", "repository"); err != nil {
			return nil, err
		}

		if opts.OnnxFilePath, err = conf.FieldString("download_options", "onnx_filepath"); err != nil {
			return nil, err
		}

		start := time.Now()
		if p.modelPath, err = hugot.DownloadModel(modelRepo, p.modelPath, opts); err != nil {
			return nil, fmt.Errorf("failed to download model %s from HuggingFace to %s: %w", modelRepo, modelPath, err)
		}

		p.log.With("repository", modelRepo).Infof("Completed download (took %d ms)", time.Since(start).Milliseconds())

	}

	return p, nil
}

//------------------------------------------------------------------------------

func (p *pipelineProcessor) ProcessBatch(ctx context.Context, b service.MessageBatch) ([]service.MessageBatch, error) {
	messages := make([]string, 0, len(b))
	validIndices := make([]int, 0, len(b))

	batchCopy := b.Copy()

	for i, msg := range batchCopy {
		msgBytes, err := msg.AsBytes()
		if err != nil {
			msg.SetError(err)
			continue
		}
		messages = append(messages, string(msgBytes))
		validIndices = append(validIndices, i)
	}

	if len(messages) == 0 {
		return []service.MessageBatch{batchCopy}, nil
	}

	results, err := p.pipeline.Run(messages)
	if err != nil {
		p.log.Errorf("Failed to process input against model: %v", err)
		for _, idx := range validIndices {
			if batchCopy[idx].GetError() == nil {
				batchCopy[idx].SetError(err)
			}
		}
		return []service.MessageBatch{batchCopy}, nil
	}

	resultsOut := convertPipelineOutput(results)
	for resultIdx, msgIdx := range validIndices {
		msg := batchCopy[msgIdx]
		if msg.GetError() != nil {
			continue
		}

		if resultIdx >= len(resultsOut) {
			msg.SetError(fmt.Errorf("result index %d out of range", resultIdx))
			continue
		}

		msg.SetStructuredMut(resultsOut[resultIdx])
		msg.MetaSetMut("pipeline_name", p.pipelineName)
		msg.MetaSetMut("output_type", p.metaOutputType)
		msg.MetaSetMut("output_shape", p.metaOutputLayerDim)
	}

	return []service.MessageBatch{batchCopy}, nil
}

func (p *pipelineProcessor) Close(context.Context) error {
	return p.pipeline.GetModel().Destroy()
}

func convertPipelineOutput(output pipelineBackends.PipelineBatchOutput) []any {
	switch result := output.(type) {
	case *pipelines.FeatureExtractionOutput:
		return result.GetOutput()
	case *pipelines.TextClassificationOutput:
		return convertTextClassificationOutput(result)
	case *pipelines.TokenClassificationOutput:
		return convertTokenClassificationOutput(result)
	case *pipelines.ZeroShotOutput:
		return convertZeroShotTextClassificationOutput(result)
	default:
		return output.GetOutput()
	}
}
