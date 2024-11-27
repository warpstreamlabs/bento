//go:build GO

package huggingface

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/knights-analytics/hugot"
	"github.com/knights-analytics/hugot/pipelines"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "github.com/warpstreamlabs/bento/public/components/io"
	_ "github.com/warpstreamlabs/bento/public/components/pure"
	"github.com/warpstreamlabs/bento/public/service"
)

//go:embed testdata/expected_token_classification.json
var tokenClassificationExpectedByte []byte

//go:embed testdata/expected_feature_extraction.json
var featureExtractionExpectedByte []byte

var (
	onnxRuntimeSession *hugot.Session
	onnxLibPath        string
)

func setup(t *testing.T) {
	var (
		err error
	)

	onnxRuntimeSession, err = globalSession.NewSession()
	require.NoError(t, err)
	require.NotNil(t, onnxRuntimeSession)
}

func TestIntegration_TextClassifier(t *testing.T) {
	setup(t)

	tmpDir := t.TempDir()

	modelName := "KnightsAnalytics/distilbert-base-uncased-finetuned-sst-2-english"
	modelPath, err := onnxRuntimeSession.DownloadModel(modelName, tmpDir, hugot.NewDownloadOptions())
	require.NoError(t, err)
	t.Logf("downloading to %v", modelPath)
	defer t.Cleanup(func() {
		assert.NoError(t, os.RemoveAll(tmpDir))
	})

	template := fmt.Sprintf(`
nlp_classify_text:
  pipeline_name: classify-incoming-data-1
  onnx_library_path: %s
  model_path: %s
`, onnxLibPath, modelPath)

	b := service.NewStreamBuilder()
	require.NoError(t, b.SetLoggerYAML("level: INFO"))
	require.NoError(t, b.AddProcessorYAML(template))

	outBatches := map[string]struct{}{}
	var outMut sync.Mutex
	handler := func(_ context.Context, mb service.MessageBatch) error {
		outMut.Lock()
		defer outMut.Unlock()

		outMsgs := []string{}
		for _, m := range mb {
			b, err := m.AsBytes()
			assert.NoError(t, err)
			outMsgs = append(outMsgs, string(b))
		}

		outBatches[strings.Join(outMsgs, ",")] = struct{}{}
		return nil
	}
	require.NoError(t, b.AddBatchConsumerFunc(handler))

	pushFn, err := b.AddBatchProducerFunc()

	strm, err := b.Build()
	require.NoError(t, err)

	promptsBatch := [][]string{
		{"Bento boxes taste amazing!", "Meow meow meow... meow meow."},
		{"Why does the blobfish look so sad? :(", "Sir, are you aware of the magnificent octopus on your head?"},
		{"Streaming data is my favourite pastime.", "You are wearing a silly hat."},
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		ctx, done := context.WithTimeout(context.Background(), time.Second*60)
		defer done()

		for _, prompts := range promptsBatch {
			batch := make(service.MessageBatch, len(prompts))
			for i, prompt := range prompts {
				batch[i] = service.NewMessage([]byte(prompt))
			}
			require.NoError(t, pushFn(ctx, batch))
		}

		require.NoError(t, strm.StopWithin(time.Second*30))
	}()

	require.NoError(t, strm.Run(context.Background()))
	wg.Wait()

	outMut.Lock()
	assert.Equal(t, map[string]struct{}{
		`[{"Label":"POSITIVE","Score":0.999869}],[{"Label":"POSITIVE","Score":0.9992634}]`:  {},
		`[{"Label":"NEGATIVE","Score":0.9996588}],[{"Label":"POSITIVE","Score":0.9908547}]`: {},
		`[{"Label":"POSITIVE","Score":0.9811118}],[{"Label":"NEGATIVE","Score":0.9700846}]`: {},
	}, outBatches)
	outMut.Unlock()

}

func TestIntegration_TokenClassifier(t *testing.T) {
	setup(t)

	tmpDir := t.TempDir()

	modelName := "KnightsAnalytics/distilbert-NER"
	modelPath, err := onnxRuntimeSession.DownloadModel(modelName, tmpDir, hugot.NewDownloadOptions())
	require.NoError(t, err)

	defer t.Cleanup(func() {
		assert.NoError(t, os.RemoveAll(tmpDir))
	})

	template := fmt.Sprintf(`
nlp_classify_tokens:
  pipeline_name: classify-tokens
  onnx_library_path: %s
  model_path: %s
`, onnxLibPath, modelPath)

	b := service.NewStreamBuilder()
	require.NoError(t, b.SetLoggerYAML("level: INFO"))
	require.NoError(t, b.AddProcessorYAML(template))

	var outBatches [][]string
	var outMut sync.Mutex
	handler := func(_ context.Context, mb service.MessageBatch) error {
		outMut.Lock()
		defer outMut.Unlock()

		outMsgs := []string{}
		for _, m := range mb {
			b, err := m.AsBytes()
			assert.NoError(t, err)
			outMsgs = append(outMsgs, string(b))
		}
		outBatches = append(outBatches, outMsgs)
		return nil
	}
	require.NoError(t, b.AddBatchConsumerFunc(handler))

	pushFn, err := b.AddBatchProducerFunc()

	strm, err := b.Build()
	require.NoError(t, err)

	promptsBatch := [][]string{
		{"Japanese Bento boxes taste amazing!", "My name is Wolfgang and I live in Berlin."},
		{"WarpStream Labs have a great offering!", "An Italian man went to Malta..."},
		{"NVIDIA corporation was valued higher than Apple!?", "My silly hat is from Hatfield."},
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		ctx, done := context.WithTimeout(context.Background(), time.Second*60)
		defer done()

		for _, prompts := range promptsBatch {
			batch := make(service.MessageBatch, len(prompts))
			for i, prompt := range prompts {
				batch[i] = service.NewMessage([]byte(prompt))
			}
			require.NoError(t, pushFn(ctx, batch))
		}

		require.NoError(t, strm.StopWithin(time.Second*30))
	}()

	require.NoError(t, strm.Run(context.Background()))
	wg.Wait()

	type Tokens struct {
		Entities []pipelines.Entity `json:"entities"`
	}

	var expectedResults map[string]Tokens
	err = json.Unmarshal(tokenClassificationExpectedByte, &expectedResults)
	require.NoError(t, err)

	outMut.Lock()
	for batch, prompts := range promptsBatch {
		for msg, prompt := range prompts {
			var actualEntity []pipelines.Entity
			err = json.Unmarshal([]byte(outBatches[batch][msg]), &actualEntity)
			require.NoError(t, err)
			assert.Equal(t, expectedResults[prompt].Entities, actualEntity)
		}

	}
	outMut.Unlock()
}

func TestIntegration_FeatureExtractor(t *testing.T) {
	setup(t)

	tmpDir := t.TempDir()

	modelName := "sentence-transformers/all-MiniLM-L6-v2"

	modelPath, err := onnxRuntimeSession.DownloadModel(modelName, tmpDir, hugot.NewDownloadOptions())
	require.NoError(t, err)

	defer t.Cleanup(func() {
		assert.NoError(t, os.RemoveAll(tmpDir))
	})

	template := fmt.Sprintf(`
nlp_extract_features:
  pipeline_name: classify-incoming-data-1
  onnx_library_path: %s
  model_path: %s
`, onnxLibPath, modelPath)

	b := service.NewStreamBuilder()
	require.NoError(t, b.SetLoggerYAML("level: INFO"))
	require.NoError(t, b.AddProcessorYAML(template))

	var outBatches [][]string
	var outMut sync.Mutex
	handler := func(_ context.Context, mb service.MessageBatch) error {
		outMut.Lock()
		defer outMut.Unlock()

		outMsgs := []string{}
		for _, m := range mb {
			b, err := m.AsBytes()
			assert.NoError(t, err)
			outMsgs = append(outMsgs, string(b))
		}
		outBatches = append(outBatches, outMsgs)
		return nil
	}
	require.NoError(t, b.AddBatchConsumerFunc(handler))

	pushFn, err := b.AddBatchProducerFunc()

	strm, err := b.Build()
	require.NoError(t, err)

	promptsBatch := [][]string{
		{"Bento boxes taste amazing!", "Meow meow meow... meow meow."},
		{"Streaming data is my favourite pastime.", "You are wearing a silly hat."},
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		ctx, done := context.WithTimeout(context.Background(), time.Second*60)
		defer done()

		for _, prompts := range promptsBatch {
			batch := make(service.MessageBatch, len(prompts))
			for i, prompt := range prompts {
				batch[i] = service.NewMessage([]byte(prompt))
			}
			require.NoError(t, pushFn(ctx, batch))
		}

		require.NoError(t, strm.StopWithin(time.Second*30))
	}()

	require.NoError(t, strm.Run(context.Background()))
	wg.Wait()

	var expectedResults map[string][]float64
	err = json.Unmarshal(featureExtractionExpectedByte, &expectedResults)
	require.NoError(t, err)

	outMut.Lock()
	for batch, prompts := range promptsBatch {
		for msg, prompt := range prompts {
			var actualEntity []float64
			err = json.Unmarshal([]byte(outBatches[batch][msg]), &actualEntity)
			require.NoError(t, err)
			assert.Equal(t, expectedResults[prompt], actualEntity)
		}

	}
	outMut.Unlock()

}

func TestIntegration_TextClassifier_Download(t *testing.T) {
	setup(t)

	tmpDir := t.TempDir()

	modelName := "KnightsAnalytics/distilbert-base-uncased-finetuned-sst-2-english"

	defer t.Cleanup(func() {
		assert.NoError(t, os.RemoveAll(tmpDir))
	})

	template := fmt.Sprintf(`
nlp_classify_text:
  pipeline_name: classify-incoming-data-1
  onnx_library_path: %s
  model_path: %s
  enable_model_download: true
  model_download_options:
    model_repository: %s
`, onnxLibPath, tmpDir, modelName)

	b := service.NewStreamBuilder()
	require.NoError(t, b.SetLoggerYAML("level: DEBUG"))
	require.NoError(t, b.AddProcessorYAML(template))

	outBatches := map[string]struct{}{}
	var outMut sync.Mutex
	handler := func(_ context.Context, mb service.MessageBatch) error {
		outMut.Lock()
		defer outMut.Unlock()

		outMsgs := []string{}
		for _, m := range mb {
			b, err := m.AsBytes()
			assert.NoError(t, err)
			outMsgs = append(outMsgs, string(b))
		}

		outBatches[strings.Join(outMsgs, ",")] = struct{}{}
		return nil
	}
	require.NoError(t, b.AddBatchConsumerFunc(handler))

	pushFn, err := b.AddBatchProducerFunc()

	strm, err := b.Build()
	require.NoError(t, err)

	promptsBatch := [][]string{
		{"Bento boxes taste amazing!", "Meow meow meow... meow meow."},
		{"Why does the blobfish look so sad? :(", "Sir, are you aware of the magnificent octopus on your head?"},
		{"Streaming data is my favourite pastime.", "You are wearing a silly hat."},
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(60 * time.Second)
		ctx, done := context.WithTimeout(context.Background(), time.Second*60)
		defer done()

		for _, prompts := range promptsBatch {
			batch := make(service.MessageBatch, len(prompts))
			for i, prompt := range prompts {
				batch[i] = service.NewMessage([]byte(prompt))
			}
			require.NoError(t, pushFn(ctx, batch))
		}

		require.NoError(t, strm.StopWithin(time.Second*30))
	}()

	require.NoError(t, strm.Run(context.Background()))

	wg.Wait()

	outMut.Lock()
	assert.Equal(t, map[string]struct{}{
		`[{"Label":"POSITIVE","Score":0.999869}],[{"Label":"POSITIVE","Score":0.9992634}]`:  {},
		`[{"Label":"NEGATIVE","Score":0.9996588}],[{"Label":"POSITIVE","Score":0.9908547}]`: {},
		`[{"Label":"POSITIVE","Score":0.9811118}],[{"Label":"NEGATIVE","Score":0.9700846}]`: {},
	}, outBatches)
	outMut.Unlock()

}
