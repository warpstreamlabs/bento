package huggingface_test

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/knights-analytics/hugot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/internal/impl/huggingface"
	_ "github.com/warpstreamlabs/bento/public/components/io"
	_ "github.com/warpstreamlabs/bento/public/components/pure"
	"github.com/warpstreamlabs/bento/public/service"
)

func compareResults(t *testing.T, expected, actual any) {
	t.Helper()

	// Normalize types by marshaling and unmarshaling...
	// HACK: This is a SUPER dirty hack but there are issues when converting to-and-from
	// JSON since all decimals numbers are treated as float64s
	expectedJSON, _ := json.Marshal(expected)
	actualJSON, _ := json.Marshal(actual)

	var expectedNorm, actualNorm any
	require.NoError(t, json.Unmarshal(expectedJSON, &expectedNorm))
	require.NoError(t, json.Unmarshal(actualJSON, &actualNorm))

	opts := cmp.Options{
		cmpopts.EquateApprox(0, 1e-6), // allows for floating point precision to differ between runs
	}

	if !cmp.Equal(expectedNorm, actualNorm, opts...) {
		t.Errorf("Results differ:\n%s", cmp.Diff(expectedNorm, actualNorm, opts...))
	}
}

func TestIntegration_TextClassifier(t *testing.T) {
	tests := []struct {
		name string

		snapshotId   string
		snapshotPath string
		problemType  string
	}{
		{

			name:         "single label test",
			snapshotId:   "test-snapshot-text-classifier",
			snapshotPath: "expected_text_classification.json",
			problemType:  "singleLabel",
		},
		{
			name:         "multi label test",
			snapshotId:   "test-snapshot-text-multi-classifier",
			snapshotPath: "expected_text_classification.json",
			problemType:  "multiLabel",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			snapshot := loadSnapshot(t, tt.snapshotId, tt.snapshotPath)

			tmpDir := t.TempDir()
			modelName := snapshot.Metadata.ModelName
			modelPath, err := hugot.DownloadModel(modelName, tmpDir, hugot.NewDownloadOptions())
			require.NoError(t, err)

			t.Logf("downloading to %v", modelPath)
			defer t.Cleanup(func() {
				assert.NoError(t, os.RemoveAll(tmpDir))
			})

			template := fmt.Sprintf(`
pipeline_name: classify-incoming-data
model_path: %s
problem_type: %s
`, modelPath, tt.problemType)

			conf, err := huggingface.HugotTextClassificationConfigSpec().ParseYAML(template, nil)
			require.NoError(t, err)

			proc, err := huggingface.NewTextClassificationPipeline(conf, service.MockResources())
			if err != nil {
				t.Fatal(err)
			}

			ctx, done := context.WithTimeout(context.Background(), time.Second*60)
			defer done()

			for _, expected := range snapshot.Results {
				input := service.NewMessage([]byte(expected.Input))

				batches, err := proc.ProcessBatch(ctx, []*service.Message{input})
				require.NoError(t, err)

				for _, batch := range batches {
					for _, msg := range batch {

						output, err := msg.AsStructured()
						require.NoError(t, err)

						compareResults(t, expected.Result, output)
					}
				}
			}
		})
	}

}

func TestIntegration_TokenClassifier(t *testing.T) {
	snapshot := loadSnapshot(t, "test-snapshot-token-classification", "expected_token_classification.json")

	tmpDir := t.TempDir()
	modelPath, err := hugot.DownloadModel(snapshot.Metadata.ModelName, tmpDir, hugot.NewDownloadOptions())
	require.NoError(t, err)

	template := fmt.Sprintf(`
pipeline_name: classify-tokens
model_path: %s
`, modelPath)

	conf, err := huggingface.HugotTokenClassificationConfigSpec().ParseYAML(template, nil)
	require.NoError(t, err)

	proc, err := huggingface.NewTokenClassificationPipeline(conf, service.MockResources())
	require.NoError(t, err)

	ctx, done := context.WithTimeout(context.Background(), time.Second*60)
	defer done()

	for _, expected := range snapshot.Results {
		input := service.NewMessage([]byte(expected.Input))

		batches, err := proc.ProcessBatch(ctx, []*service.Message{input})
		require.NoError(t, err)

		for _, batch := range batches {
			for _, msg := range batch {
				output, err := msg.AsStructured()
				require.NoError(t, err)
				compareResults(t, expected.Result, output)
			}
		}
	}
}

func TestIntegration_FeatureExtractor(t *testing.T) {
	snapshot := loadSnapshot(t, "test-snapshot-feature-extraction", "expected_feature_extraction.json")

	tmpDir := t.TempDir()
	opts := hugot.NewDownloadOptions()
	opts.OnnxFilePath = "onnx/model.onnx"
	modelPath, err := hugot.DownloadModel(snapshot.Metadata.ModelName, tmpDir, opts)
	require.NoError(t, err)

	template := fmt.Sprintf(`
pipeline_name: extract-features
model_path: %s
`, modelPath)

	conf, err := huggingface.HugotFeatureExtractionConfigSpec().ParseYAML(template, nil)
	require.NoError(t, err)

	proc, err := huggingface.NewFeatureExtractionPipeline(conf, service.MockResources())
	require.NoError(t, err)

	ctx, done := context.WithTimeout(context.Background(), time.Second*60)
	defer done()

	for _, expected := range snapshot.Results {
		input := service.NewMessage([]byte(expected.Input))

		batches, err := proc.ProcessBatch(ctx, []*service.Message{input})
		require.NoError(t, err)

		for _, batch := range batches {
			for _, msg := range batch {
				output, err := msg.AsStructured()
				require.NoError(t, err)
				compareResults(t, expected.Result, output)
			}
		}
	}
}
