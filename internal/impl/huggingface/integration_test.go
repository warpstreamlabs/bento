package huggingface_test

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
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
	"github.com/warpstreamlabs/bento/public/service/integration"
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
	integration.CheckSkip(t)

	tests := []struct {
		name string

		snapshotID   string
		snapshotPath string
		multiLabel   bool
	}{
		{

			name:         "single label test",
			snapshotID:   "test-snapshot-text-classifier",
			snapshotPath: "expected_text_classification.json",
			multiLabel:   false,
		},
		{
			name:         "multi label test",
			snapshotID:   "test-snapshot-text-multi-classifier",
			snapshotPath: "expected_text_classification.json",
			multiLabel:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			snapshot := loadSnapshot(t, tt.snapshotID, tt.snapshotPath)

			tmpDir := t.TempDir()
			modelName := snapshot.Metadata.ModelName
			modelPath, err := hugot.DownloadModel(modelName, tmpDir, hugot.NewDownloadOptions())
			require.NoError(t, err)

			t.Logf("downloading to %v", modelPath)
			defer t.Cleanup(func() {
				assert.NoError(t, os.RemoveAll(tmpDir))
			})

			template := fmt.Sprintf(`
name: classify-incoming-data
path: %s
multi_label: %s
`, modelPath, strconv.FormatBool(tt.multiLabel))

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
	integration.CheckSkip(t)

	snapshot := loadSnapshot(t, "test-snapshot-token-classification", "expected_token_classification.json")

	tmpDir := t.TempDir()
	modelPath, err := hugot.DownloadModel(snapshot.Metadata.ModelName, tmpDir, hugot.NewDownloadOptions())
	require.NoError(t, err)

	template := fmt.Sprintf(`
name: classify-tokens
path: %s
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
	integration.CheckSkip(t)

	snapshot := loadSnapshot(t, "test-snapshot-feature-extraction", "expected_feature_extraction.json")

	tmpDir := t.TempDir()
	opts := hugot.NewDownloadOptions()
	opts.OnnxFilePath = "onnx/model.onnx"
	modelPath, err := hugot.DownloadModel(snapshot.Metadata.ModelName, tmpDir, opts)
	require.NoError(t, err)

	template := fmt.Sprintf(`
name: extract-features
path: %s
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

func TestIntegration_ZeroShotTextClassifier(t *testing.T) {
	integration.CheckSkip(t)

	tests := []struct {
		name         string
		snapshotID   string
		snapshotPath string
		multiLabel   bool
		labels       []string
	}{
		{
			name:         "single label test",
			snapshotID:   "test-snapshot-zero-shot-classification",
			snapshotPath: "expected_zero_shot_classification.json",
			multiLabel:   false,
			labels:       []string{"spam"},
		},
		{
			name:         "multi label test",
			snapshotID:   "test-snapshot-zero-shot-classification-multi",
			snapshotPath: "expected_zero_shot_classification.json",
			multiLabel:   true,
			labels:       []string{"positive", "negative", "neutral"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snapshot := loadSnapshot(t, tt.snapshotID, tt.snapshotPath)
			tmpDir := t.TempDir()
			modelName := snapshot.Metadata.ModelName

			opts := hugot.NewDownloadOptions()
			opts.OnnxFilePath = "onnx/model.onnx"
			modelPath, err := hugot.DownloadModel(modelName, tmpDir, opts)
			require.NoError(t, err)

			defer t.Cleanup(func() {
				assert.NoError(t, os.RemoveAll(tmpDir))
			})

			template := fmt.Sprintf(`
name: zero-shot-classify
path: %s
labels: [%s]
multi_label: %s
hypothesis_template: "This example is {}."
`, modelPath, strings.Join(tt.labels, ","), strconv.FormatBool(tt.multiLabel))

			conf, err := huggingface.HugotZeroShotTextClassificationConfigSpec().ParseYAML(template, nil)
			require.NoError(t, err)

			proc, err := huggingface.NewZeroShotTextClassificationPipeline(conf, service.MockResources())
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

func TestIntegration_Download(t *testing.T) {
	integration.CheckSkip(t)

	snapshot := loadSnapshot(t, "test-snapshot-feature-extraction", "expected_feature_extraction.json")

	tmpDir := t.TempDir()
	template := fmt.Sprintf(`
name: extract-features
path: %s
enable_download: true
download_options:
  repository: %s
  onnx_filepath: onnx/model.onnx
`, tmpDir, snapshot.Metadata.ModelName)

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
