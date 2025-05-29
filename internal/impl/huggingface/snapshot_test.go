package huggingface_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/knights-analytics/hugot"
	"github.com/knights-analytics/hugot/pipelines"
	"github.com/stretchr/testify/require"
)

type SnapshotResult struct {
	Entities  []pipelines.Entity `json:"entities"`
	Timestamp time.Time          `json:"timestamp"`
	Version   string             `json:"version"`
}

type FeatureSnapshotResult struct {
	Embeddings []float64 `json:"embeddings"`
	Dimensions int       `json:"dimensions"`
	Timestamp  time.Time `json:"timestamp"`
}

type ClassificationSnapshotResult struct {
	Label     string    `json:"label"`
	Score     float64   `json:"score"`
	Timestamp time.Time `json:"timestamp"`
}

type SnapshotMetadata struct {
	CreatedAt time.Time `json:"created_at"`
	ModelName string    `json:"model_name"`
	GoVersion string    `json:"go_version"`
	Platform  string    `json:"platform"`
	TestName  string    `json:"test_name"`
}

//------------------------------------------------------------------------------

func isSnapshotMode() bool {
	return true
	val := os.Getenv("SNAPSHOT_UPDATE")
	switch strings.ToLower(val) {
	case "1", "true", "yes":
		return true
	}
	return false
}

//------------------------------------------------------------------------------

func TestSnapshot_CreateTokenClassificationData(t *testing.T) {
	if !isSnapshotMode() {
		t.Skip("Skipping snapshot creation (set SNAPSHOT_UPDATE=1)")
	}

	tmpDir := t.TempDir()
	modelName := "KnightsAnalytics/distilbert-NER"

	opts := hugot.NewDownloadOptions()
	opts.OnnxFilePath = "model.onnx"

	modelPath, err := hugot.DownloadModel(modelName, tmpDir, opts)
	require.NoError(t, err, "Failed to download model")
	t.Logf("Model downloaded to: %s", modelPath)

	session, err := hugot.NewGoSession()
	require.NoError(t, err, "Failed to create session")
	defer session.Destroy()

	config := hugot.TokenClassificationConfig{
		ModelPath: modelPath,
		Name:      "snapshot-token-classifier",
		Options: []hugot.TokenClassificationOption{
			pipelines.WithSimpleAggregation(),
			pipelines.WithIgnoreLabels([]string{"O"}),
		},
	}

	pipeline, err := hugot.NewPipeline(session, config)
	require.NoError(t, err, "Failed to create pipeline")

	testPrompts := []string{
		"Barack Obama visited New York City last week.",
		"Apple Inc. and Microsoft Corporation are competing.",
		"Dr. Martin Luther King Jr. spoke in Washington, D.C.",
		"The University of California is in Los Angeles.",
		"John works at Google with Mary Smith.",
	}

	snapshotData := make(map[string]SnapshotResult)

	for _, prompt := range testPrompts {
		result, err := pipeline.RunPipeline([]string{prompt})
		require.NoError(t, err, "Failed to run pipeline for prompt: %s", prompt)

		var allEntities []pipelines.Entity
		for _, entities := range result.Entities {
			allEntities = append(allEntities, entities...)
		}

		snapshotData[prompt] = SnapshotResult{
			Entities:  allEntities,
			Timestamp: time.Now(),
			Version:   "hugot-direct",
		}
	}

	saveSnapshot(t, "expected_token_classification.json", modelName, snapshotData)
}

func TestSnapshot_CreateFeatureExtractionData(t *testing.T) {
	if !isSnapshotMode() {
		t.Skip("Skipping snapshot creation (set SNAPSHOT_UPDATE=1)")
	}

	tmpDir := t.TempDir()
	modelName := "sentence-transformers/all-MiniLM-L6-v2"

	opts := hugot.NewDownloadOptions()
	opts.OnnxFilePath = "onnx/model.onnx"
	opts.ConcurrentConnections = 1

	modelPath, err := hugot.DownloadModel(modelName, tmpDir, opts)
	require.NoError(t, err, "Failed to download model")

	session, err := hugot.NewGoSession()
	require.NoError(t, err, "Failed to create session")
	defer session.Destroy()

	config := hugot.FeatureExtractionConfig{
		ModelPath:    modelPath,
		Name:         "snapshot-feature-extractor",
		OnnxFilename: "model.onnx",
	}

	pipeline, err := hugot.NewPipeline(session, config)
	require.NoError(t, err, "Failed to create pipeline")

	testPrompts := []string{
		"Bento boxes taste amazing!",
		"Streaming data is my favourite pastime.",
		"The weather is nice today.",
		"Machine learning models are powerful tools.",
	}

	snapshotData := make(map[string]FeatureSnapshotResult)

	for _, prompt := range testPrompts {
		result, err := pipeline.RunPipeline([]string{prompt})
		require.NoError(t, err, "Failed to run pipeline for prompt: %s", prompt)

		require.Greater(t, len(result.Embeddings), 0, "No embeddings returned")

		// Convert float32 to float64 for JSON compatibility
		embeddings := make([]float64, len(result.Embeddings[0]))
		for i, v := range result.Embeddings[0] {
			embeddings[i] = float64(v)
		}

		snapshotData[prompt] = FeatureSnapshotResult{
			Embeddings: embeddings,
			Dimensions: len(embeddings),
			Timestamp:  time.Now(),
		}
	}

	saveSnapshot(t, "expected_feature_extraction.json", modelName, snapshotData)
}

func TestSnapshot_CreateClassificationData(t *testing.T) {
	if !isSnapshotMode() {
		t.Skip("Skipping snapshot creation (set SNAPSHOT_UPDATE=1)")
	}

	tmpDir := t.TempDir()
	modelName := "KnightsAnalytics/distilbert-base-uncased-finetuned-sst-2-english"

	opts := hugot.NewDownloadOptions()
	opts.ConcurrentConnections = 1

	modelPath, err := hugot.DownloadModel(modelName, tmpDir, opts)
	require.NoError(t, err, "Failed to download model")

	session, err := hugot.NewGoSession()
	require.NoError(t, err, "Failed to create session")
	defer session.Destroy()

	config := hugot.TextClassificationConfig{
		ModelPath: modelPath,
		Name:      "snapshot-text-classifier",
		Options: []hugot.TextClassificationOption{
			pipelines.WithSoftmax(),
		},
	}

	pipeline, err := hugot.NewPipeline(session, config)
	require.NoError(t, err, "Failed to create pipeline")

	testPrompts := []string{
		"I love this amazing product!",
		"This is absolutely terrible.",
		"The service was okay, nothing special.",
		"Best experience ever, highly recommend!",
	}

	snapshotData := make(map[string]ClassificationSnapshotResult)

	for _, prompt := range testPrompts {

		result, err := pipeline.RunPipeline([]string{prompt})
		require.NoError(t, err, "Failed to run pipeline for prompt: %s", prompt)

		require.Greater(t, len(result.ClassificationOutputs), 0, "No classification outputs returned")
		require.Greater(t, len(result.ClassificationOutputs[0]), 0, "No classification results returned")

		// Take the first (highest confidence) result
		classification := result.ClassificationOutputs[0][0]

		snapshotData[prompt] = ClassificationSnapshotResult{
			Label:     classification.Label,
			Score:     float64(classification.Score),
			Timestamp: time.Now(),
		}
	}

	saveSnapshot(t, "expected_text_classification.json", modelName, snapshotData)
}

func TestSnapshot_CreateMultiClassificationData(t *testing.T) {
	if !isSnapshotMode() {
		t.Skip("Skipping snapshot creation (set SNAPSHOT_UPDATE=1)")
	}

	tmpDir := t.TempDir()
	modelName := "KnightsAnalytics/roberta-base-go_emotions"

	opts := hugot.NewDownloadOptions()
	opts.ConcurrentConnections = 1

	modelPath, err := hugot.DownloadModel(modelName, tmpDir, opts)
	require.NoError(t, err, "Failed to download model")

	session, err := hugot.NewGoSession()
	require.NoError(t, err, "Failed to create session")
	defer session.Destroy()

	config := hugot.TextClassificationConfig{
		ModelPath: modelPath,
		Name:      "snapshot-multi-token-classifier",
		Options: []hugot.TextClassificationOption{
			pipelines.WithSigmoid(),
			pipelines.WithMultiLabel(),
		},
	}

	pipeline, err := hugot.NewPipeline(session, config)
	require.NoError(t, err, "Failed to create pipeline")

	testPrompts := []string{
		"Barack Obama visited New York City last week.",
		"Apple Inc. and Microsoft Corporation are competing in Seattle, Washington.",
		"The University of California, Los Angeles (UCLA) is in Los Angeles, California.",
		"Dr. Martin Luther King Jr. spoke at Washington, D.C. in 1963.",
		"Goldman Sachs Group Inc. is headquartered in New York.",
	}

	snapshotData := make(map[string][]ClassificationSnapshotResult)

	for _, prompt := range testPrompts {
		t.Logf("Processing: %s", prompt)

		result, err := pipeline.RunPipeline([]string{prompt})
		require.NoError(t, err, "Failed to run pipeline for prompt: %s", prompt)
		require.Greater(t, len(result.ClassificationOutputs), 0, "No classification outputs returned")
		require.Greater(t, len(result.ClassificationOutputs[0]), 0, "No classification results returned")

		// Let's store _all_ classification results since this is a multi-token snapshot
		var promptResults []ClassificationSnapshotResult
		for _, classification := range result.ClassificationOutputs[0] {
			promptResults = append(promptResults, ClassificationSnapshotResult{
				Label:     classification.Label,
				Score:     float64(classification.Score),
				Timestamp: time.Now(),
			})
		}
		snapshotData[prompt] = promptResults
	}

	saveSnapshot(t, "expected_multi_token_classification.json", modelName, snapshotData)
}

//------------------------------------------------------------------------------

func saveSnapshot(t *testing.T, filename, modelName string, data interface{}) {
	t.Helper()

	testDataDir := "testdata"
	if err := os.MkdirAll(testDataDir, 0755); err != nil {
		t.Fatalf("Failed to create testdata directory: %v", err)
	}

	snapshotWithMeta := struct {
		Metadata SnapshotMetadata `json:"metadata"`
		Data     interface{}      `json:"data"`
	}{
		Metadata: SnapshotMetadata{
			TestName:  t.Name(),
			ModelName: modelName,
			CreatedAt: time.Now(),
			GoVersion: runtime.Version(),
			Platform:  fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
		},
		Data: data,
	}

	jsonData, err := json.MarshalIndent(snapshotWithMeta, "", "  ")
	require.NoError(t, err, "Failed to marshal snapshot data")

	filePath := filepath.Join(testDataDir, filename)
	err = os.WriteFile(filePath, jsonData, 0644)
	require.NoError(t, err, "Failed to write snapshot file")
}

func loadSnapshot(t *testing.T, filename string, target interface{}) {
	t.Helper()

	filePath := filepath.Join("testdata", filename)
	data, err := os.ReadFile(filePath)
	require.NoError(t, err, "Failed to read snapshot file: %s", filename)

	var snapshotWithMeta struct {
		Metadata SnapshotMetadata `json:"metadata"`
		Data     json.RawMessage  `json:"data"`
	}

	require.NoError(t, json.Unmarshal(data, &snapshotWithMeta),
		"Failed to unmarshal snapshot metadata")

	t.Logf("Loading snapshot %s created at %s (Go %s, %s)",
		filename,
		snapshotWithMeta.Metadata.CreatedAt.Format(time.RFC3339),
		snapshotWithMeta.Metadata.GoVersion,
		snapshotWithMeta.Metadata.Platform)

	require.NoError(t, json.Unmarshal(snapshotWithMeta.Data, target),
		"Failed to unmarshal snapshot data")
}
