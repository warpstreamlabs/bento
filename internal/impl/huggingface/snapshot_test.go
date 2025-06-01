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

type Result struct {
	Input  string `json:"input"`
	Result any    `json:"result"`
}

type Stats struct {
	Duration time.Duration `json:"total_duration"`
	Calls    uint64        `json:"total_calls"`
}

type Metadata struct {
	CreatedAt time.Time `json:"created_at"`
	ModelName string    `json:"model_name"`
	GoVersion string    `json:"go_version"`
	Platform  string    `json:"platform"`
	TestName  string    `json:"test_name"`
}

type Snapshot struct {
	Results        []Result `json:"results"`
	TokenizerStats Stats    `json:"tokenizer_stats"`
	PipelineStats  Stats    `json:"pipeline_stats"`
	Metadata       Metadata `json:"metadata"`
}

//------------------------------------------------------------------------------

func isSnapshotMode() bool {
	val := os.Getenv("SNAPSHOT_UPDATE")
	switch strings.ToLower(val) {
	case "1", "true", "yes":
		return true
	}
	return false
}

//------------------------------------------------------------------------------

// Generate the snapshots

func TestSnapshot_CreateFeatureExtractionData(t *testing.T) {
	if !isSnapshotMode() {
		t.Skip("Skipping snapshot creation (set SNAPSHOT_UPDATE=1)")
	}

	// Create a new Hugot Go session
	session, err := hugot.NewGoSession()
	require.NoError(t, err, "Failed to create session")
	t.Cleanup(func() {
		require.NoError(t, session.Destroy())
	})

	tests := []struct {
		name  string
		model string

		opts              []hugot.FeatureExtractionOption
		modelDownloadPath string

		prompts [][]string
	}{
		{
			name:              "test-snapshot-feature-extraction",
			model:             "sentence-transformers/all-MiniLM-L6-v2",
			modelDownloadPath: "onnx/model.onnx",
			opts:              []hugot.FeatureExtractionOption{},
			prompts: [][]string{
				{"Barack Obama visited New York City last week."},
				{"Apple Inc. and Microsoft Corporation are competing."},
				{"Dr. Martin Luther King Jr. spoke in Washington, D.C."},
				{"The University of California is in Los Angeles."},
				{"John works at Google with Mary Smith."},
			},
		},
	}

	for _, tt := range tests {
		tmpDir := t.TempDir()

		// Download the model from huggingface
		opts := hugot.NewDownloadOptions()
		if tt.modelDownloadPath != "" {
			opts.OnnxFilePath = tt.modelDownloadPath
		}

		path, err := hugot.DownloadModel(tt.model, tmpDir, opts)
		require.NoError(t, err, "Failed to download model")

		// Setup
		config := hugot.FeatureExtractionConfig{
			Name:      tt.name,
			ModelPath: path,
			Options:   tt.opts,
		}

		pipeline, err := hugot.NewPipeline(session, config)
		require.NoError(t, err, "Failed to create pipeline")

		t.Cleanup(func() {
			require.NoError(t, pipeline.GetModel().Destroy())
		})

		// Run the pipeline and save the results
		var results []Result
		for _, batch := range tt.prompts {

			res, err := pipeline.RunPipeline(batch)
			require.NoError(t, err, "Failed to run pipeline for prompt: %s", batch)

			for i, outRes := range res.GetOutput() {
				results = append(results, Result{
					Input:  batch[i],
					Result: outRes,
				})
			}
		}

		snapshot := Snapshot{
			Results: results,
			TokenizerStats: Stats{
				Duration: time.Duration(pipeline.Model.Tokenizer.TokenizerTimings.TotalNS),
				Calls:    pipeline.Model.Tokenizer.TokenizerTimings.NumCalls,
			},
			PipelineStats: Stats{
				Duration: time.Duration(pipeline.PipelineTimings.TotalNS),
				Calls:    pipeline.PipelineTimings.NumCalls,
			},
			Metadata: Metadata{
				TestName:  tt.name,
				ModelName: tt.model,
				CreatedAt: time.Now(),
				GoVersion: runtime.Version(),
				Platform:  fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
			},
		}
		updateSnapshot(t, "expected_feature_extraction.json", tt.name, snapshot)
	}
}

func TestSnapshot_CreateTokenClassificationData(t *testing.T) {
	if !isSnapshotMode() {
		t.Skip("Skipping snapshot creation (set SNAPSHOT_UPDATE=1)")
	}

	// Create a new Hugot Go session
	session, err := hugot.NewGoSession()
	require.NoError(t, err, "Failed to create session")
	t.Cleanup(func() {
		require.NoError(t, session.Destroy())
	})

	tests := []struct {
		name  string
		model string

		opts []hugot.TokenClassificationOption

		prompts [][]string
	}{
		{
			name:  "test-snapshot-token-classification",
			model: "KnightsAnalytics/distilbert-NER",
			opts: []hugot.TokenClassificationOption{
				pipelines.WithSimpleAggregation(),
				pipelines.WithIgnoreLabels([]string{"O"}),
			},
			prompts: [][]string{
				{"Barack Obama visited New York City last week."},
				{"Apple Inc. and Microsoft Corporation are competing."},
				{"Dr. Martin Luther King Jr. spoke in Washington, D.C."},
				{"The University of California is in Los Angeles."},
				{"John works at Google with Mary Smith."},
			},
		},
	}

	for _, tt := range tests {
		tmpDir := t.TempDir()

		// Download the model from huggingface
		path, err := hugot.DownloadModel(tt.model, tmpDir, hugot.NewDownloadOptions())
		require.NoError(t, err, "Failed to download model")

		// Setup
		config := hugot.TokenClassificationConfig{
			Name:      tt.name,
			ModelPath: path,
			Options:   tt.opts,
		}

		pipeline, err := hugot.NewPipeline(session, config)
		require.NoError(t, err, "Failed to create pipeline")

		t.Cleanup(func() {
			require.NoError(t, pipeline.GetModel().Destroy())
		})
		// Run the pipeline and save the results
		var results []Result
		for _, batch := range tt.prompts {

			res, err := pipeline.RunPipeline(batch)
			require.NoError(t, err, "Failed to run pipeline for prompt: %s", batch)

			for i, outRes := range res.GetOutput() {
				results = append(results, Result{
					Input:  batch[i],
					Result: outRes,
				})
			}
		}

		snapshot := Snapshot{
			Results: results,
			TokenizerStats: Stats{
				Duration: time.Duration(pipeline.Model.Tokenizer.TokenizerTimings.TotalNS),
				Calls:    pipeline.Model.Tokenizer.TokenizerTimings.NumCalls,
			},
			PipelineStats: Stats{
				Duration: time.Duration(pipeline.PipelineTimings.TotalNS),
				Calls:    pipeline.PipelineTimings.NumCalls,
			},
			Metadata: Metadata{
				TestName:  tt.name,
				ModelName: tt.model,
				CreatedAt: time.Now(),
				GoVersion: runtime.Version(),
				Platform:  fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
			},
		}
		updateSnapshot(t, "expected_token_classification.json", tt.name, snapshot)
	}
}

func TestSnapshot_CreateClassificationData(t *testing.T) {
	if !isSnapshotMode() {
		t.Skip("Skipping snapshot creation (set SNAPSHOT_UPDATE=1)")
	}

	// Create a new Hugot Go session
	session, err := hugot.NewGoSession()
	require.NoError(t, err, "Failed to create session")
	t.Cleanup(func() {
		require.NoError(t, session.Destroy())
	})

	tests := []struct {
		name  string
		model string

		opts []hugot.TextClassificationOption

		prompts [][]string
	}{
		{
			name:  "test-snapshot-text-classifier",
			model: "KnightsAnalytics/distilbert-base-uncased-finetuned-sst-2-english",
			opts: []hugot.TextClassificationOption{
				pipelines.WithSoftmax(),
				pipelines.WithSingleLabel(),
			},
			prompts: [][]string{
				{"I love this amazing product!"},
				{"This is absolutely terrible."},
				{"The service was okay, nothing special."},
				{"Best experience ever, highly recommend!"},
			},
		},
		{
			name:  "test-snapshot-text-multi-classifier",
			model: "KnightsAnalytics/roberta-base-go_emotions",
			opts: []hugot.TextClassificationOption{
				pipelines.WithSoftmax(),
				pipelines.WithMultiLabel(),
			},
			prompts: [][]string{
				{"Bento boxes taste amazing!", "Meow meow meow... meow meow."},
				{"Why does the blobfish look so sad? :(", "Sir, are you aware of the magnificent octopus on your head?"},
				{"Streaming data is my favourite pastime.", "You are wearing a silly hat."},
			},
		},
	}

	for _, tt := range tests {
		tmpDir := t.TempDir()

		// Download the model from huggingface
		path, err := hugot.DownloadModel(tt.model, tmpDir, hugot.NewDownloadOptions())
		require.NoError(t, err, "Failed to download model")

		// Setup
		config := hugot.TextClassificationConfig{
			Name:      tt.name,
			ModelPath: path,
			Options:   tt.opts,
		}

		pipeline, err := hugot.NewPipeline(session, config)
		require.NoError(t, err, "Failed to create pipeline")

		t.Cleanup(func() {
			require.NoError(t, pipeline.GetModel().Destroy())
		})

		// Run the pipeline and save the results
		var results []Result
		for _, batch := range tt.prompts {

			res, err := pipeline.RunPipeline(batch)
			require.NoError(t, err, "Failed to run pipeline for prompt: %s", batch)

			for i, outRes := range res.ClassificationOutputs {
				results = append(results, Result{
					Input:  batch[i],
					Result: outRes,
				})
			}
		}

		snapshot := Snapshot{
			Results: results,
			TokenizerStats: Stats{
				Duration: time.Duration(pipeline.Model.Tokenizer.TokenizerTimings.TotalNS),
				Calls:    pipeline.Model.Tokenizer.TokenizerTimings.NumCalls,
			},
			PipelineStats: Stats{
				Duration: time.Duration(pipeline.PipelineTimings.TotalNS),
				Calls:    pipeline.PipelineTimings.NumCalls,
			},
			Metadata: Metadata{
				TestName:  tt.name,
				ModelName: tt.model,
				CreatedAt: time.Now(),
				GoVersion: runtime.Version(),
				Platform:  fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
			},
		}
		updateSnapshot(t, "expected_text_classification.json", tt.name, snapshot)
	}
}

func TestSnapshot_ZeroShotClassificationData(t *testing.T) {
	if !isSnapshotMode() {
		t.Skip("Skipping snapshot creation (set SNAPSHOT_UPDATE=1)")
	}

	// Create a new Hugot Go session
	session, err := hugot.NewGoSession()
	require.NoError(t, err, "Failed to create session")

	t.Cleanup(func() {
		require.NoError(t, session.Destroy())
	})

	tests := []struct {
		name              string
		model             string
		labels            []string
		prompts           [][]string
		modelDownloadPath string
	}{
		{
			name:              "test-snapshot-zero-shot-classification",
			model:             "MoritzLaurer/xtremedistil-l6-h256-zeroshot-v1.1-all-33",
			labels:            []string{"spam"},
			modelDownloadPath: "onnx/model.onnx",
			prompts: [][]string{
				{"BUY ONE GET ONE FREE"},
				{"hi do yuo have time for a job meetign?"},
				{"Sir, this parrot is dead."},
				{"I thought /r/TheDoors was about doors?!"},
			},
		},
		{
			name:              "test-snapshot-zero-shot-classification-multi",
			model:             "MoritzLaurer/xtremedistil-l6-h256-zeroshot-v1.1-all-33",
			labels:            []string{"positive", "negative", "neutral"},
			modelDownloadPath: "onnx/model.onnx",
			prompts: [][]string{
				{"I love this new restaurant, the food was amazing!"},
				{"This movie was terrible and boring."},
				{"The weather is okay today."},
				{"Best purchase I've ever made!"},
				{"I hate waiting in long lines."},
			},
		},
	}

	for _, tt := range tests {
		tmpDir := t.TempDir()

		// Download the model from huggingface
		opts := hugot.NewDownloadOptions()
		if tt.modelDownloadPath != "" {
			opts.OnnxFilePath = tt.modelDownloadPath
		}

		path, err := hugot.DownloadModel(tt.model, tmpDir, opts)
		require.NoError(t, err, "Failed to download model")

		// Setup
		isMultiLabel := len(tt.labels) > 1
		modelOpts := []hugot.ZeroShotClassificationOption{
			pipelines.WithLabels(tt.labels),
			pipelines.WithMultilabel(isMultiLabel),
		}

		config := hugot.ZeroShotClassificationConfig{
			Name:      tt.name,
			ModelPath: path,
			Options:   modelOpts,
		}

		pipeline, err := hugot.NewPipeline(session, config)
		require.NoError(t, err, "Failed to create pipeline")

		t.Cleanup(func() {
			require.NoError(t, pipeline.GetModel().Destroy())
		})
		// Run the pipeline and save the results
		var results []Result
		for _, batch := range tt.prompts {

			res, err := pipeline.RunPipeline(batch)
			require.NoError(t, err, "Failed to run pipeline for prompt: %s", batch)

			for i, outRes := range res.GetOutput() {
				results = append(results, Result{
					Input:  batch[i],
					Result: outRes,
				})
			}
		}

		snapshot := Snapshot{
			Results: results,
			TokenizerStats: Stats{
				Duration: time.Duration(pipeline.Model.Tokenizer.TokenizerTimings.TotalNS),
				Calls:    pipeline.Model.Tokenizer.TokenizerTimings.NumCalls,
			},
			PipelineStats: Stats{
				Duration: time.Duration(pipeline.PipelineTimings.TotalNS),
				Calls:    pipeline.PipelineTimings.NumCalls,
			},
			Metadata: Metadata{
				TestName:  tt.name,
				ModelName: tt.model,
				CreatedAt: time.Now(),
				GoVersion: runtime.Version(),
				Platform:  fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
			},
		}
		updateSnapshot(t, "expected_zero_shot_classification.json", tt.name, snapshot)
	}
}

//------------------------------------------------------------------------------

func saveSnapshot(t *testing.T, filename string, updates map[string]Snapshot) {
	t.Helper()

	testDataDir := "testdata"
	if err := os.MkdirAll(testDataDir, 0755); err != nil {
		t.Fatalf("Failed to create testdata directory: %v", err)
	}

	filePath := filepath.Join(testDataDir, filename)
	existing := make(map[string]Snapshot)

	data, err := os.ReadFile(filePath)
	if !os.IsNotExist(err) {
		require.NoError(t, err)
	}

	if len(data) > 0 {
		err = json.Unmarshal(data, &existing)
		require.NoError(t, err, "Failed to unmarshal existing snapshot data")
	}

	// Merge the new snapshot objects with what we retrieved from our saved file
	for key, value := range updates {
		existing[key] = value
	}

	jsonData, err := json.MarshalIndent(existing, "", "  ")
	require.NoError(t, err, "Failed to marshal snapshot data")

	err = os.WriteFile(filePath, jsonData, 0644)
	require.NoError(t, err, "Failed to write snapshot file")
}

func loadSnapshots(t *testing.T, filename string) map[string]Snapshot {
	t.Helper()

	filePath := filepath.Join("testdata", filename)
	data, err := os.ReadFile(filePath)
	require.NoError(t, err, "Failed to read snapshot file: %s", filename)

	snapshots := make(map[string]Snapshot)
	err = json.Unmarshal(data, &snapshots)
	require.NoError(t, err, "Failed to unmarshal snapshot data")

	return snapshots
}

func loadSnapshot(t *testing.T, name, filename string) Snapshot {
	t.Helper()

	snapshots := loadSnapshots(t, filename)
	snapshot, ok := snapshots[name]
	if !ok {
		t.Fatalf("Could not load in snapshot '%s' from '%s'", name, filename)
	}
	return snapshot
}

func updateSnapshot(t *testing.T, filename string, key string, snapshot Snapshot) {
	t.Helper()
	updates := map[string]Snapshot{key: snapshot}
	saveSnapshot(t, filename, updates)
}
