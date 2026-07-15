package aws

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

type mockBedrock struct {
	calls int
	fn    func(*bedrockruntime.InvokeModelInput) (*bedrockruntime.InvokeModelOutput, error)
}

func (m *mockBedrock) InvokeModel(ctx context.Context, in *bedrockruntime.InvokeModelInput, opts ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelOutput, error) {
	m.calls++
	return m.fn(in)
}

func structuredBatch(t *testing.T, batch service.MessageBatch) []any {
	t.Helper()
	out := make([]any, len(batch))
	for i, msg := range batch {
		require.NoError(t, msg.GetError())
		s, err := msg.AsStructured()
		require.NoError(t, err)
		out[i] = s
	}
	return out
}

func TestBedrockEmbeddingsUnsupportedModel(t *testing.T) {
	_, err := newBedrockEmbeddingsProc(&mockBedrock{}, "anthropic.claude-3-sonnet", "auto", "search_document", "", service.MockResources())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "could not infer the model family")
}

func TestBedrockEmbeddingsEmptyModel(t *testing.T) {
	_, err := newBedrockEmbeddingsProc(&mockBedrock{}, "", "auto", "search_document", "", service.MockResources())
	require.Error(t, err)
}

func TestBedrockEmbeddingsInvalidProvider(t *testing.T) {
	_, err := newBedrockEmbeddingsProc(&mockBedrock{}, "cohere.embed-english-v3", "openai", "search_document", "", service.MockResources())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid 'provider'")
}

// An inference profile ARN carries no model name, so auto-detection must fail
// with a clear instruction to set the provider explicitly.
func TestBedrockEmbeddingsInferenceProfileARNNeedsProvider(t *testing.T) {
	arn := "arn:aws:bedrock:us-west-2:123456789012:application-inference-profile/wrdo2nb2muxt"

	_, err := newBedrockEmbeddingsProc(&mockBedrock{}, arn, "auto", "search_document", "", service.MockResources())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "could not infer the model family")

	// With the provider set explicitly the ARN is accepted and passed straight
	// through to InvokeModel as the model ID.
	mock := &mockBedrock{
		fn: func(in *bedrockruntime.InvokeModelInput) (*bedrockruntime.InvokeModelOutput, error) {
			require.Equal(t, arn, *in.ModelId)
			return &bedrockruntime.InvokeModelOutput{
				Body: []byte(`{"embeddings":{"float":[[0.5,0.6]]},"response_type":"embeddings_by_type"}`),
			}, nil
		},
	}
	p, err := newBedrockEmbeddingsProc(mock, arn, "cohere", "search_document", "", service.MockResources())
	require.NoError(t, err)

	outBatches, err := p.ProcessBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte("doc")),
	})
	require.NoError(t, err)
	assert.Equal(t, []any{[]float64{0.5, 0.6}}, structuredBatch(t, outBatches[0]))
}

// Titan has no batch API, so a batch of N messages must produce N separate
// InvokeModel calls, each carrying a single inputText.
func TestBedrockEmbeddingsTitanOneCallPerMessage(t *testing.T) {
	seen := []string{}
	mock := &mockBedrock{
		fn: func(in *bedrockruntime.InvokeModelInput) (*bedrockruntime.InvokeModelOutput, error) {
			require.Equal(t, "amazon.titan-embed-text-v2:0", *in.ModelId)

			var req struct {
				InputText string `json:"inputText"`
			}
			require.NoError(t, json.Unmarshal(in.Body, &req))
			seen = append(seen, req.InputText)

			return &bedrockruntime.InvokeModelOutput{
				Body: []byte(`{"embedding":[0.1,0.2],"inputTextTokenCount":2}`),
			}, nil
		},
	}

	p, err := newBedrockEmbeddingsProc(mock, "amazon.titan-embed-text-v2:0", "auto", "search_document", "", service.MockResources())
	require.NoError(t, err)

	outBatches, err := p.ProcessBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte("one")),
		service.NewMessage([]byte("two")),
		service.NewMessage([]byte("three")),
	})
	require.NoError(t, err)
	require.Len(t, outBatches, 1)

	assert.Equal(t, 3, mock.calls)
	assert.Equal(t, []string{"one", "two", "three"}, seen)
	assert.Equal(t, []any{
		[]float64{0.1, 0.2},
		[]float64{0.1, 0.2},
		[]float64{0.1, 0.2},
	}, structuredBatch(t, outBatches[0]))
}

// Cohere accepts many texts per request, so a batch of N messages must collapse
// into a single InvokeModel call and the embeddings must be unnested back onto
// the messages in order. The request must set embedding_types and the response
// is decoded from the {"embeddings": {"float": [...]}} (v4) shape.
func TestBedrockEmbeddingsCohereV4BatchesIntoSingleCall(t *testing.T) {
	mock := &mockBedrock{
		fn: func(in *bedrockruntime.InvokeModelInput) (*bedrockruntime.InvokeModelOutput, error) {
			require.Equal(t, "cohere.embed-v4:0", *in.ModelId)

			var req struct {
				Texts          []string `json:"texts"`
				InputType      string   `json:"input_type"`
				EmbeddingTypes []string `json:"embedding_types"`
			}
			require.NoError(t, json.Unmarshal(in.Body, &req))
			assert.Equal(t, []string{"a", "b", "c"}, req.Texts)
			assert.Equal(t, "search_query", req.InputType)
			assert.Equal(t, []string{"float"}, req.EmbeddingTypes)

			return &bedrockruntime.InvokeModelOutput{
				Body: []byte(`{"embeddings":{"float":[[1.0],[2.0],[3.0]]},"response_type":"embeddings_by_type"}`),
			}, nil
		},
	}

	p, err := newBedrockEmbeddingsProc(mock, "cohere.embed-v4:0", "cohere", "search_query", "", service.MockResources())
	require.NoError(t, err)

	outBatches, err := p.ProcessBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte("a")),
		service.NewMessage([]byte("b")),
		service.NewMessage([]byte("c")),
	})
	require.NoError(t, err)

	assert.Equal(t, 1, mock.calls)
	assert.Equal(t, []any{
		[]float64{1.0},
		[]float64{2.0},
		[]float64{3.0},
	}, structuredBatch(t, outBatches[0]))
}

// The older v3 bare-array response shape must still decode.
func TestBedrockEmbeddingsCohereV3BareArrayResponse(t *testing.T) {
	mock := &mockBedrock{
		fn: func(in *bedrockruntime.InvokeModelInput) (*bedrockruntime.InvokeModelOutput, error) {
			return &bedrockruntime.InvokeModelOutput{
				Body: []byte(`{"embeddings":[[0.4,0.5]],"response_type":"embeddings_floats"}`),
			}, nil
		},
	}

	p, err := newBedrockEmbeddingsProc(mock, "cohere.embed-english-v3", "auto", "search_document", "", service.MockResources())
	require.NoError(t, err)

	outBatches, err := p.ProcessBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte("a document")),
	})
	require.NoError(t, err)
	assert.Equal(t, []any{[]float64{0.4, 0.5}}, structuredBatch(t, outBatches[0]))
}

// A batch larger than the provider's per-request limit must be split into
// multiple calls.
func TestBedrockEmbeddingsCohereChunksOversizedBatch(t *testing.T) {
	mock := &mockBedrock{
		fn: func(in *bedrockruntime.InvokeModelInput) (*bedrockruntime.InvokeModelOutput, error) {
			var req struct {
				Texts []string `json:"texts"`
			}
			require.NoError(t, json.Unmarshal(in.Body, &req))
			require.LessOrEqual(t, len(req.Texts), cohereMaxBatchTexts)

			floats := make([][]float64, len(req.Texts))
			for i := range floats {
				floats[i] = []float64{float64(i)}
			}
			body, err := json.Marshal(map[string]any{"embeddings": map[string]any{"float": floats}})
			require.NoError(t, err)
			return &bedrockruntime.InvokeModelOutput{Body: body}, nil
		},
	}

	p, err := newBedrockEmbeddingsProc(mock, "cohere.embed-v4:0", "cohere", "search_document", "", service.MockResources())
	require.NoError(t, err)

	batch := make(service.MessageBatch, 0, 200)
	for range 200 {
		batch = append(batch, service.NewMessage([]byte("doc")))
	}

	outBatches, err := p.ProcessBatch(context.Background(), batch)
	require.NoError(t, err)

	// 200 texts / 96 per call = 3 calls.
	assert.Equal(t, 3, mock.calls)
	require.Len(t, outBatches[0], 200)
	for _, msg := range outBatches[0] {
		require.NoError(t, msg.GetError())
	}
}

// When a batched request fails, every message in that group must be flagged.
func TestBedrockEmbeddingsBatchFailureFlagsWholeGroup(t *testing.T) {
	mock := &mockBedrock{
		fn: func(in *bedrockruntime.InvokeModelInput) (*bedrockruntime.InvokeModelOutput, error) {
			return nil, errors.New("throttled")
		},
	}

	p, err := newBedrockEmbeddingsProc(mock, "cohere.embed-v4:0", "cohere", "search_document", "", service.MockResources())
	require.NoError(t, err)

	outBatches, err := p.ProcessBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte("a")),
		service.NewMessage([]byte("b")),
	})
	require.NoError(t, err)
	require.Len(t, outBatches[0], 2)

	assert.Equal(t, 1, mock.calls)
	assert.EqualError(t, outBatches[0][0].GetError(), "throttled")
	assert.EqualError(t, outBatches[0][1].GetError(), "throttled")
}

// A response whose embedding count does not match the request is an error for
// the whole group rather than a silent mismatch.
func TestBedrockEmbeddingsCountMismatch(t *testing.T) {
	mock := &mockBedrock{
		fn: func(in *bedrockruntime.InvokeModelInput) (*bedrockruntime.InvokeModelOutput, error) {
			return &bedrockruntime.InvokeModelOutput{Body: []byte(`{"embeddings":{"float":[[1.0]]}}`)}, nil
		},
	}

	p, err := newBedrockEmbeddingsProc(mock, "cohere.embed-v4:0", "cohere", "search_document", "", service.MockResources())
	require.NoError(t, err)

	outBatches, err := p.ProcessBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte("a")),
		service.NewMessage([]byte("b")),
	})
	require.NoError(t, err)
	assert.Error(t, outBatches[0][0].GetError())
	assert.Error(t, outBatches[0][1].GetError())
}

func TestBedrockEmbeddingsTitanEmptyResponse(t *testing.T) {
	mock := &mockBedrock{
		fn: func(in *bedrockruntime.InvokeModelInput) (*bedrockruntime.InvokeModelOutput, error) {
			return &bedrockruntime.InvokeModelOutput{Body: []byte(`{"embedding":[]}`)}, nil
		},
	}

	p, err := newBedrockEmbeddingsProc(mock, "amazon.titan-embed-text-v1", "auto", "search_document", "", service.MockResources())
	require.NoError(t, err)

	outBatches, err := p.ProcessBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte("foo")),
	})
	require.NoError(t, err)
	assert.Error(t, outBatches[0][0].GetError())
}

// A rate limit that has not been registered as a resource must fail at
// construction rather than being silently ignored.
func TestBedrockEmbeddingsUnknownRateLimit(t *testing.T) {
	_, err := newBedrockEmbeddingsProc(&mockBedrock{}, "cohere.embed-v4:0", "cohere", "search_document", "missing", service.MockResources())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rate limit resource 'missing' was not found")
}

// Each InvokeModel request must acquire the rate limit once. Titan issues one
// request per message, so a three-message batch must consult the limiter three
// times.
func TestBedrockEmbeddingsRateLimitAcquiredPerRequest(t *testing.T) {
	var accesses int64
	res := service.MockResources(service.MockResourcesOptAddRateLimit("throttle", func(context.Context) (time.Duration, error) {
		atomic.AddInt64(&accesses, 1)
		return 0, nil
	}))

	mock := &mockBedrock{
		fn: func(in *bedrockruntime.InvokeModelInput) (*bedrockruntime.InvokeModelOutput, error) {
			return &bedrockruntime.InvokeModelOutput{Body: []byte(`{"embedding":[0.1,0.2]}`)}, nil
		},
	}

	p, err := newBedrockEmbeddingsProc(mock, "amazon.titan-embed-text-v2:0", "auto", "search_document", "throttle", res)
	require.NoError(t, err)

	outBatches, err := p.ProcessBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte("one")),
		service.NewMessage([]byte("two")),
		service.NewMessage([]byte("three")),
	})
	require.NoError(t, err)
	require.Len(t, outBatches[0], 3)

	assert.Equal(t, 3, mock.calls)
	assert.Equal(t, int64(3), atomic.LoadInt64(&accesses))
}
