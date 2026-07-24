package aws

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/public/service"
)

type bedrockTestServer struct {
	*httptest.Server

	mu       sync.Mutex
	requests []bedrockCapturedReq
}

type bedrockCapturedReq struct {
	path   string
	header http.Header
	body   []byte
}

func newBedrockTestServer(t *testing.T, handler func(body []byte) (int, []byte)) *bedrockTestServer {
	t.Helper()

	ts := &bedrockTestServer{}
	ts.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()

		ts.mu.Lock()
		ts.requests = append(ts.requests, bedrockCapturedReq{
			path:   r.URL.Path,
			header: r.Header.Clone(),
			body:   body,
		})
		ts.mu.Unlock()

		code, resp := handler(body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_, _ = w.Write(resp)
	}))
	t.Cleanup(ts.Close)
	return ts
}

func (ts *bedrockTestServer) captured() []bedrockCapturedReq {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	out := make([]bedrockCapturedReq, len(ts.requests))
	copy(out, ts.requests)
	return out
}

// ------------------------------------------------------------------------------

const baseYAML = `
model: test.embedding-model
region: us-east-1
endpoint: "%v"
credentials:
  id: xxxxxx
  secret: xxxxxx
  token: xxxxxx
`

const rateLimitYAML = baseYAML + `rate_limit: testrl
`

func echoEmbeddingsHandler(body []byte) (int, []byte) {
	var req struct {
		Texts []string `json:"texts"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, []byte(`{"message":"bad request"}`)
	}
	embeddings := make([][]float64, len(req.Texts))
	for i := range req.Texts {
		embeddings[i] = []float64{float64(i), float64(i) + 0.5}
	}
	resp, _ := json.Marshal(map[string]any{
		"embeddings": map[string]any{"float": embeddings},
	})
	return http.StatusOK, resp
}

func alwaysErrHandler(body []byte) (int, []byte) {
	return http.StatusInternalServerError, []byte(`{"message":"kaboom"}`)
}

// failBodyHandler errors for requests whose body matches fail, echoes the rest.
// Keyed on body rather than a call count so that SDK retries keep failing.
func failBodyHandler(fail string) func(body []byte) (int, []byte) {
	return func(body []byte) (int, []byte) {
		if string(body) == fail {
			return alwaysErrHandler(body)
		}
		return echoEmbeddingsHandler(body)
	}
}

func msgs(bodies ...string) service.MessageBatch {
	batch := make(service.MessageBatch, len(bodies))
	for i, b := range bodies {
		batch[i] = service.NewMessage([]byte(b))
	}
	return batch
}

//------------------------------------------------------------------------------

func TestBedrockProcessorProcessBatch(t *testing.T) {
	tests := []struct {
		name        string
		conf        string
		resOpts     []service.MockResourcesOptFn
		handler     func(body []byte) (int, []byte)
		input       service.MessageBatch
		wantErrored int
		wantReqs    int
	}{
		{
			name:     "single message",
			conf:     baseYAML,
			handler:  echoEmbeddingsHandler,
			input:    msgs(`{"texts":["This is a test sentence."],"input_type":"search_document","embedding_types":["float"]}`),
			wantReqs: 1,
		},
		{
			name:    "multi message fans out one request per message",
			conf:    baseYAML,
			handler: echoEmbeddingsHandler,
			input: msgs(
				`{"texts":["a"]}`, `{"texts":["b"]}`, `{"texts":["c"]}`, `{"texts":["d"]}`,
				`{"texts":["e"]}`, `{"texts":["f"]}`, `{"texts":["g"]}`, `{"texts":["h"]}`,
			),
			wantReqs: 8,
		},
		{
			name:        "single message error is attached to the message",
			conf:        baseYAML,
			handler:     alwaysErrHandler,
			input:       msgs(`{"texts":["a"]}`),
			wantErrored: 1,
			wantReqs:    3,
		},
		{
			name:        "batched error is attached per message",
			conf:        baseYAML,
			handler:     failBodyHandler(`{"texts":["a"]}`),
			input:       msgs(`{"texts":["a"]}`, `{"texts":["b"]}`),
			wantErrored: 1,
			wantReqs:    4,
		},
		{
			name:    "rate limit granting immediately does not drop requests",
			conf:    rateLimitYAML,
			handler: echoEmbeddingsHandler,
			input:   msgs(`{"texts":["a"]}`, `{"texts":["b"]}`),
			resOpts: []service.MockResourcesOptFn{
				service.MockResourcesOptAddRateLimit("testrl",
					func(ctx context.Context) (time.Duration, error) { return 0, nil },
				),
			},
			wantReqs: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			srv := newBedrockTestServer(t, test.handler)

			mgr := service.MockResources(test.resOpts...)

			pConf, err := bedrockInvokeProcSpec().ParseYAML(fmt.Sprintf(test.conf, srv.URL), nil)
			require.NoError(t, err)

			proc, err := bedrockProcessorFromParsed(pConf, mgr)
			require.NoError(t, err)
			t.Cleanup(func() { _ = proc.Close(context.Background()) })

			wantLen := len(test.input)

			res, err := proc.ProcessBatch(context.Background(), test.input)

			require.NoError(t, err)
			require.Len(t, res, 1)
			require.Len(t, res[0], wantLen)

			var errored int
			for _, m := range res[0] {
				if m.GetError() != nil {
					errored++
					continue
				}
				out, err := m.AsBytes()
				require.NoError(t, err)
				require.Contains(t, string(out), "embeddings")
			}
			require.Equal(t, test.wantErrored, errored)
			require.Len(t, srv.captured(), test.wantReqs)
		})
	}
}

func TestBedrockProcessorRequestShape(t *testing.T) {
	srv := newBedrockTestServer(t, echoEmbeddingsHandler)

	pConf, err := bedrockInvokeProcSpec().ParseYAML(fmt.Sprintf(baseYAML, srv.URL), nil)
	require.NoError(t, err)

	proc, err := bedrockProcessorFromParsed(pConf, service.MockResources())
	require.NoError(t, err)
	t.Cleanup(func() { _ = proc.Close(context.Background()) })

	in := `{"texts":["This is a test sentence."],"input_type":"search_document","embedding_types":["float"]}`

	res, err := proc.ProcessBatch(context.Background(), msgs(in))
	require.NoError(t, err)
	require.Len(t, res[0], 1)
	require.NoError(t, res[0][0].GetError())

	out, err := res[0][0].AsBytes()
	require.NoError(t, err)

	var got struct {
		Embeddings struct {
			Float [][]float64 `json:"float"`
		} `json:"embeddings"`
	}
	require.NoError(t, json.Unmarshal(out, &got))
	require.Equal(t, [][]float64{{0, 0.5}}, got.Embeddings.Float)

	reqs := srv.captured()
	require.Len(t, reqs, 1)

	require.JSONEq(t, in, string(reqs[0].body))

	require.Contains(t, reqs[0].header.Get("Authorization"), "AWS4-HMAC-SHA256")
	require.Contains(t, reqs[0].path, "anthropic.claude-sonnet-5")
	require.Equal(t, "application/json", reqs[0].header.Get("Content-Type"))
	require.Equal(t, "application/json", reqs[0].header.Get("Accept"))
}
