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

	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

type bedrockConverseTestServer struct {
	*httptest.Server

	mu       sync.Mutex
	requests []bedrockConverseCapturedReq
}

type bedrockConverseCapturedReq struct {
	path   string
	header http.Header
	body   []byte
}

func newBedrockConverseTestServer(t *testing.T, handler func(body []byte) (int, []byte)) *bedrockConverseTestServer {
	t.Helper()

	ts := &bedrockConverseTestServer{}
	ts.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()

		ts.mu.Lock()
		ts.requests = append(ts.requests, bedrockConverseCapturedReq{
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

func (ts *bedrockConverseTestServer) captured() []bedrockConverseCapturedReq {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	out := make([]bedrockConverseCapturedReq, len(ts.requests))
	copy(out, ts.requests)
	return out
}

// ------------------------------------------------------------------------------

const converseBaseYAML = `
model: anthropic.claude-3-5-sonnet-20241022-v2:0
region: us-east-1
endpoint: "%v"
credentials:
  id: xxxxxx
  secret: xxxxxx
  token: xxxxxx
`

func converseHandler(body []byte) (int, []byte) {
	var req struct {
		Messages []struct {
			Role    string `json:"role"`
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, []byte(`{"message":"bad request"}`)
	}
	if len(req.Messages) == 0 || len(req.Messages[0].Content) == 0 {
		return http.StatusBadRequest, []byte(`{"message":"no content"}`)
	}
	userText := req.Messages[0].Content[0].Text
	resp, _ := json.Marshal(map[string]any{
		"output": map[string]any{
			"message": map[string]any{
				"role": "assistant",
				"content": []map[string]any{
					{"text": "Echo: " + userText},
				},
			},
		},
		"usage": map[string]any{
			"inputTokens":  10,
			"outputTokens": 5,
		},
	})
	return http.StatusOK, resp
}

func converseErrHandler(_ []byte) (int, []byte) {
	return http.StatusInternalServerError, []byte(`{"message":"kaboom"}`)
}

func converseMsgs(bodies ...string) service.MessageBatch {
	batch := make(service.MessageBatch, len(bodies))
	for i, b := range bodies {
		batch[i] = service.NewMessage([]byte(b))
	}
	return batch
}

// ------------------------------------------------------------------------------

func TestBedrockChatProcessorProcess(t *testing.T) {
	tests := []struct {
		name        string
		conf        string
		handler     func(body []byte) (int, []byte)
		input       service.MessageBatch
		wantErrored int
		wantReqs    int
		wantOutput  string
	}{
		{
			name:       "single message",
			conf:       converseBaseYAML,
			handler:    converseHandler,
			input:      converseMsgs("Hello, world!"),
			wantReqs:   1,
			wantOutput: "Echo: Hello, world!",
		},
		{
			name:        "error is returned",
			conf:        converseBaseYAML,
			handler:     converseErrHandler,
			input:       converseMsgs("Hello"),
			wantErrored: 1,
			wantReqs:    3,
		},
		{
			name: "with system prompt",
			conf: converseBaseYAML + `system_prompt: You are a helpful assistant.
`,
			handler:    converseHandler,
			input:      converseMsgs("test prompt"),
			wantReqs:   1,
			wantOutput: "Echo: test prompt",
		},
		{
			name: "with max_tokens",
			conf: converseBaseYAML + `max_tokens: 512
`,
			handler:    converseHandler,
			input:      converseMsgs("test"),
			wantReqs:   1,
			wantOutput: "Echo: test",
		},
		{
			name: "with temperature",
			conf: converseBaseYAML + `temperature: 0.5
`,
			handler:    converseHandler,
			input:      converseMsgs("test"),
			wantReqs:   1,
			wantOutput: "Echo: test",
		},
		{
			name: "with top_p",
			conf: converseBaseYAML + `top_p: 0.9
`,
			handler:    converseHandler,
			input:      converseMsgs("test"),
			wantReqs:   1,
			wantOutput: "Echo: test",
		},
		{
			name: "with stop sequences",
			conf: converseBaseYAML + `stop: ["STOP","END"]
`,
			handler:    converseHandler,
			input:      converseMsgs("test"),
			wantReqs:   1,
			wantOutput: "Echo: test",
		},
		{
			name:       "multiple messages processed individually",
			conf:       converseBaseYAML,
			handler:    converseHandler,
			input:      converseMsgs("msg1", "msg2", "msg3"),
			wantReqs:   3,
			wantOutput: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			srv := newBedrockConverseTestServer(t, test.handler)

			pConf, err := bedrockChatProcSpec().ParseYAML(fmt.Sprintf(test.conf, srv.URL), nil)
			require.NoError(t, err)

			proc, err := bedrockChatProcessorFromParsed(pConf, service.MockResources())
			require.NoError(t, err)
			t.Cleanup(func() { _ = proc.Close(context.Background()) })

			var exploded []string
			for _, msg := range test.input {
				_, err := proc.Process(context.Background(), msg)
				if err != nil {
					exploded = append(exploded, err.Error())
					continue
				}
			}
			if len(exploded) > 0 {
				require.Len(t, exploded, test.wantErrored)
			} else {
				require.Equal(t, 0, test.wantErrored)
			}
			require.Len(t, srv.captured(), test.wantReqs)

			if test.wantOutput != "" {
				batch, err := proc.Process(context.Background(), test.input[0])
				require.NoError(t, err)
				require.Len(t, batch, 1)
				require.NoError(t, batch[0].GetError())
				out, err := batch[0].AsBytes()
				require.NoError(t, err)
				require.Contains(t, string(out), test.wantOutput)
			}
		})
	}
}

func TestBedrockChatProcessorRequestShape(t *testing.T) {
	srv := newBedrockConverseTestServer(t, converseHandler)

	pConf, err := bedrockChatProcSpec().ParseYAML(fmt.Sprintf(converseBaseYAML+`system_prompt: You are a helpful assistant.
`, srv.URL), nil)
	require.NoError(t, err)

	proc, err := bedrockChatProcessorFromParsed(pConf, service.MockResources())
	require.NoError(t, err)
	t.Cleanup(func() { _ = proc.Close(context.Background()) })

	in := "What is the capital of France?"
	batch, err := proc.Process(context.Background(), service.NewMessage([]byte(in)))
	require.NoError(t, err)
	require.Len(t, batch, 1)
	require.NoError(t, batch[0].GetError())

	out, err := batch[0].AsBytes()
	require.NoError(t, err)

	var outStr string
	require.NoError(t, json.Unmarshal(out, &outStr))
	require.Equal(t, "Echo: "+in, outStr)

	reqs := srv.captured()
	require.Len(t, reqs, 1)

	var reqBody map[string]any
	require.NoError(t, json.Unmarshal(reqs[0].body, &reqBody))
	require.Contains(t, reqBody, "messages")

	messages := reqBody["messages"].([]any)
	require.Len(t, messages, 1)
	msg := messages[0].(map[string]any)
	require.Equal(t, "user", msg["role"])

	content := msg["content"].([]any)
	require.Len(t, content, 1)
	contentBlock := content[0].(map[string]any)
	require.Equal(t, in, contentBlock["text"])

	require.Contains(t, reqBody, "system")

	require.Contains(t, reqs[0].header.Get("Authorization"), "AWS4-HMAC-SHA256")
	require.Equal(t, "application/json", reqs[0].header.Get("Content-Type"))
}

func TestBedrockChatProcessorUsesPromptField(t *testing.T) {
	srv := newBedrockConverseTestServer(t, converseHandler)

	pConf, err := bedrockChatProcSpec().ParseYAML(fmt.Sprintf(converseBaseYAML+`prompt: "test prompt"
`, srv.URL), nil)
	require.NoError(t, err)

	proc, err := bedrockChatProcessorFromParsed(pConf, service.MockResources())
	require.NoError(t, err)
	t.Cleanup(func() { _ = proc.Close(context.Background()) })

	batch, err := proc.Process(context.Background(), service.NewMessage([]byte("ignored payload")))
	require.NoError(t, err)
	require.Len(t, batch, 1)

	reqs := srv.captured()
	require.Len(t, reqs, 1)

	var reqBody map[string]any
	require.NoError(t, json.Unmarshal(reqs[0].body, &reqBody))

	messages := reqBody["messages"].([]any)
	msg := messages[0].(map[string]any)
	content := msg["content"].([]any)
	contentBlock := content[0].(map[string]any)
	require.Equal(t, "test prompt", contentBlock["text"])
}

func TestBedrockChatProcessorFromParsedDefaults(t *testing.T) {
	srv := newBedrockConverseTestServer(t, converseHandler)

	// No optional fields set - should work with defaults
	pConf, err := bedrockChatProcSpec().ParseYAML(fmt.Sprintf(converseBaseYAML, srv.URL), nil)
	require.NoError(t, err)

	proc, err := bedrockChatProcessorFromParsed(pConf, service.MockResources())
	require.NoError(t, err)
	t.Cleanup(func() { _ = proc.Close(context.Background()) })

	batch, err := proc.Process(context.Background(), service.NewMessage([]byte("test")))
	require.NoError(t, err)
	require.Len(t, batch, 1)
	require.NoError(t, batch[0].GetError())

	reqs := srv.captured()
	require.Len(t, reqs, 1)

	var reqBody map[string]any
	require.NoError(t, json.Unmarshal(reqs[0].body, &reqBody))

	// Inference config should be present but with nil values
	inferenceConfig := reqBody["inferenceConfig"].(map[string]any)
	require.Nil(t, inferenceConfig["maxTokens"])
	require.Nil(t, inferenceConfig["temperature"])
	require.Nil(t, inferenceConfig["topP"])
	require.Empty(t, inferenceConfig["stopSequences"])
}
