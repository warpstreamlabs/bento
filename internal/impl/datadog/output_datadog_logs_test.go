package datadog

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

func TestDatadogLogWriterConfigParsing(t *testing.T) {
	spec := datadogLogOutputSpec()

	tests := []struct {
		name        string
		yaml        string
		assertConf  func(t *testing.T, conf datadogLogWriterConfig)
		expectError bool
	}{
		{
			name: "minimal config uses defaults",
			yaml: ``,
			assertConf: func(t *testing.T, conf datadogLogWriterConfig) {
				require.Empty(t, conf.apiKey)
				require.Empty(t, conf.site)
				require.Equal(t, datadogV2.CONTENTENCODING_GZIP, conf.contentEncoding)
				require.Nil(t, conf.ddsource)
				require.Nil(t, conf.ddtags)
				require.Nil(t, conf.hostname)
				require.Nil(t, conf.service)
			},
		},
		{
			name: "all fields set",
			yaml: `
api_key: test-api-key
site: datadoghq.eu
source: nginx
tags: "env:prod,version:1.0"
hostname: my-host
service: my-service
content_encoding: deflate
`,
			assertConf: func(t *testing.T, conf datadogLogWriterConfig) {
				require.Equal(t, "test-api-key", conf.apiKey)
				require.Equal(t, "datadoghq.eu", conf.site)
				require.Equal(t, datadogV2.CONTENTENCODING_DEFLATE, conf.contentEncoding)
				require.NotNil(t, conf.ddsource)
				require.NotNil(t, conf.ddtags)
				require.NotNil(t, conf.hostname)
				require.NotNil(t, conf.service)
			},
		},
		{
			name:        "invalid content_encoding rejected",
			yaml:        `content_encoding: zstd`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := spec.ParseYAML(tt.yaml, nil)
			if tt.expectError {
				if err == nil {
					_, err = newDatadogLogWriterFromParsed(parsed, service.MockResources())
				}
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			w, err := newDatadogLogWriterFromParsed(parsed, service.MockResources())
			require.NoError(t, err)
			tt.assertConf(t, w.writerConf)
		})
	}
}

func TestGetContext(t *testing.T) {
	tests := []struct {
		name   string
		conf   datadogLogWriterConfig
		assert func(t *testing.T, ctx context.Context)
	}{
		{
			name: "site set when configured",
			conf: datadogLogWriterConfig{site: "datadoghq.eu"},
			assert: func(t *testing.T, ctx context.Context) {
				vars, ok := ctx.Value(datadog.ContextServerVariables).(map[string]string)
				require.True(t, ok)
				require.Equal(t, "datadoghq.eu", vars["site"])
			},
		},
		{
			name: "site not set when empty",
			conf: datadogLogWriterConfig{},
			assert: func(t *testing.T, ctx context.Context) {
				require.Nil(t, ctx.Value(datadog.ContextServerVariables))
			},
		},
		{
			name: "api key set when configured",
			conf: datadogLogWriterConfig{apiKey: "my-key"},
			assert: func(t *testing.T, ctx context.Context) {
				keys, ok := ctx.Value(datadog.ContextAPIKeys).(map[string]datadog.APIKey)
				require.True(t, ok)
				require.Equal(t, "my-key", keys["apiKeyAuth"].Key)
			},
		},
		{
			name: "api key not overridden when empty",
			conf: datadogLogWriterConfig{},
			assert: func(t *testing.T, ctx context.Context) {
				keys, _ := ctx.Value(datadog.ContextAPIKeys).(map[string]datadog.APIKey)
				require.Empty(t, keys["apiKeyAuth"].Key)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.conf.buildDDContext(context.Background())
			tt.assert(t, ctx)
		})
	}
}

type mockLogServer struct {
	*httptest.Server
	received []datadogV2.HTTPLogItem
}

func newMockLogServer(t *testing.T, statusCode int) *mockLogServer {
	t.Helper()
	m := &mockLogServer{}
	m.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		err = json.Unmarshal(body, &m.received)
		require.NoError(t, err)

		w.WriteHeader(statusCode)
		_, _ = w.Write([]byte("{}"))
	}))
	t.Cleanup(m.Close)
	return m
}

func newTestWriter(t *testing.T, m *mockLogServer, yaml string) *datadogLogWriter {
	t.Helper()
	parsed, err := datadogLogOutputSpec().ParseYAML(yaml, nil)
	require.NoError(t, err)

	w, err := newDatadogLogWriterFromParsed(parsed, service.MockResources())
	require.NoError(t, err)

	w.ddConf.OperationServers["v2.LogsApi.SubmitLog"] = datadog.ServerConfigurations{
		{URL: m.URL, Description: "test"},
	}
	w.ddConf.HTTPClient = m.Client()

	require.NoError(t, w.Connect(t.Context()))
	return w
}

func TestWriteBatch(t *testing.T) {
	t.Run("uses DD_API_KEY env var when api_key not configured", func(t *testing.T) {
		t.Setenv("DD_API_KEY", "env-api-key")

		var gotHeader string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotHeader = r.Header.Get("DD-API-KEY")
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte("{}"))
		}))
		t.Cleanup(srv.Close)

		m := &mockLogServer{Server: srv}
		w := newTestWriter(t, m, `content_encoding: identity`)
		require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{service.NewMessage([]byte("x"))}))
		require.Equal(t, "env-api-key", gotHeader)
	})

	t.Run("sends messages as log items", func(t *testing.T) {
		m := newMockLogServer(t, http.StatusAccepted)
		w := newTestWriter(t, m, `
source: nginx
tags: "env:prod"
hostname: my-host
service: my-svc
content_encoding: identity
`)
		batch := service.MessageBatch{
			service.NewMessage([]byte("hello world")),
			service.NewMessage([]byte("second log")),
		}
		require.NoError(t, w.WriteBatch(t.Context(), batch))
		require.Len(t, m.received, 2)
		require.Equal(t, "hello world", m.received[0].Message)
		require.Equal(t, "second log", m.received[1].Message)
	})

	t.Run("returns error on bad request", func(t *testing.T) {
		m := newMockLogServer(t, http.StatusBadRequest)
		w := newTestWriter(t, m, `content_encoding: identity`)
		err := w.WriteBatch(t.Context(), service.MessageBatch{service.NewMessage([]byte("x"))})
		require.Error(t, err)
	})

	t.Run("interpolated fields resolved per message", func(t *testing.T) {
		m := newMockLogServer(t, http.StatusAccepted)
		w := newTestWriter(t, m, `
source: ${! meta("src") }
content_encoding: identity
`)
		msg1 := service.NewMessage([]byte("a"))
		msg1.MetaSetMut("src", "nginx")
		msg2 := service.NewMessage([]byte("b"))
		msg2.MetaSetMut("src", "app")

		require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{msg1, msg2}))
		require.Len(t, m.received, 2)
		require.Equal(t, "nginx", *m.received[0].Ddsource)
		require.Equal(t, "app", *m.received[1].Ddsource)
	})
}
