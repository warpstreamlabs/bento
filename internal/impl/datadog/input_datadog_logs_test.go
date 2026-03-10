package datadog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

func TestDatadogLogInputConfigParsing(t *testing.T) {
	spec := datadogLogInputSpec()

	t.Run("defaults", func(t *testing.T) {
		parsed, err := spec.ParseYAML(``, nil)
		require.NoError(t, err)
		r, err := newDatadogLogReaderFromParsed(parsed, service.MockResources())
		require.NoError(t, err)
		require.Equal(t, "*", r.conf.query)
		require.Equal(t, datadogV2.LOGSSORT_TIMESTAMP_ASCENDING, r.conf.sortDirection)
		require.Equal(t, int32(100), r.conf.limit)
	})

	t.Run("all fields", func(t *testing.T) {
		parsed, err := spec.ParseYAML(`
api_key: my-key
app_key: my-app-key
site: datadoghq.eu
query: "service:foo"
from: "2024-01-01T00:00:00Z"
to: "2024-02-01T00:00:00Z"
start_from_oldest: false
indexes: [main, security]
page_limit: 500
`, nil)
		require.NoError(t, err)
		r, err := newDatadogLogReaderFromParsed(parsed, service.MockResources())
		require.NoError(t, err)
		require.Equal(t, "my-key", r.conf.apiKey)
		require.Equal(t, "my-app-key", r.conf.appKey)
		require.Equal(t, "datadoghq.eu", r.conf.site)
		require.Equal(t, "service:foo", r.conf.query)
		require.Equal(t, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), r.conf.from)
		require.Equal(t, time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC), r.conf.to)
		require.Equal(t, datadogV2.LOGSSORT_TIMESTAMP_DESCENDING, r.conf.sortDirection)
		require.Equal(t, []string{"main", "security"}, r.conf.indices)
		require.Equal(t, int32(500), r.conf.limit)
	})

	for _, bad := range []struct{ name, yaml string }{
		{"invalid from", `from: "not-a-timestamp"`},
		{"invalid to", `to: "not-a-timestamp"`},
	} {
		t.Run(bad.name, func(t *testing.T) {
			parsed, err := spec.ParseYAML(bad.yaml, nil)
			if err == nil {
				_, err = newDatadogLogReaderFromParsed(parsed, service.MockResources())
			}
			require.Error(t, err)
		})
	}
}

func TestLogReaderRead(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	richLog := func() datadogV2.Log {
		attrs := datadogV2.NewLogAttributes()
		attrs.SetMessage("boom")
		attrs.SetHost("web-01")
		attrs.SetService("payments")
		attrs.SetTags([]string{"env:prod", "version:1.2"})
		attrs.SetStatus("error")
		attrs.SetTimestamp(ts)
		attrs.SetAttributes(map[string]interface{}{"request_id": "abc123"})
		l := datadogV2.NewLog()
		l.SetAttributes(*attrs)
		return *l
	}()

	tests := []struct {
		name     string
		pages    [][]datadogV2.Log
		wantMsgs []string
		wantMeta []map[string]string
	}{
		{
			name:     "single page",
			pages:    [][]datadogV2.Log{{makeLog("a", "h1", ""), makeLog("b", "", "s1")}},
			wantMsgs: []string{"a", "b"},
			wantMeta: []map[string]string{
				{"datadog_log_host": "h1"},
				{"datadog_log_service": "s1"},
			},
		},
		{
			name:     "multiple pages",
			pages:    [][]datadogV2.Log{{makeLog("p1", "", "")}, {makeLog("p2", "", "")}},
			wantMsgs: []string{"p1", "p2"},
			wantMeta: []map[string]string{{}, {}},
		},
		{
			name:     "full metadata",
			pages:    [][]datadogV2.Log{{richLog}},
			wantMsgs: []string{"boom"},
			wantMeta: []map[string]string{{
				"datadog_log_host":       "web-01",
				"datadog_log_service":    "payments",
				"datadog_log_tags":       "env:prod,version:1.2",
				"datadog_log_status":     "error",
				"datadog_log_timestamp":  ts.Format(time.RFC3339),
				"datadog_log_request_id": "abc123",
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestReader(t, newMockLogsServer(t, tt.pages), `from: "2024-01-01T00:00:00Z"`)
			msgs := readN(t, r, len(tt.wantMsgs))
			for i, msg := range msgs {
				require.Equal(t, tt.wantMsgs[i], msgBody(t, msg))
				got := msgMeta(t, msg)
				for k, v := range tt.wantMeta[i] {
					require.Equal(t, v, got[k], "meta key %s", k)
				}
			}
		})
	}
}

func TestLogReaderDDAPIKeyEnvVar(t *testing.T) {
	t.Setenv("DD_API_KEY", "env-key")
	var gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("DD-API-KEY")
		resp := datadogV2.LogsListResponse{}
		resp.SetData([]datadogV2.Log{makeLog("msg", "", "")})
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)

	readN(t, newTestReader(t, srv, `from: "2024-01-01T00:00:00Z"`), 1)
	require.Equal(t, "env-key", gotKey)
}

func TestLogReaderExhausted(t *testing.T) {
	srv := newMockLogsServer(t, [][]datadogV2.Log{
		{makeLog("only-log", "", "")},
	})
	r := newTestReader(t, srv, `
from: "2024-01-01T00:00:00Z"
to: "2024-02-01T00:00:00Z"
`)

	readN(t, r, 1)

	_, _, err := r.Read(t.Context())
	require.ErrorIs(t, err, service.ErrEndOfInput)
}

func TestLogReaderContextCancellation(t *testing.T) {
	r := newTestReader(t, newMockLogsServer(t, nil), `from: "2024-01-01T00:00:00Z"`)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := r.Read(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func newMockLogsServer(t *testing.T, pages [][]datadogV2.Log) *httptest.Server {
	t.Helper()
	pageIdx := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := datadogV2.LogsListResponse{}
		if pageIdx < len(pages) {
			resp.SetData(pages[pageIdx])
			pageIdx++
			if pageIdx < len(pages) {
				resp.SetMeta(datadogV2.LogsResponseMetadata{
					Page: &datadogV2.LogsResponseMetadataPage{
						After: datadog.PtrString("cursor-" + string(rune('0'+pageIdx))),
					},
				})
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newTestReader(t *testing.T, srv *httptest.Server, yaml string) *datadogLogReader {
	t.Helper()
	parsed, err := datadogLogInputSpec().ParseYAML(yaml, nil)
	require.NoError(t, err)
	r, err := newDatadogLogReaderFromParsed(parsed, service.MockResources())
	require.NoError(t, err)

	r.conf.ddConf.OperationServers["v2.LogsApi.ListLogs"] = datadog.ServerConfigurations{
		{URL: srv.URL, Description: "test"},
	}
	r.conf.ddConf.HTTPClient = srv.Client()

	require.NoError(t, r.Connect(t.Context()))

	t.Cleanup(func() { _ = r.Close(context.Background()) })
	return r
}

func makeLog(message, host, svc string) datadogV2.Log {
	attrs := datadogV2.NewLogAttributes()
	attrs.SetMessage(message)
	if host != "" {
		attrs.SetHost(host)
	}
	if svc != "" {
		attrs.SetService(svc)
	}
	l := datadogV2.NewLog()
	l.SetAttributes(*attrs)
	return *l
}

func readN(t *testing.T, r *datadogLogReader, n int) []*service.Message {
	t.Helper()
	msgs := make([]*service.Message, n)
	for i := range msgs {
		msg, ack, err := r.Read(t.Context())
		require.NoError(t, err)
		require.NoError(t, ack(t.Context(), nil))
		msgs[i] = msg
	}
	return msgs
}

func msgBody(t *testing.T, msg *service.Message) string {
	t.Helper()
	b, err := msg.AsBytes()
	require.NoError(t, err)
	return string(b)
}

func msgMeta(t *testing.T, msg *service.Message) map[string]string {
	t.Helper()
	m := map[string]string{}
	_ = msg.MetaWalk(func(k, v string) error { m[k] = v; return nil })
	return m
}
