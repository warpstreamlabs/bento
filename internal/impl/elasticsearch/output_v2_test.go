package elasticsearch

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

// fakeES answers the client's product check on "/" and passes every _bulk
// request to bulk.
func fakeES(t *testing.T, bulk http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/_bulk") {
			bulk(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"version":{"number":"9.0.0","build_flavor":"default"},"tagline":"You Know, for Search"}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newTestOutputV2(t *testing.T, url string) *EsOutput {
	t.Helper()
	pConf, err := OutputSpecV2().ParseYAML(fmt.Sprintf(`
urls: [ %q ]
index: test
id: ${! json("id") }
`, url), nil)
	require.NoError(t, err)

	out, err := EsoOutputConstructor(pConf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, out.Connect(context.Background()))
	return out
}

func testBatch(ids ...string) service.MessageBatch {
	var batch service.MessageBatch
	for _, id := range ids {
		batch = append(batch, service.NewMessage(fmt.Appendf(nil, `{"id":%q}`, id)))
	}
	return batch
}

// bulkIDs returns the document ids of a _bulk request body, in order.
func bulkIDs(t *testing.T, r *http.Request) []string {
	t.Helper()
	var ids []string
	sc := bufio.NewScanner(r.Body)
	for line := 0; sc.Scan(); line++ {
		if line%2 != 0 {
			continue
		}
		var action map[string]struct {
			ID string `json:"_id"`
		}
		require.NoError(t, json.Unmarshal(sc.Bytes(), &action))
		for _, a := range action {
			ids = append(ids, a.ID)
		}
	}
	return ids
}

func TestOutputV2WholeBulkRequestFailure(t *testing.T) {
	srv := fakeES(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"boom","status":500}`))
	})
	out := newTestOutputV2(t, srv.URL)

	err := out.WriteBatch(context.Background(), testBatch("a", "b"))
	require.Error(t, err, "a failed _bulk request must not ack the batch")

	var batchErr *service.BatchError
	require.ErrorAs(t, err, &batchErr)
	assert.Equal(t, 2, batchErr.IndexedErrors())
}

func TestOutputV2BulkUnreachable(t *testing.T) {
	srv := fakeES(t, func(w http.ResponseWriter, r *http.Request) {})
	out := newTestOutputV2(t, srv.URL)
	srv.Close()

	err := out.WriteBatch(context.Background(), testBatch("a"))
	require.Error(t, err, "an unreachable cluster must not ack the batch")
}

func TestOutputV2PartialBulkFailure(t *testing.T) {
	srv := fakeES(t, func(w http.ResponseWriter, r *http.Request) {
		var items []string
		for _, id := range bulkIDs(t, r) {
			if id == "bad" {
				items = append(items, fmt.Sprintf(`{"index":{"_id":%q,"status":400,"error":{"type":"document_parsing_exception","reason":"failed to parse"}}}`, id))
				continue
			}
			items = append(items, fmt.Sprintf(`{"index":{"_id":%q,"status":201,"result":"created"}}`, id))
		}
		_, _ = fmt.Fprintf(w, `{"errors":true,"items":[%s]}`, strings.Join(items, ","))
	})
	out := newTestOutputV2(t, srv.URL)

	batch := testBatch("ok", "bad")
	indexer := batch.Index()
	err := out.WriteBatch(context.Background(), batch)
	var batchErr *service.BatchError
	require.ErrorAs(t, err, &batchErr)
	assert.Equal(t, 1, batchErr.IndexedErrors())

	failed := map[int]bool{}
	batchErr.WalkMessagesIndexedBy(indexer, func(i int, _ *service.Message, err error) bool {
		if err != nil {
			failed[i] = true
		}
		return true
	})
	assert.Equal(t, map[int]bool{1: true}, failed)
}

func TestOutputV2BulkSuccess(t *testing.T) {
	srv := fakeES(t, func(w http.ResponseWriter, r *http.Request) {
		var items []string
		for _, id := range bulkIDs(t, r) {
			items = append(items, fmt.Sprintf(`{"index":{"_id":%q,"status":201,"result":"created"}}`, id))
		}
		_, _ = fmt.Fprintf(w, `{"errors":false,"items":[%s]}`, strings.Join(items, ","))
	})
	out := newTestOutputV2(t, srv.URL)

	require.NoError(t, out.WriteBatch(context.Background(), testBatch("a", "b")))
}
