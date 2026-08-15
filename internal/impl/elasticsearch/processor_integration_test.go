package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	goEs "github.com/elastic/go-elasticsearch/v9"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

func processorFromConfIntegration(t testing.TB, conf string, args ...any) *EsProcessor {
	t.Helper()
	pConf, err := ProcessorSpec().ParseYAML(fmt.Sprintf(conf, args...), nil)
	require.NoError(t, err)

	p, err := NewProcessorFromConfig(pConf, service.MockResources())
	require.NoError(t, err)
	return p
}

func seedDocument(t *testing.T, client *goEs.Client, index, id string, body map[string]any) {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	res, err := client.Index(index, strings.NewReader(string(bodyBytes)),
		client.Index.WithDocumentID(id),
		client.Index.WithRefresh("true"),
	)
	require.NoError(t, err)
	defer res.Body.Close()
	require.False(t, res.IsError(), "seed document failed: %s", res.Status())
}

func documentExists(t *testing.T, client *goEs.Client, index, id string) bool {
	t.Helper()
	res, err := client.Exists(index, id)
	require.NoError(t, err)
	defer res.Body.Close()
	return res.StatusCode == 200
}

func TestIntegrationProcessor(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = 3 * time.Minute

	resource, err := pool.Run("elasticsearch", "8.16.5", []string{
		"discovery.type=single-node",
		"xpack.security.enabled=false",
		"xpack.security.transport.ssl.enabled=false",
		"xpack.security.http.ssl.enabled=false",
		"ES_JAVA_OPTS=-Xms512m -Xmx512m",
	})
	if err != nil {
		t.Fatalf("Could not start Elasticsearch container: %s", err)
	}
	defer func() {
		if err := pool.Purge(resource); err != nil {
			t.Logf("Could not purge resource: %s", err)
		}
	}()

	urls := []string{fmt.Sprintf("http://127.0.0.1:%s", resource.GetPort("9200/tcp"))}

	cfg := goEs.Config{Addresses: urls}
	client, err := goEs.NewClient(cfg)
	require.NoError(t, err)

	// Wait until the cluster is ready
	if err = pool.Retry(func() error {
		res, err := client.Cluster.Health()
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if res.IsError() {
			return fmt.Errorf("cluster health returned error: %s", res.String())
		}
		return nil
	}); err != nil {
		t.Fatalf("Elasticsearch did not become ready: %s", err)
	}

	t.Run("SearchReturnsHits", func(t *testing.T) {
		testProcessorSearchReturnsHits(t, urls, client)
	})
	t.Run("SearchNoResults", func(t *testing.T) {
		testProcessorSearchNoResults(t, urls, client)
	})
	t.Run("SearchArgsMappingFromMessage", func(t *testing.T) {
		testProcessorSearchArgsMappingFromMessage(t, urls, client)
	})
	t.Run("SearchSetsMetadata", func(t *testing.T) {
		testProcessorSearchSetsMetadata(t, urls, client)
	})
	t.Run("GetReturnsSource", func(t *testing.T) {
		testProcessorGetReturnsSource(t, urls, client)
	})
	t.Run("GetNotFound", func(t *testing.T) {
		testProcessorGetNotFound(t, urls, client)
	})
	t.Run("GetSetsMetadata", func(t *testing.T) {
		testProcessorGetSetsMetadata(t, urls, client)
	})
	t.Run("GetIDInterpolation", func(t *testing.T) {
		testProcessorGetIDInterpolation(t, urls, client)
	})
	t.Run("DeleteRemovesDocument", func(t *testing.T) {
		testProcessorDeleteRemovesDocument(t, urls, client)
	})
	t.Run("DeleteMessagePassthrough", func(t *testing.T) {
		testProcessorDeleteMessagePassthrough(t, urls, client)
	})
	t.Run("DeleteSetsMetadata", func(t *testing.T) {
		testProcessorDeleteSetsMetadata(t, urls, client)
	})
	t.Run("BatchIndependentErrors", func(t *testing.T) {
		testProcessorBatchIndependentErrors(t, urls, client)
	})
	t.Run("IndexInterpolation", func(t *testing.T) {
		testProcessorIndexInterpolation(t, urls, client)
	})
}

func testProcessorSearchReturnsHits(t *testing.T, urls []string, client *goEs.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	index := "it-search-hits"
	seedDocument(t, client, index, "doc-1", map[string]any{"product": "widget", "price": 9.99})
	seedDocument(t, client, index, "doc-2", map[string]any{"product": "gadget", "price": 19.99})

	p := processorFromConfIntegration(t, `
urls:
  - %s
action: search
index: %s
`, urls[0], index)
	defer p.Close(ctx)

	batches, err := p.ProcessBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte(`{}`)),
	})
	require.NoError(t, err)
	require.NoError(t, batches[0][0].GetError())

	got, err := batches[0][0].AsStructured()
	require.NoError(t, err)

	arr, ok := got.([]any)
	require.True(t, ok)
	assert.Len(t, arr, 2)

	products := []string{}
	for _, item := range arr {
		products = append(products, item.(map[string]any)["product"].(string))
	}
	assert.ElementsMatch(t, []string{"widget", "gadget"}, products)
}

func testProcessorSearchNoResults(t *testing.T, urls []string, client *goEs.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	index := "it-search-empty"
	seedDocument(t, client, index, "doc-1", map[string]any{"status": "active"})

	p := processorFromConfIntegration(t, `
urls:
  - %s
action: search
index: %s
args_mapping: 'root = { "query": { "term": { "status": "nonexistent" } } }'
`, urls[0], index)
	defer p.Close(ctx)

	batches, err := p.ProcessBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte(`{}`)),
	})
	require.NoError(t, err)
	require.NoError(t, batches[0][0].GetError())

	got, err := batches[0][0].AsStructured()
	require.NoError(t, err)
	assert.Empty(t, got.([]any))
}

func testProcessorSearchArgsMappingFromMessage(t *testing.T, urls []string, client *goEs.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	index := "it-search-args"
	seedDocument(t, client, index, "u-1", map[string]any{"user_id": "U-001", "score": 42})
	seedDocument(t, client, index, "u-2", map[string]any{"user_id": "U-002", "score": 77})

	p := processorFromConfIntegration(t, `
urls:
  - %s
action: search
index: %s
args_mapping: 'root = { "query": { "term": { "user_id.keyword": this.uid } } }'
`, urls[0], index)
	defer p.Close(ctx)

	batches, err := p.ProcessBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte(`{"uid":"U-001"}`)),
	})
	require.NoError(t, err)
	require.NoError(t, batches[0][0].GetError())

	got, err := batches[0][0].AsStructured()
	require.NoError(t, err)

	arr := got.([]any)
	require.Len(t, arr, 1)
	assert.Equal(t, "U-001", arr[0].(map[string]any)["user_id"])
	assert.Equal(t, float64(42), arr[0].(map[string]any)["score"])
}

func testProcessorSearchSetsMetadata(t *testing.T, urls []string, client *goEs.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	index := "it-search-meta"
	seedDocument(t, client, index, "d-1", map[string]any{"foo": "bar"})

	p := processorFromConfIntegration(t, `
urls:
  - %s
action: search
index: %s
args_mapping: 'root = { "query": { "match_all": {} } }'
`, urls[0], index)
	defer p.Close(ctx)

	batches, err := p.ProcessBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte(`{}`)),
	})
	require.NoError(t, err)
	require.NoError(t, batches[0][0].GetError())

	resultMsg := batches[0][0]

	val, exists := resultMsg.MetaGetMut("es_index")
	assert.True(t, exists, "es_index should be set")
	assert.Equal(t, index, val)

	val, exists = resultMsg.MetaGetMut("es_took_ms")
	assert.True(t, exists, "es_took_ms should be set")
	assert.NotNil(t, val)

	val, exists = resultMsg.MetaGetMut("es_result_count")
	assert.True(t, exists, "es_result_count should be set")
	assert.Equal(t, 1, val)

	val, exists = resultMsg.MetaGetMut("es_total_hits")
	assert.True(t, exists, "es_total_hits should be set")
	assert.Equal(t, 1, val)
}

func testProcessorGetReturnsSource(t *testing.T, urls []string, client *goEs.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	index := "it-get-source"
	seedDocument(t, client, index, "user-7", map[string]any{"name": "Eve", "role": "engineer"})

	p := processorFromConfIntegration(t, `
urls:
  - %s
action: get
index: %s
id: user-7
`, urls[0], index)
	defer p.Close(ctx)

	batches, err := p.ProcessBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte(`{}`)),
	})
	require.NoError(t, err)
	require.NoError(t, batches[0][0].GetError())

	got, err := batches[0][0].AsStructured()
	require.NoError(t, err)

	m := got.(map[string]any)
	assert.Equal(t, "Eve", m["name"])
	assert.Equal(t, "engineer", m["role"])
}

func testProcessorGetNotFound(t *testing.T, urls []string, _ *goEs.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	p := processorFromConfIntegration(t, `
urls:
  - %s
action: get
index: it-get-notfound
id: does-not-exist
`, urls[0])
	defer p.Close(ctx)

	batches, err := p.ProcessBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte(`{}`)),
	})
	require.NoError(t, err)
	require.Error(t, batches[0][0].GetError())
	assert.Contains(t, batches[0][0].GetError().Error(), "404")
}

func testProcessorGetSetsMetadata(t *testing.T, urls []string, client *goEs.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	index := "it-get-meta"
	seedDocument(t, client, index, "doc-x", map[string]any{"val": 123})

	p := processorFromConfIntegration(t, `
urls:
  - %s
action: get
index: %s
id: doc-x
`, urls[0], index)
	defer p.Close(ctx)

	batches, err := p.ProcessBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte(`{}`)),
	})
	require.NoError(t, err)
	require.NoError(t, batches[0][0].GetError())

	resultMsg := batches[0][0]

	val, exists := resultMsg.MetaGetMut("es_index")
	assert.True(t, exists, "es_index should be set")
	assert.Equal(t, index, val)

	val, exists = resultMsg.MetaGetMut("es_id")
	assert.True(t, exists, "es_id should be set")
	assert.Equal(t, "doc-x", val)

	val, exists = resultMsg.MetaGetMut("es_found")
	assert.True(t, exists, "es_found should be set")
	assert.Equal(t, true, val)
}

func testProcessorGetIDInterpolation(t *testing.T, urls []string, client *goEs.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	index := "it-get-interp"
	seedDocument(t, client, index, "item-A", map[string]any{"label": "alpha"})
	seedDocument(t, client, index, "item-B", map[string]any{"label": "beta"})

	p := processorFromConfIntegration(t, `
urls:
  - %s
action: get
index: %s
id: ${! this.item_id }
`, urls[0], index)
	defer p.Close(ctx)

	for _, tc := range []struct{ itemID, wantLabel string }{
		{"item-A", "alpha"},
		{"item-B", "beta"},
	} {
		batches, err := p.ProcessBatch(ctx, service.MessageBatch{
			service.NewMessage(fmt.Appendf(nil, `{"item_id":%q}`, tc.itemID)),
		})
		require.NoError(t, err)
		require.NoError(t, batches[0][0].GetError())

		got, err := batches[0][0].AsStructured()
		require.NoError(t, err)
		assert.Equal(t, tc.wantLabel, got.(map[string]any)["label"])
	}
}

func testProcessorDeleteRemovesDocument(t *testing.T, urls []string, client *goEs.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	index := "it-delete-removes"
	seedDocument(t, client, index, "del-1", map[string]any{"temp": true})
	require.True(t, documentExists(t, client, index, "del-1"), "doc should exist before delete")

	p := processorFromConfIntegration(t, `
urls:
  - %s
action: delete
index: %s
id: del-1
`, urls[0], index)
	defer p.Close(ctx)

	batches, err := p.ProcessBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte(`{}`)),
	})
	require.NoError(t, err)
	require.NoError(t, batches[0][0].GetError())

	res, err := client.Indices.Refresh(client.Indices.Refresh.WithIndex(index))
	require.NoError(t, err)
	res.Body.Close()

	assert.False(t, documentExists(t, client, index, "del-1"), "doc should not exist after delete")
}

func testProcessorDeleteMessagePassthrough(t *testing.T, urls []string, client *goEs.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	index := "it-delete-passthrough"
	seedDocument(t, client, index, "pt-1", map[string]any{"x": 1})

	p := processorFromConfIntegration(t, `
urls:
  - %s
action: delete
index: %s
id: pt-1
`, urls[0], index)
	defer p.Close(ctx)

	originalBody := []byte(`{"session_id":"pt-1","user":"frank"}`)
	batches, err := p.ProcessBatch(ctx, service.MessageBatch{
		service.NewMessage(originalBody),
	})
	require.NoError(t, err)
	require.NoError(t, batches[0][0].GetError())

	gotBytes, err := batches[0][0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, originalBody, gotBytes)
}

func testProcessorDeleteSetsMetadata(t *testing.T, urls []string, client *goEs.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	index := "it-delete-meta"
	seedDocument(t, client, index, "del-meta-1", map[string]any{"data": "test"})

	p := processorFromConfIntegration(t, `
urls:
  - %s
action: delete
index: %s
id: del-meta-1
`, urls[0], index)
	defer p.Close(ctx)

	batches, err := p.ProcessBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte(`{}`)),
	})
	require.NoError(t, err)
	require.NoError(t, batches[0][0].GetError())

	resultMsg := batches[0][0]

	val, exists := resultMsg.MetaGetMut("es_index")
	assert.True(t, exists, "es_index should be set")
	assert.Equal(t, index, val)

	val, exists = resultMsg.MetaGetMut("es_id")
	assert.True(t, exists, "es_id should be set")
	assert.Equal(t, "del-meta-1", val)

	val, exists = resultMsg.MetaGetMut("es_delete_result")
	assert.True(t, exists, "es_delete_result should be set")
	assert.Equal(t, "deleted", val)
}

func testProcessorBatchIndependentErrors(t *testing.T, urls []string, client *goEs.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	index := "it-batch-errors"
	seedDocument(t, client, index, "present", map[string]any{"found": true})

	p := processorFromConfIntegration(t, `
urls:
  - %s
action: get
index: %s
id: ${! this.doc_id }
`, urls[0], index)
	defer p.Close(ctx)

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"doc_id":"present"}`)),
		service.NewMessage([]byte(`{"doc_id":"absent"}`)),
		service.NewMessage([]byte(`{"doc_id":"present"}`)),
	}

	batches, err := p.ProcessBatch(ctx, batch)
	require.NoError(t, err)
	require.Len(t, batches[0], 3)

	require.NoError(t, batches[0][0].GetError())
	require.NoError(t, batches[0][2].GetError())

	for _, i := range []int{0, 2} {
		got, err := batches[0][i].AsStructured()
		require.NoError(t, err)
		m := got.(map[string]any)
		assert.Equal(t, true, m["found"])
	}

	require.Error(t, batches[0][1].GetError())
	assert.Contains(t, batches[0][1].GetError().Error(), "404")
}

func testProcessorIndexInterpolation(t *testing.T, urls []string, client *goEs.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	seedDocument(t, client, "it-idx-alpha", "doc-1", map[string]any{"tag": "alpha"})
	seedDocument(t, client, "it-idx-beta", "doc-1", map[string]any{"tag": "beta"})

	p := processorFromConfIntegration(t, `
urls:
  - %s
action: get
index: ${! this.target_index }
id: doc-1
`, urls[0])
	defer p.Close(ctx)

	for _, tc := range []struct{ idx, wantTag string }{
		{"it-idx-alpha", "alpha"},
		{"it-idx-beta", "beta"},
	} {
		batches, err := p.ProcessBatch(ctx, service.MessageBatch{
			service.NewMessage(fmt.Appendf(nil, `{"target_index":%q}`, tc.idx)),
		})
		require.NoError(t, err)
		require.NoError(t, batches[0][0].GetError())

		got, err := batches[0][0].AsStructured()
		require.NoError(t, err)
		assert.Equal(t, tc.wantTag, got.(map[string]any)["tag"])
	}
}
