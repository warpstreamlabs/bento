package elasticsearch_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	goEs "github.com/elastic/go-elasticsearch/v9"
	"github.com/ory/dockertest/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/impl/elasticsearch"
	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

func outputFromConfV2(t testing.TB, conf string, args ...any) *elasticsearch.EsOutput {
	t.Helper()

	pConf, err := elasticsearch.OutputSpecV2().ParseYAML(fmt.Sprintf(conf, args...), nil)
	require.NoError(t, err)

	o, err := elasticsearch.EsoOutputConstructor(pConf, service.MockResources())
	require.NoError(t, err)

	return o
}

func TestOutputV2NegativeIntFields(t *testing.T) {
	for _, field := range []string{"retry_on_conflict", "num_workers"} {
		t.Run(field, func(t *testing.T) {
			pConf, err := elasticsearch.OutputSpecV2().ParseYAML(fmt.Sprintf(`
index: foo
urls: [ http://localhost:9200 ]
%s: -1
`, field), nil)
			require.NoError(t, err)

			_, err = elasticsearch.EsoOutputConstructor(pConf, service.MockResources())
			require.ErrorContains(t, err, "must be 0 or more")
		})
	}
}

func TestOutputV2DocAsUpsertWithUpsertMapping(t *testing.T) {
	pConf, err := elasticsearch.OutputSpecV2().ParseYAML(`
index: foo
urls: [ http://localhost:9200 ]
action: update
doc_as_upsert: true
upsert_mapping: 'root = this'
`, nil)
	require.NoError(t, err)

	_, err = elasticsearch.EsoOutputConstructor(pConf, service.MockResources())
	require.ErrorContains(t, err, "cannot be combined")
}

func TestIntegrationWriterV2(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool := dockertest.NewPoolT(t, "", dockertest.WithMaxWait(time.Minute))

	resource, err := pool.Run(t.Context(), "elasticsearch",
		dockertest.WithTag("8.16.5"),
		dockertest.WithEnv([]string{
			"discovery.type=single-node",
			"xpack.security.enabled=false",
			"xpack.security.transport.ssl.enabled=false",
			"xpack.security.http.ssl.enabled=false",
			"ES_JAVA_OPTS=-Xms512m -Xmx512m",
		}),
		dockertest.WithoutReuse(),
	)
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
	}
	urls := []string{fmt.Sprintf("http://127.0.0.1:%v", resource.GetPort("9200/tcp"))}

	cfg := goEs.Config{
		Addresses: urls,
	}
	client, err := goEs.NewClient(cfg)
	require.NoError(t, err)

	if err = pool.Retry(t.Context(), 0, func() error {
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
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	// Create an index with a placeholder for the JSON mapping
	indexMapping := `{
	"settings":{
		"number_of_shards": 1,
		"number_of_replicas": 0
	},
	"mappings":{
		"properties": {
			"user":{
				"type":"keyword"
			},
			"message":{
				"type":"text",
				"store": true,
				"fielddata": true
			}
		}
	}
}`

	res, err := client.Indices.Create("test_index", client.Indices.Create.WithBody(strings.NewReader(indexMapping)))
	require.NoError(t, err)
	defer res.Body.Close()

	if res.IsError() {
		t.Fatalf("Failed to create index: %s", res.String())
	}

	res, err = client.Indices.Create("test_index_2", client.Indices.Create.WithBody(strings.NewReader(indexMapping)))
	require.NoError(t, err)
	defer res.Body.Close()

	if res.IsError() {
		t.Fatalf("Failed to create index: %s", res.String())
	}

	t.Run("TestElasticNoIndexV2", func(te *testing.T) {
		testElasticNoIndexV2(urls, client, te)
	})

	t.Run("TestElasticParallelWritesV2", func(te *testing.T) {
		testElasticParallelWritesV2(urls, client, te)
	})

	t.Run("TestElasticErrorHandlingV2", func(te *testing.T) {
		testElasticErrorHandlingV2(urls, te)
	})

	t.Run("TestElasticConnectV2", func(te *testing.T) {
		testElasticConnectV2(urls, client, te)
	})

	t.Run("testElasticIndexInterpolationV2", func(te *testing.T) {
		testElasticIndexInterpolationV2(urls, client, te)
	})

	t.Run("TestElasticBatchV2", func(te *testing.T) {
		testElasticBatchV2(urls, client, te)
	})

	t.Run("TestElasticBatchDeleteV2", func(te *testing.T) {
		testElasticBatchDeleteV2(urls, client, te)
	})

	t.Run("TestElasticBatchUpdateV2", func(te *testing.T) {
		testElasticBatchUpdateV2(urls, client, te)
	})

	t.Run("TestElasticBatchCreateV2", func(te *testing.T) {
		testElasticBatchCreateV2(urls, client, te)
	})

	t.Run("TestElasticBatchUpsertV2", func(te *testing.T) {
		testElasticBatchUpsertV2(urls, client, te)
	})

	t.Run("TestElasticUpsertRetryOnConflictV2", func(te *testing.T) {
		testElasticUpsertRetryOnConflictV2(urls, client, te)
	})

	t.Run("TestElasticUpsertSameIDSingleWorkerV2", func(te *testing.T) {
		testElasticUpsertSameIDSingleWorkerV2(urls, client, te)
	})

	t.Run("TestElasticUpsertMappingV2", func(te *testing.T) {
		testElasticUpsertMappingV2(urls, client, te)
	})
}

// The upsert_mapping result is only indexed when the document is created;
// later updates merge just the message, so first_seen keeps its first value.
// A mapping that deletes the message behaves as if the field was not set: a
// plain update, which fails for a missing document.
func testElasticUpsertMappingV2(urls []string, client *goEs.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	o := outputFromConfV2(t, `
index: upsert_mapping
id: ${! this.id }
urls: %v
action: update
upsert_mapping: |
  root = if this.skip_upsert_mapping == true {
    deleted()
  } else {
    this.merge({"first_seen": this.seen})
  }
`, urls)

	require.NoError(t, o.Connect(ctx))
	defer func() {
		require.NoError(t, o.Close(ctx))
	}()

	require.NoError(t, o.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte(`{"id":"doc-1","seen":1}`)),
	}))
	require.NoError(t, o.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte(`{"id":"doc-1","seen":2}`)),
	}))

	source := getSourceV2(t, client, "upsert_mapping", "doc-1")
	assert.EqualValues(t, 1, source["first_seen"])
	assert.EqualValues(t, 2, source["seen"])

	// deleted() on an existing document: merged like a plain update
	require.NoError(t, o.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte(`{"id":"doc-1","seen":3,"skip_upsert_mapping":true}`)),
	}))
	source = getSourceV2(t, client, "upsert_mapping", "doc-1")
	assert.EqualValues(t, 1, source["first_seen"])
	assert.EqualValues(t, 3, source["seen"])

	// deleted() on a missing document: same error as an update without the field
	err := o.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte(`{"id":"doc-2","seen":1,"skip_upsert_mapping":true}`)),
	})
	require.ErrorContains(t, err, "document missing")

	// the upsert action already sends the message as upsert document
	o2 := outputFromConfV2(t, `
index: upsert_mapping
id: ${! this.id }
urls: %v
action: upsert
upsert_mapping: 'root = this'
`, urls)
	require.NoError(t, o2.Connect(ctx))
	defer func() {
		require.NoError(t, o2.Close(ctx))
	}()
	require.ErrorContains(t, o2.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte(`{"id":"doc-3"}`)),
	}), "cannot be used with action 'upsert'")
}

func getSourceV2(t *testing.T, client *goEs.Client, index, id string) map[string]any {
	t.Helper()

	get, err := client.Get(index, id)
	require.NoError(t, err)
	defer get.Body.Close()
	require.Equal(t, 200, get.StatusCode)

	var res struct {
		Source map[string]any `json:"_source"`
	}
	require.NoError(t, json.NewDecoder(get.Body).Decode(&res))
	return res.Source
}

// Concurrent upserts to the same document conflict on its version; with
// retry_on_conflict Elasticsearch retries them internally, so every write
// succeeds and none of the partial updates is lost.
func testElasticUpsertRetryOnConflictV2(urls []string, client *goEs.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	o := outputFromConfV2(t, `
index: upsert_retry_on_conflict
id: 'same-doc'
urls: %v
action: upsert
retry_on_conflict: 10
`, urls)

	require.NoError(t, o.Connect(ctx))
	defer func() {
		require.NoError(t, o.Close(ctx))
	}()

	N := 10

	startChan := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(N)

	for i := range N {
		go func(i int) {
			defer wg.Done()
			<-startChan
			assert.NoError(t, o.WriteBatch(ctx, service.MessageBatch{
				service.NewMessage(fmt.Appendf(nil, `{"field_%v":%v}`, i, i)),
			}))
		}(i)
	}

	close(startChan)
	wg.Wait()

	source := getSourceV2(t, client, "upsert_retry_on_conflict", "same-doc")
	for i := range N {
		assert.Contains(t, source, fmt.Sprintf("field_%v", i))
	}
}

// With a single worker a batch is sent as one bulk request, which
// Elasticsearch applies in order, so several upserts to the same document in
// one batch all succeed.
func testElasticUpsertSameIDSingleWorkerV2(urls []string, client *goEs.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	o := outputFromConfV2(t, `
index: upsert_single_worker
id: 'same-doc'
urls: %v
action: upsert
num_workers: 1
`, urls)

	require.NoError(t, o.Connect(ctx))
	defer func() {
		require.NoError(t, o.Close(ctx))
	}()

	N := 10

	var batch service.MessageBatch
	for i := range N {
		batch = append(batch, service.NewMessage(fmt.Appendf(nil, `{"field_%v":%v,"last":%v}`, i, i, i)))
	}
	require.NoError(t, o.WriteBatch(ctx, batch))

	source := getSourceV2(t, client, "upsert_single_worker", "same-doc")
	for i := range N {
		assert.Contains(t, source, fmt.Sprintf("field_%v", i))
	}
	assert.EqualValues(t, N-1, source["last"])
}

func testElasticNoIndexV2(urls []string, client *goEs.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	o := outputFromConfV2(t, `
index: does_not_exist
id: 'foo-${!count("noIndexTest")}'
urls: %v
`, urls)

	require.NoError(t, o.Connect(ctx))
	defer func() {
		require.NoError(t, o.Close(ctx))
	}()

	require.NoError(t, o.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte(`{"message":"hello world","user":"1"}`)),
	}))

	require.NoError(t, o.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte(`{"message":"hello world","user":"2"}`)),
		service.NewMessage([]byte(`{"message":"hello world","user":"3"}`)),
	}))

	for i := range 3 {
		id := fmt.Sprintf("foo-%v", i+1)

		get, err := client.Get("does_not_exist", id)
		require.NoError(t, err, id)

		assert.Equal(t, 200, get.StatusCode)
		require.NoError(t, err, id)

		get.Body.Close()
	}
}

func testElasticParallelWritesV2(urls []string, client *goEs.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	o := outputFromConfV2(t, `
index: new_index_parallel_writes
id: '${!json("key")}'
urls: %v
`, urls)

	require.NoError(t, o.Connect(ctx))
	defer func() {
		require.NoError(t, o.Close(ctx))
	}()

	N := 10

	startChan := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(N)

	docs := map[string]string{}

	for i := range N {
		str := fmt.Sprintf(`{"key":"doc-%v","message":"foobar"}`, i)
		docs[fmt.Sprintf("doc-%v", i)] = str
		go func(content string) {
			<-startChan
			assert.NoError(t, o.WriteBatch(ctx, service.MessageBatch{
				service.NewMessage([]byte(content)),
			}))
			wg.Done()
		}(str)
	}

	close(startChan)
	wg.Wait()

	for id, exp := range docs {
		get, err := client.Get("new_index_parallel_writes", id)
		require.NoError(t, err, id)

		assert.Equal(t, 200, get.StatusCode)
		require.NoError(t, err, id)

		var source map[string]any
		err = json.NewDecoder(get.Body).Decode(&source)
		require.NoError(t, err)

		sourceBytes, err := json.Marshal(source["_source"])
		require.NoError(t, err)

		assert.Equal(t, exp, string(sourceBytes), id)
	}
}

func testElasticErrorHandlingV2(urls []string, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	o := outputFromConfV2(t, `
index: test_index?
id: foo-static
urls: %v`, urls)

	require.NoError(t, o.Connect(ctx))
	defer func() {
		require.NoError(t, o.Close(ctx))
	}()

	require.Error(t, o.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte(`{"message":"foo"}`)),
	}))

	require.Error(t, o.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte(`{"message":"foo"}`)),
		service.NewMessage([]byte(`{"message":"foo"}`)),
	}))
}

func testElasticConnectV2(urls []string, client *goEs.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConfV2(t, `
index: test_index
id: 'foo-${!count("foo")}'
urls: %v
`, urls)

	require.NoError(t, m.Connect(ctx))
	defer func() {
		require.NoError(t, m.Close(ctx))
	}()

	N := 10

	var testMsgs [][]byte
	for i := range N {
		testData := fmt.Appendf(nil, `{"message":"hello world","user":"%v"}`, i)
		testMsgs = append(testMsgs, testData)
	}
	for i := range N {
		require.NoError(t, m.WriteBatch(ctx, service.MessageBatch{
			service.NewMessage(testMsgs[i]),
		}))
	}
	for i := range N {
		id := fmt.Sprintf("foo-%v", i+1)

		get, err := client.Get("test_index", id)
		require.NoError(t, err)
		assert.Equal(t, 200, get.StatusCode)
		require.NoError(t, err, id)
	}
}

func testElasticIndexInterpolationV2(urls []string, client *goEs.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConfV2(t, `
index: ${! @index }
id: 'bar-${!count("bar")}'
urls: %v
`, urls)

	require.NoError(t, m.Connect(ctx))
	defer func() {
		require.NoError(t, m.Close(ctx))
	}()

	N := 10

	testMsgs := [][]byte{}
	for i := range N {
		testMsgs = append(testMsgs, fmt.Appendf(nil, `{"message":"hello world","user":"%v"}`, i))
	}
	for i := range N {
		msg := service.NewMessage(testMsgs[i])
		msg.MetaSetMut("index", "test_index")
		require.NoError(t, m.WriteBatch(ctx, service.MessageBatch{msg}))
	}
	for i := range N {
		id := fmt.Sprintf("bar-%v", i+1)
		get, err := client.Get("test_index", id)
		require.NoError(t, err)
		assert.Equal(t, 200, get.StatusCode)

		var source map[string]any
		err = json.NewDecoder(get.Body).Decode(&source)
		require.NoError(t, err)

		sourceBytes, err := json.Marshal(source["_source"])
		require.NoError(t, err)

		assert.Equal(t, string(testMsgs[i]), string(sourceBytes))
	}
}

func testElasticBatchV2(urls []string, client *goEs.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConfV2(t, `
index: ${! @index }
id: 'baz-${!count("baz")}'
urls: %v
`, urls)

	require.NoError(t, m.Connect(ctx))
	defer func() {
		require.NoError(t, m.Close(ctx))
	}()

	N := 10

	var testMsg [][]byte
	var testBatch service.MessageBatch
	for i := range N {
		testMsg = append(testMsg, fmt.Appendf(nil, `{"message":"hello world","user":"%v"}`, i))
		testBatch = append(testBatch, service.NewMessage(testMsg[i]))
		testBatch[i].MetaSetMut("index", "test_index")
	}

	require.NoError(t, m.WriteBatch(ctx, testBatch))

	for i := range N {
		id := fmt.Sprintf("baz-%v", i+1)
		get, err := client.Get("test_index", id)
		require.NoError(t, err)
		assert.Equal(t, 200, get.StatusCode)

		var source map[string]any
		err = json.NewDecoder(get.Body).Decode(&source)
		require.NoError(t, err)

		sourceBytes, err := json.Marshal(source["_source"])
		require.NoError(t, err)

		assert.Equal(t, string(testMsg[i]), string(sourceBytes))
	}
}

func testElasticBatchDeleteV2(urls []string, client *goEs.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConfV2(t, `
index: ${! @index }
id: 'buz-${!count("elasticBatchDeleteMessages")}'
urls: %v
action: ${! @elastic_action }
`, urls)

	require.NoError(t, m.Connect(ctx))
	defer func() {
		require.NoError(t, m.Close(ctx))
	}()

	N := 10

	var testMsg [][]byte
	var testBatch service.MessageBatch
	for i := range N {
		testMsg = append(testMsg, fmt.Appendf(nil, `{"message":"hello world","user":"%v"}`, i))
		testBatch = append(testBatch, service.NewMessage(testMsg[i]))
		testBatch[i].MetaSetMut("index", "test_index")
		testBatch[i].MetaSetMut("elastic_action", "index")
	}

	require.NoError(t, m.WriteBatch(ctx, testBatch))

	for i := range N {
		id := fmt.Sprintf("buz-%v", i+1)
		get, err := client.Get("test_index", id)

		require.NoError(t, err)
		assert.Equal(t, 200, get.StatusCode)

		var source map[string]any
		err = json.NewDecoder(get.Body).Decode(&source)
		require.NoError(t, err)

		sourceBytes, err := json.Marshal(source["_source"])
		require.NoError(t, err)

		assert.Equal(t, string(testMsg[i]), string(sourceBytes))
	}

	// Set elastic_action to deleted for some message parts
	for i := N / 2; i < N; i++ {
		testBatch[i].MetaSetMut("elastic_action", "delete")
	}

	require.NoError(t, m.WriteBatch(ctx, testBatch))

	for i := range N {
		id := fmt.Sprintf("buz-%v", i+1)
		get, err := client.Get("test_index", id)
		require.NoError(t, err)

		partAction, _ := testBatch[i].MetaGet("elastic_action")
		if partAction == "deleted" && get.StatusCode == 200 {
			t.Errorf("document %v found when it should have been deleted", i)
		} else if partAction != "deleted" && (get.StatusCode != 200) {
			t.Errorf("document %v was not found", i)
		}
	}
}

func testElasticBatchUpdateV2(urls []string, client *goEs.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	o := outputFromConfV2(t, `
index: 'test_index'
id: ${! this.user }
urls: %v
`, urls)

	require.NoError(t, o.Connect(ctx))
	defer func() {
		require.NoError(t, o.Close(ctx))
	}()

	testMsg := [][]byte{
		[]byte(`{"message":"hello world","user":"0"}`),
		[]byte(`{"message":"hello world","user":"1"}`),
	}
	testBatch := service.MessageBatch{
		service.NewMessage(testMsg[0]),
		service.NewMessage(testMsg[1]),
	}

	require.NoError(t, o.WriteBatch(ctx, testBatch))

	for i := range 2 {
		get, err := client.Get("test_index", strconv.Itoa(i))
		require.NoError(t, err)
		assert.Equal(t, 200, get.StatusCode)

		var source map[string]any
		err = json.NewDecoder(get.Body).Decode(&source)
		require.NoError(t, err)

		sourceBytes, err := json.Marshal(source["_source"])
		require.NoError(t, err)

		assert.Equal(t, string(testMsg[i]), string(sourceBytes))
	}

	// testing update to a document created above
	o2 := outputFromConfV2(t, `
index: test_index
id: '0'
urls: %v
action: update
`, urls)

	require.NoError(t, o2.Connect(ctx))
	defer func() {
		require.NoError(t, o2.Close(ctx))
	}()

	testBatch = service.MessageBatch{
		service.NewMessage([]byte(`{"message":"goodbye","user":"updated"}`)),
	}
	require.NoError(t, o2.WriteBatch(ctx, testBatch))

	get, err := client.Get("test_index", "0")
	require.NoError(t, err)
	assert.Equal(t, 200, get.StatusCode)

	var doc struct {
		Message string `json:"message"`
		User    string `json:"user"`
	}

	var source map[string]any
	err = json.NewDecoder(get.Body).Decode(&source)
	require.NoError(t, err)

	sourceBytes, err := json.Marshal(source["_source"])
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(sourceBytes, &doc))
	assert.Equal(t, "updated", doc.User)
	assert.Equal(t, "goodbye", doc.Message)

	// test update to a non existing doc

	o3 := outputFromConfV2(t, `
index: test_index
id: 'does-not-exist'
urls: %v
action: update
`, urls)

	require.NoError(t, o3.Connect(ctx))
	defer func() {
		require.NoError(t, o3.Close(ctx))
	}()

	testBatch = service.MessageBatch{
		service.NewMessage([]byte(`{"message":"goodbye","user":"updated"}`)),
	}
	err = o3.WriteBatch(ctx, testBatch)
	require.ErrorContains(t, err, "document missing")
}

func testElasticBatchCreateV2(urls []string, client *goEs.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	o := outputFromConfV2(t, `
index: test_index
id: ${! @id }
urls: %v
`, urls)

	require.NoError(t, o.Connect(ctx))
	defer func() {
		require.NoError(t, o.Close(ctx))
	}()

	testMsg := [][]byte{
		[]byte(`{"message":"hello world","user":"0"}`),
		[]byte(`{"message":"hello world","user":"1"}`),
	}
	testBatch := service.MessageBatch{
		service.NewMessage(testMsg[0]),
		service.NewMessage(testMsg[1]),
	}

	testBatch[0].MetaSetMut("id", "0")
	testBatch[1].MetaSetMut("id", "1")

	require.NoError(t, o.WriteBatch(ctx, testBatch))

	for i := range 2 {
		get, err := client.Get("test_index", strconv.Itoa(i))
		require.NoError(t, err)
		assert.Equal(t, 200, get.StatusCode)

		var source map[string]any
		err = json.NewDecoder(get.Body).Decode(&source)
		require.NoError(t, err)

		sourceBytes, err := json.Marshal(source["_source"])
		require.NoError(t, err)

		assert.Equal(t, string(testMsg[i]), string(sourceBytes))
	}

	// test successful create action:
	o2 := outputFromConfV2(t, `
index: test_index
id: '3'
urls: %v
action: create
`, urls)

	require.NoError(t, o2.Connect(ctx))
	defer func() {
		require.NoError(t, o2.Close(ctx))
	}()

	testBatch = service.MessageBatch{
		service.NewMessage([]byte(`{"message":"hello world","user":"3"}`)),
	}
	require.NoError(t, o2.WriteBatch(ctx, testBatch))

	get, err := client.Get("test_index", "3")
	require.NoError(t, err)
	assert.Equal(t, 200, get.StatusCode)

	var doc struct {
		Message string `json:"message"`
		User    string `json:"user"`
	}

	var source map[string]any
	err = json.NewDecoder(get.Body).Decode(&source)
	require.NoError(t, err)

	sourceBytes, err := json.Marshal(source["_source"])
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(sourceBytes, &doc))
	assert.Equal(t, "3", doc.User)
	assert.Equal(t, "hello world", doc.Message)

	// test create action on existing doc:
	o3 := outputFromConfV2(t, `
index: test_index
id: '3'
urls: %v
action: create
`, urls)

	require.NoError(t, o3.Connect(ctx))
	defer func() {
		require.NoError(t, o3.Close(ctx))
	}()

	testBatch = service.MessageBatch{
		service.NewMessage([]byte(`{"message":"hello world","user":"3"}`)),
	}
	err = o3.WriteBatch(ctx, testBatch)
	require.ErrorContains(t, err, "document already exists")
}

func testElasticBatchUpsertV2(urls []string, client *goEs.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	o := outputFromConfV2(t, `
index: test_index
id: ${! @id }
urls: %v
`, urls)

	require.NoError(t, o.Connect(ctx))
	defer func() {
		require.NoError(t, o.Close(ctx))
	}()

	testMsg := [][]byte{
		[]byte(`{"message":"hello world","user":"0"}`),
		[]byte(`{"message":"hello world","user":"1"}`),
	}

	initialBatch := service.MessageBatch{
		service.NewMessage(testMsg[0]),
		service.NewMessage(testMsg[1]),
	}

	initialBatch[0].MetaSetMut("id", "0")
	initialBatch[1].MetaSetMut("id", "1")

	require.NoError(t, o.WriteBatch(ctx, initialBatch))

	for i := range 2 {
		get, err := client.Get("test_index", strconv.Itoa(i))
		require.NoError(t, err)
		assert.Equal(t, 200, get.StatusCode)

		var source map[string]any
		err = json.NewDecoder(get.Body).Decode(&source)
		require.NoError(t, err)

		sourceBytes, err := json.Marshal(source["_source"])
		require.NoError(t, err)

		assert.Equal(t, string(testMsg[i]), string(sourceBytes))
	}

	o2 := outputFromConfV2(t, `
index: test_index
id: ${! @id }
urls: %v
action: upsert
`, urls)

	require.NoError(t, o2.Connect(ctx))
	defer func() {
		require.NoError(t, o2.Close(ctx))
	}()

	upsertMsgs := [][]byte{
		[]byte(`{"message":"goodbye","user":"0"}`),
		[]byte(`{"message":"new message","user":"2"}`),
	}

	upsertBatch := service.MessageBatch{
		service.NewMessage(upsertMsgs[0]),
		service.NewMessage(upsertMsgs[1]),
	}

	upsertBatch[0].MetaSetMut("id", "0")
	upsertBatch[1].MetaSetMut("id", "2")

	require.NoError(t, o2.WriteBatch(ctx, upsertBatch))

	get, err := client.Get("test_index", "0")
	require.NoError(t, err)
	assert.Equal(t, 200, get.StatusCode)

	var doc struct {
		Message string `json:"message"`
		User    string `json:"user"`
	}

	var source map[string]any
	err = json.NewDecoder(get.Body).Decode(&source)
	require.NoError(t, err)

	sourceBytes, err := json.Marshal(source["_source"])
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(sourceBytes, &doc))
	assert.Equal(t, "0", doc.User)
	assert.Equal(t, "goodbye", doc.Message)

	get, err = client.Get("test_index", "2")
	require.NoError(t, err)
	assert.Equal(t, 200, get.StatusCode)

	err = json.NewDecoder(get.Body).Decode(&source)
	require.NoError(t, err)

	sourceBytes, err = json.Marshal(source["_source"])
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(sourceBytes, &doc))
	assert.Equal(t, "2", doc.User)
	assert.Equal(t, "new message", doc.Message)
}
