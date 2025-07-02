package opensearch_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	os "github.com/opensearch-project/opensearch-go/v4"
	osapi "github.com/opensearch-project/opensearch-go/v4/opensearchapi"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/impl/opensearch"
	_ "github.com/warpstreamlabs/bento/public/components/pure"
	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

func outputFromConf(t testing.TB, confStr string, args ...any) *opensearch.Output {
	t.Helper()

	pConf, err := opensearch.OutputSpec().ParseYAML(fmt.Sprintf(confStr, args...), nil)
	require.NoError(t, err)

	o, err := opensearch.OutputFromParsed(pConf, service.MockResources())
	require.NoError(t, err)

	return o
}

func TestIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = time.Second * 60

	resource, err := pool.Run("opensearchproject/opensearch", "latest", []string{
		"discovery.type=single-node",
		"DISABLE_SECURITY_PLUGIN=true",
	})
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
	}

	urls := []string{fmt.Sprintf("http://127.0.0.1:%v", resource.GetPort("9200/tcp"))}

	var client *os.Client

	if err = pool.Retry(func() error {
		opts := os.Config{
			Addresses: urls,
			Transport: http.DefaultTransport,
		}

		var cerr error
		client, cerr = os.NewClient(opts)

		if cerr == nil {
			index := `{
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
			_, cerr = client.Do(context.Background(), osapi.IndicesCreateReq{
				Index: "test_conn_index",
				Body:  strings.NewReader(index),
			}, nil)
			if cerr == nil {
				_, cerr = client.Do(context.Background(), osapi.IndicesCreateReq{
					Index: "test_conn_index_2",
					Body:  strings.NewReader(index),
				}, nil)
			}
			if cerr == nil {
				// Create index template for data stream
				template := `{
	"index_patterns": ["test_datastream*"],
	"data_stream": {},
	"priority": 100,
	"template": {
		"settings": {
			"number_of_shards": 1,
			"number_of_replicas": 0
		},
		"mappings": {
			"properties": {
				"@timestamp": {
					"type": "date"
				},
				"user": {
					"type": "keyword"
				},
				"message": {
					"type": "text",
					"store": true,
					"fielddata": true
				}
			}
		}
	}
}`

				_, cerr = client.Do(context.Background(), osapi.IndexTemplateCreateReq{
					IndexTemplate: "test_datastream_template",
					Body:          strings.NewReader(template),
				}, nil)
			}
			if cerr == nil {
				// Create an index to trigger data stream creation
				_, cerr = client.Do(context.Background(), osapi.DataStreamCreateReq{
					DataStream: "test_datastream",
				}, nil)
			}

		}
		return cerr
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	defer func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()

	t.Run("TestOpenSearchNoIndex", func(te *testing.T) {
		testOpenSearchNoIndex(urls, client, te)
	})

	t.Run("TestOpenSearchParallelWrites", func(te *testing.T) {
		testOpenSearchParallelWrites(urls, client, te)
	})

	t.Run("TestOpenSearchErrorHandling", func(te *testing.T) {
		testOpenSearchErrorHandling(urls, client, te)
	})

	t.Run("TestOpenSearchConnect", func(te *testing.T) {
		testOpenSearchConnect(urls, client, te)
	})

	t.Run("TestOpenSearchIndexInterpolation", func(te *testing.T) {
		testOpenSearchIndexInterpolation(urls, client, te)
	})

	t.Run("TestOpenSearchBatch", func(te *testing.T) {
		testOpenSearchBatch(urls, client, te)
	})

	t.Run("TestOpenSearchBatchDelete", func(te *testing.T) {
		testOpenSearchBatchDelete(urls, client, te)
	})

	t.Run("TestOpenSearchBatchIDCollision", func(te *testing.T) {
		testOpenSearchBatchIDCollision(urls, client, te)
	})

	t.Run("TestOpenSearchDataStream", func(te *testing.T) {
		testOpenSearchDataStream(urls, client, te)
	})

	t.Run("TestOpenSearchBatchCreate", func(te *testing.T) {
		testOpenSearchBatchCreate(urls, client, te)
	})
}

func testOpenSearchNoIndex(urls []string, client *os.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConf(t, `
index: does_not_exist
id: 'foo-${!count("noIndexTest")}'
urls: %v
action: index
`, urls)

	require.NoError(t, m.Connect(ctx))
	defer func() {
		require.NoError(t, m.Close(ctx))
	}()

	require.NoError(t, m.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte(`{"message":"hello world","user":"1"}`)),
	}))

	require.NoError(t, m.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte(`{"message":"hello world","user":"2"}`)),
		service.NewMessage([]byte(`{"message":"hello world","user":"3"}`)),
	}))

	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("foo-%v", i+1)
		get, err := client.Do(ctx, osapi.DocumentGetReq{
			Index:      "does_not_exist",
			DocumentID: id,
		}, nil)
		require.NoError(t, err, id)
		assert.False(t, get.IsError())
	}
}

func resEqualsJSON(t testing.TB, res *os.Response, exp string) {
	t.Helper()
	var tmp struct {
		Source json.RawMessage `json:"_source"`
	}
	dec := json.NewDecoder(res.Body)
	require.NoError(t, dec.Decode(&tmp))
	assert.JSONEq(t, exp, string(tmp.Source))
}

func testOpenSearchParallelWrites(urls []string, client *os.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConf(t, `
index: new_index_parallel_writes
id: '${!json("key")}'
urls: %v
action: index
`, urls)

	require.NoError(t, m.Connect(ctx))
	defer func() {
		require.NoError(t, m.Close(ctx))
	}()

	N := 10

	startChan := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(N)

	docs := map[string]string{}

	for i := 0; i < N; i++ {
		str := fmt.Sprintf(`{"key":"doc-%v","message":"foobar"}`, i)
		docs[fmt.Sprintf("doc-%v", i)] = str
		go func(content string) {
			<-startChan
			assert.NoError(t, m.WriteBatch(ctx, service.MessageBatch{
				service.NewMessage([]byte(content)),
			}))
			wg.Done()
		}(str)
	}

	close(startChan)
	wg.Wait()

	for id, exp := range docs {
		get, err := client.Do(ctx, osapi.DocumentGetReq{
			Index:      "new_index_parallel_writes",
			DocumentID: id,
		}, nil)
		require.NoError(t, err, id)
		assert.False(t, get.IsError())

		resEqualsJSON(t, get, exp)
	}
}

func testOpenSearchErrorHandling(urls []string, client *os.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConf(t, `
index: test_conn_index?
id: 'foo-static'
urls: %v
action: index
`, urls)

	require.NoError(t, m.Connect(ctx))
	defer func() {
		require.NoError(t, m.Close(ctx))
	}()

	require.Error(t, m.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte(`{"message":true}`)),
	}))

	require.Error(t, m.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte(`{"message":"foo"}`)),
		service.NewMessage([]byte(`{"message":"bar"}`)),
	}))
}

func testOpenSearchConnect(urls []string, client *os.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConf(t, `
index: test_conn_index
id: 'foo-${!count("foo")}'
urls: %v
action: index
`, urls)

	require.NoError(t, m.Connect(ctx))
	defer func() {
		require.NoError(t, m.Close(ctx))
	}()

	N := 10

	var testMsgs [][]byte
	for i := 0; i < N; i++ {
		testData := []byte(fmt.Sprintf(`{"message":"hello world","user":"%v"}`, i))
		testMsgs = append(testMsgs, testData)
	}
	for i := 0; i < N; i++ {
		require.NoError(t, m.WriteBatch(ctx, service.MessageBatch{
			service.NewMessage(testMsgs[i]),
		}))
	}
	for i := 0; i < N; i++ {
		id := fmt.Sprintf("foo-%v", i+1)
		get, err := client.Do(ctx, osapi.DocumentGetReq{
			Index:      "test_conn_index",
			DocumentID: id,
		}, nil)
		require.NoError(t, err, id)
		assert.False(t, get.IsError())

		resEqualsJSON(t, get, string(testMsgs[i]))
	}
}

func testOpenSearchIndexInterpolation(urls []string, client *os.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConf(t, `
index: ${! @index }
id: 'bar-${!count("bar")}'
urls: %v
action: index
`, urls)

	require.NoError(t, m.Connect(ctx))
	defer func() {
		require.NoError(t, m.Close(ctx))
	}()

	N := 10

	testMsgs := [][]byte{}
	for i := 0; i < N; i++ {
		testMsgs = append(testMsgs, []byte(fmt.Sprintf(`{"message":"hello world","user":"%v"}`, i)))
	}
	for i := 0; i < N; i++ {
		msg := service.NewMessage(testMsgs[i])
		msg.MetaSetMut("index", "test_conn_index")
		require.NoError(t, m.WriteBatch(ctx, service.MessageBatch{msg}))
	}
	for i := 0; i < N; i++ {
		id := fmt.Sprintf("bar-%v", i+1)
		get, err := client.Do(ctx, osapi.DocumentGetReq{
			Index:      "test_conn_index",
			DocumentID: id,
		}, nil)
		require.NoError(t, err, id)
		assert.False(t, get.IsError())

		resEqualsJSON(t, get, string(testMsgs[i]))
	}
}

func testOpenSearchBatch(urls []string, client *os.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConf(t, `
index: ${! @index }
id: 'baz-${!count("baz")}'
urls: %v
action: index
`, urls)

	require.NoError(t, m.Connect(ctx))
	defer func() {
		require.NoError(t, m.Close(ctx))
	}()

	N := 10

	var testMsg [][]byte
	var testBatch service.MessageBatch
	for i := 0; i < N; i++ {
		testMsg = append(testMsg, []byte(fmt.Sprintf(`{"message":"hello world","user":"%v"}`, i)))
		testBatch = append(testBatch, service.NewMessage(testMsg[i]))
		testBatch[i].MetaSetMut("index", "test_conn_index")
	}

	require.NoError(t, m.WriteBatch(ctx, testBatch))

	for i := 0; i < N; i++ {
		id := fmt.Sprintf("baz-%v", i+1)
		get, err := client.Do(ctx, osapi.DocumentGetReq{
			Index:      "test_conn_index",
			DocumentID: id,
		}, nil)
		require.NoError(t, err, id)
		assert.False(t, get.IsError())

		resEqualsJSON(t, get, string(testMsg[i]))
	}
}

func testOpenSearchBatchDelete(urls []string, client *os.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConf(t, `
index: test_conn_index
id: ${! @elastic_id }
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
	for i := 0; i < N; i++ {
		id := fmt.Sprintf("buz-%v", i+1)
		testMsg = append(testMsg, []byte(fmt.Sprintf(`{"message":"hello world","user":"%v"}`, i)))
		testBatch = append(testBatch, service.NewMessage(testMsg[i]))
		testBatch[i].MetaSetMut("elastic_action", "index")
		testBatch[i].MetaSetMut("elastic_id", id)
	}

	require.NoError(t, m.WriteBatch(ctx, testBatch))

	for i := 0; i < N; i++ {
		id := fmt.Sprintf("buz-%v", i+1)
		get, err := client.Do(ctx, osapi.DocumentGetReq{
			Index:      "test_conn_index",
			DocumentID: id,
		}, nil)
		require.NoError(t, err, id)
		assert.False(t, get.IsError())

		resEqualsJSON(t, get, string(testMsg[i]))
	}

	// Set elastic_action to deleted for some message parts
	for i := N / 2; i < N; i++ {
		testBatch[i].MetaSetMut("elastic_action", "delete")
	}

	require.NoError(t, m.WriteBatch(ctx, testBatch))

	for i := 0; i < N; i++ {
		id := fmt.Sprintf("buz-%v", i+1)
		get, err := client.Do(ctx, osapi.DocumentGetReq{
			Index:      "test_conn_index",
			DocumentID: id,
		}, nil)
		require.NoError(t, err, id)

		partAction, _ := testBatch[i].MetaGet("elastic_action")
		if partAction == "delete" {
			assert.True(t, get.IsError())
		} else {
			assert.False(t, get.IsError())

			resEqualsJSON(t, get, string(testMsg[i]))
		}
	}

	// Test deleting a non-existing document
	m2 := outputFromConf(t, `
index: test_conn_index
id: 'non-existing-id'
urls: %v
action: delete
`, urls)

	require.NoError(t, m2.Connect(ctx))
	defer func() {
		require.NoError(t, m2.Close(ctx))
	}()

	require.Error(t, m2.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte{}),
	}))

	// Verify the document was not found
	get, err := client.Do(ctx, osapi.DocumentGetReq{
		Index:      "test_conn_index",
		DocumentID: "non-existing-id",
	}, nil)
	require.NoError(t, err)
	if get.IsError() {
		if respCode := get.StatusCode; respCode == http.StatusNotFound {
			// Document was not found, as expected
		} else {
			t.Errorf("Unexpected error deleting non-existing document: %d", respCode)
		}
	} else {
		t.Errorf("Expected error deleting non-existing document")
	}
}

func testOpenSearchBatchIDCollision(urls []string, client *os.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConf(t, `
index: ${! @index }
id: 'bar-id'
urls: %v
action: index
`, urls)

	require.NoError(t, m.Connect(ctx))
	defer func() {
		require.NoError(t, m.Close(ctx))
	}()

	testMsg := [][]byte{
		[]byte(`{"message":"hello world","user":"0"}`),
		[]byte(`{"message":"hello world","user":"1"}`),
	}
	testBatch := service.MessageBatch{
		service.NewMessage(testMsg[0]),
		service.NewMessage(testMsg[1]),
	}

	testBatch[0].MetaSetMut("index", "test_conn_index")
	testBatch[1].MetaSetMut("index", "test_conn_index_2")

	require.NoError(t, m.WriteBatch(ctx, testBatch))

	for i := 0; i < 2; i++ {
		index, _ := testBatch[i].MetaGet("index")
		get, err := client.Do(ctx, osapi.DocumentGetReq{
			Index:      index,
			DocumentID: "bar-id",
		}, nil)
		require.NoError(t, err)
		assert.False(t, get.IsError())

		resEqualsJSON(t, get, string(testMsg[i]))
	}

	// testing sequential updates to a document created above
	m2 := outputFromConf(t, `
index: test_conn_index
id: 'bar-id'
urls: %v
action: update
`, urls)

	require.NoError(t, m2.Connect(ctx))
	defer func() {
		require.NoError(t, m2.Close(ctx))
	}()

	testBatch = service.MessageBatch{
		service.NewMessage([]byte(`{"doc":{"message":"goodbye"}}`)),
		service.NewMessage([]byte(`{"doc":{"user": "updated"}}`)),
	}
	require.NoError(t, m2.WriteBatch(ctx, testBatch))

	get, err := client.Do(ctx, osapi.DocumentGetReq{
		Index:      "test_conn_index",
		DocumentID: "bar-id",
	}, nil)
	require.NoError(t, err)
	assert.False(t, get.IsError())

	var tmp struct {
		Source map[string]any `json:"_source"`
	}
	dec := json.NewDecoder(get.Body)
	require.NoError(t, dec.Decode(&tmp))

	assert.Equal(t, "updated", tmp.Source["user"])
	assert.Equal(t, "goodbye", tmp.Source["message"])
}

func testOpenSearchDataStream(urls []string, client *os.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConf(t, `
index: test_datastream
id: 'doc-${!count("datastream")}'
urls: %v
action: create
`, urls)

	require.NoError(t, m.Connect(ctx))
	defer func() {
		require.NoError(t, m.Close(ctx))
	}()

	N := 10

	var testBatch service.MessageBatch
	for i := 0; i < N; i++ {
		testData := []byte(fmt.Sprintf(`{"@timestamp":"%s","message":"hello world","user":"%v"}`, time.Now().UTC().Format(time.RFC3339), i))
		testBatch = append(testBatch, service.NewMessage(testData))
	}

	err := m.WriteBatch(ctx, testBatch)
	require.NoError(t, err, "Failed to write batch to data stream")

	_, err = client.Do(ctx, osapi.IndicesRefreshReq{
		Indices: []string{"test_datastream"},
	}, nil)
	require.NoError(t, err, "Failed to refresh data stream")

	searchReq := osapi.SearchReq{
		Indices: []string{"test_datastream"},
		Body: strings.NewReader(`{
			"query": {
				"match_all": {}
			}
		}`),
	}
	searchRes, err := client.Do(ctx, searchReq, nil)
	require.NoError(t, err)
	assert.False(t, searchRes.IsError())

	var result struct {
		Hits struct {
			Hits []struct {
				Source json.RawMessage `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	dec := json.NewDecoder(searchRes.Body)
	require.NoError(t, dec.Decode(&result))

	// Verify the number of hits matches the number of messages sent
	assert.Len(t, result.Hits.Hits, N, "Expected %d documents in the data stream", N)

}

func testOpenSearchBatchCreate(urls []string, client *os.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	o := outputFromConf(t, `
index: test_index
id: ${! @id }
urls: %v
action: index
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

		get, err := client.Do(ctx, osapi.DocumentGetReq{
			Index:      "test_index",
			DocumentID: strconv.Itoa(i),
		}, nil)
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
	o2 := outputFromConf(t, `
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

	get, err := client.Do(ctx, osapi.DocumentGetReq{
		Index:      "test_index",
		DocumentID: "3",
	}, nil)
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
	o3 := outputFromConf(t, `
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
