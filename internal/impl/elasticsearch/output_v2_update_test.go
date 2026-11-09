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

	goEs "github.com/elastic/go-elasticsearch/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

// Unit tests for the bulk update bodies, so they run without Elasticsearch.

func updateTestOutput(t *testing.T, conf string) *EsOutput {
	t.Helper()

	pConf, err := OutputSpecV2().ParseYAML(`
index: foo
urls: [ http://localhost:9200 ]
`+conf, nil)
	require.NoError(t, err)

	o, err := EsoOutputConstructor(pConf, service.MockResources())
	require.NoError(t, err)
	return o
}

// updateBodyJSON builds and marshals the update body of a single message, the
// same way WriteBatch does.
func updateBodyJSON(t *testing.T, o *EsOutput, action, msg string) (string, error) {
	t.Helper()

	batch := service.MessageBatch{service.NewMessage([]byte(msg))}
	body, err := o.updateBody(0, action, []byte(msg), o.newUpdateExecutors(batch))
	if err != nil {
		return "", err
	}
	b, err := json.Marshal(body)
	return string(b), err
}

func TestOutputV2UpdateBody(t *testing.T) {
	const msg = `{"id":"a","n":1}`

	for name, tc := range map[string]struct {
		conf   string
		action string
		msg    string
		want   string
	}{
		// unchanged behaviour without update options
		"update without options": {
			action: "update",
			want:   `{"doc":{"id":"a","n":1},"doc_as_upsert":false}`,
		},
		"upsert without options": {
			action: "upsert",
			want:   `{"doc":{"id":"a","n":1},"doc_as_upsert":true}`,
		},

		// doc_mapping
		"doc_mapping result is the doc": {
			conf:   "update: { doc_mapping: 'root.n = this.n' }",
			action: "update",
			want:   `{"doc":{"n":1},"doc_as_upsert":false}`,
		},
		"doc_mapping deleted sends the message": {
			conf:   "update: { doc_mapping: 'root = deleted()' }",
			action: "upsert",
			want:   `{"doc":{"id":"a","n":1},"doc_as_upsert":true}`,
		},

		// upsert_mapping
		"upsert_mapping on update": {
			conf:   "update: { upsert_mapping: 'root = this.merge({\"first\": true})' }",
			action: "update",
			want:   `{"doc":{"id":"a","n":1},"upsert":{"id":"a","n":1,"first":true}}`,
		},
		"upsert_mapping on upsert replaces doc_as_upsert": {
			conf:   "update: { upsert_mapping: 'root = this.merge({\"first\": true})' }",
			action: "upsert",
			want:   `{"doc":{"id":"a","n":1},"upsert":{"id":"a","n":1,"first":true}}`,
		},
		"upsert_mapping deleted on update is a plain update": {
			conf:   "update: { upsert_mapping: 'root = deleted()' }",
			action: "update",
			want:   `{"doc":{"id":"a","n":1},"doc_as_upsert":false}`,
		},
		"upsert_mapping deleted on upsert falls back to doc_as_upsert": {
			conf:   "update: { upsert_mapping: 'root = deleted()' }",
			action: "upsert",
			want:   `{"doc":{"id":"a","n":1},"doc_as_upsert":true}`,
		},
		"doc_mapping and upsert_mapping together": {
			conf: `
update:
  doc_mapping: 'root.n = this.n'
  upsert_mapping: 'root = this'
`,
			action: "upsert",
			want:   `{"doc":{"n":1},"upsert":{"id":"a","n":1}}`,
		},

		// script
		"script on update passes the message as params": {
			conf:   "update: { script: { source: 'ctx._source.n += params.n' } }",
			action: "update",
			want:   `{"script":{"source":"ctx._source.n += params.n","lang":"painless","params":{"id":"a","n":1}}}`,
		},
		"script params_mapping result": {
			conf:   "update: { script: { source: 's', params_mapping: 'root.x = this.n' } }",
			action: "update",
			want:   `{"script":{"source":"s","lang":"painless","params":{"x":1}}}`,
		},
		"script params_mapping deleted passes the message": {
			conf:   "update: { script: { source: 's', params_mapping: 'root = deleted()' } }",
			action: "update",
			want:   `{"script":{"source":"s","lang":"painless","params":{"id":"a","n":1}}}`,
		},
		"script on upsert indexes the message when missing": {
			conf:   "update: { script: { source: 's' } }",
			action: "upsert",
			want:   `{"script":{"source":"s","lang":"painless","params":{"id":"a","n":1}},"upsert":{"id":"a","n":1}}`,
		},
		"script on upsert with upsert_mapping": {
			conf: `
update:
  script: { source: 's' }
  upsert_mapping: 'root.first = true'
`,
			action: "upsert",
			want:   `{"script":{"source":"s","lang":"painless","params":{"id":"a","n":1}},"upsert":{"first":true}}`,
		},
		"script on upsert with upsert_mapping deleted indexes the message": {
			conf: `
update:
  script: { source: 's' }
  upsert_mapping: 'root = deleted()'
`,
			action: "upsert",
			want:   `{"script":{"source":"s","lang":"painless","params":{"id":"a","n":1}},"upsert":{"id":"a","n":1}}`,
		},
		"scripted_upsert without upsert document starts empty": {
			conf: `
update:
  script: { source: 's' }
  scripted_upsert: true
`,
			action: "update",
			want:   `{"script":{"source":"s","lang":"painless","params":{"id":"a","n":1}},"scripted_upsert":true,"upsert":{}}`,
		},
		"scripted_upsert with upsert_mapping": {
			conf: `
update:
  script: { source: 's' }
  scripted_upsert: true
  upsert_mapping: 'root.first = true'
`,
			action: "update",
			want:   `{"script":{"source":"s","lang":"painless","params":{"id":"a","n":1}},"scripted_upsert":true,"upsert":{"first":true}}`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			o := updateTestOutput(t, tc.conf)
			m := tc.msg
			if m == "" {
				m = msg
			}
			got, err := updateBodyJSON(t, o, tc.action, m)
			require.NoError(t, err)
			assert.JSONEq(t, tc.want, got)
		})
	}
}

func TestOutputV2UpdateBodyErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		conf    string
		action  string
		msg     string
		wantErr string
	}{
		"doc_mapping error": {
			conf:    `update: { doc_mapping: 'root = throw("doc boom")' }`,
			action:  "update",
			msg:     `{}`,
			wantErr: "doc boom",
		},
		"upsert_mapping error": {
			conf:    `update: { upsert_mapping: 'root = throw("upsert boom")' }`,
			action:  "upsert",
			msg:     `{}`,
			wantErr: "upsert boom",
		},
		"params_mapping error": {
			conf:    `update: { script: { source: 's', params_mapping: 'root = throw("params boom")' } }`,
			action:  "update",
			msg:     `{}`,
			wantErr: "params boom",
		},
		"upsert_mapping error also fails a scripted update": {
			conf: `
update:
  script: { source: 's' }
  upsert_mapping: 'root = throw("upsert boom")'
`,
			action:  "update",
			msg:     `{}`,
			wantErr: "upsert boom",
		},
		"mapping reading a field of a non-JSON message": {
			conf:    `update: { doc_mapping: 'root.n = this.n' }`,
			action:  "update",
			msg:     `not json`,
			wantErr: "",
		},
		"non-JSON message as doc": {
			action: "update",
			msg:    `not json`,
		},
		"non-JSON message as script params": {
			conf:   `update: { script: { source: 's' } }`,
			action: "update",
			msg:    `not json`,
		},
		"non-JSON message as upsert document": {
			conf:   `update: { script: { source: 's', params_mapping: 'root = {}' } }`,
			action: "upsert",
			msg:    `not json`,
		},
		"mapping result that is not JSON": {
			conf:   `update: { doc_mapping: 'root = "plain string"' }`,
			action: "update",
			msg:    `{}`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			o := updateTestOutput(t, tc.conf)
			// the error surfaces either while building the body or while
			// marshalling it, both fail the message in WriteBatch
			_, err := updateBodyJSON(t, o, tc.action, tc.msg)
			require.Error(t, err)
			if tc.wantErr != "" {
				assert.ErrorContains(t, err, tc.wantErr)
			}
		})
	}
}

// fakeBulkServer answers every _bulk request with one successful item per
// action line, so WriteBatch can run without Elasticsearch.
func fakeBulkServer(t *testing.T) *goEs.Client {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// go-elasticsearch refuses responses without this header
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.Header().Set("Content-Type", "application/json")

		var items []string
		scanner := bufio.NewScanner(r.Body)
		scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
		for line := 0; scanner.Scan(); line++ {
			// update items are an action line followed by a body line
			if line%2 == 0 {
				items = append(items, `{"update":{"_index":"foo","status":200,"result":"updated"}}`)
			}
		}
		fmt.Fprintf(w, `{"took":1,"errors":false,"items":[%s]}`, strings.Join(items, ","))
	}))
	t.Cleanup(srv.Close)

	client, err := goEs.NewClient(goEs.Config{Addresses: []string{srv.URL}})
	require.NoError(t, err)
	return client
}

// A mapping error fails only the message it happened on; the rest of the
// batch is still written.
func TestOutputV2UpdateMappingErrorFailsOnlyThatMessage(t *testing.T) {
	o := updateTestOutput(t, `
id: ${! this.id }
action: upsert
num_workers: 1
update:
  upsert_mapping: 'root = if this.fail == true { throw("boom") } else { this }'
`)
	o.client = fakeBulkServer(t)

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"a"}`)),
		service.NewMessage([]byte(`{"id":"b","fail":true}`)),
		service.NewMessage([]byte(`{"id":"c"}`)),
	}
	// the indexer must exist before the batch is handed over
	idx := batch.Index()
	err := o.WriteBatch(context.Background(), batch)
	require.Error(t, err)

	var batchErr *service.BatchError
	require.ErrorAs(t, err, &batchErr)
	assert.Equal(t, 1, batchErr.IndexedErrors())

	var failed []int
	batchErr.WalkMessagesIndexedBy(idx, func(i int, _ *service.Message, err error) bool {
		if err != nil {
			failed = append(failed, i)
			assert.ErrorContains(t, err, "boom")
		}
		return true
	})
	assert.Equal(t, []int{1}, failed)
}

// An unknown action fails only that message too.
func TestOutputV2UnknownActionFailsOnlyThatMessage(t *testing.T) {
	o := updateTestOutput(t, `
id: ${! this.id }
action: ${! this.action }
num_workers: 1
`)
	o.client = fakeBulkServer(t)

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"a","action":"upsert"}`)),
		service.NewMessage([]byte(`{"id":"b","action":"merge"}`)),
	}
	err := o.WriteBatch(context.Background(), batch)

	var batchErr *service.BatchError
	require.ErrorAs(t, err, &batchErr)
	assert.Equal(t, 1, batchErr.IndexedErrors())
	assert.ErrorContains(t, err, "elasticsearch action 'merge' is not allowed")
}
