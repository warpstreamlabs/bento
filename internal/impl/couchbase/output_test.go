package couchbase_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/impl/couchbase"
	"github.com/warpstreamlabs/bento/public/service"

	_ "github.com/warpstreamlabs/bento/public/components/pure"
)

func TestOutputConfigLinting(t *testing.T) {
	configTests := []struct {
		name        string
		config      string
		errContains string
	}{
		{
			name: "get content forbidden",
			config: `
couchbase:
  url: 'url'
  bucket: 'bucket'
  id: '${! json("id") }'
  operation: 'get'
`,
			errContains: `value get is not a valid option for this field`,
		},
		{
			name: "remove content not required",
			config: `
couchbase:
  url: 'url'
  bucket: 'bucket'
  id: '${! json("id") }'
  operation: 'remove'
`,
		},
		{
			name: "missing insert content",
			config: `
couchbase:
  url: 'url'
  bucket: 'bucket'
  id: '${! json("id") }'
  operation: 'insert'
`,
			errContains: `content must be set for insert, replace, upsert, increment and decrement operations.`,
		},
		{
			name: "missing replace content",
			config: `
couchbase:
  url: 'url'
  bucket: 'bucket'
  id: '${! json("id") }'
  operation: 'replace'
`,
			errContains: `content must be set for insert, replace, upsert, increment and decrement operations.`,
		},
		{
			name: "missing upsert content",
			config: `
couchbase:
  url: 'url'
  bucket: 'bucket'
  id: '${! json("id") }'
  operation: 'upsert'
`,
			errContains: `content must be set for insert, replace, upsert, increment and decrement operations.`,
		},
		{
			name: "insert with content",
			config: `
couchbase:
  url: 'url'
  bucket: 'bucket'
  id: '${! json("id") }'
  content: 'root = this'
  operation: 'insert'
`,
		},
		{
			name: "increment",
			config: `
couchbase:
  url: 'url'
  bucket: 'bucket'
  id: '${! json("id") }'
  operation: 'increment'
  content: '1'
`,
		},
		{
			name: "decrement",
			config: `
couchbase:
  url: 'url'
  bucket: 'bucket'
  id: '${! json("id") }'
  operation: 'decrement'
  content: '1'
`,
		},
		{
			name: "increment without content",
			config: `
couchbase:
  url: 'url'
  bucket: 'bucket'
  id: '${! json("id") }'
  operation: 'increment'
`,
			errContains: `content must be set for insert, replace, upsert, increment and decrement operations.`,
		},
		{
			name: "decrement without content",
			config: `
couchbase:
  url: 'url'
  bucket: 'bucket'
  id: '${! json("id") }'
  operation: 'decrement'
`,
			errContains: `content must be set for insert, replace, upsert, increment and decrement operations.`,
		},
		{
			name: "max_in_flight",
			config: `
couchbase:
  url: 'url'
  bucket: 'bucket'
  id: '${! json("id") }'
  operation: 'increment'
  content: '1'
  max_in_flight: 42
`,
		},
		{
			name: "batching",
			config: `
couchbase:
  url: 'url'
  bucket: 'bucket'
  id: '${! json("id") }'
  operation: 'increment'
  content: '1'
  batching:
    count: 10
    period: 5s
`,
		},
	}

	env := service.NewEnvironment()
	for _, test := range configTests {
		t.Run(test.name, func(t *testing.T) {
			strm := env.NewStreamBuilder()
			err := strm.AddOutputYAML(test.config)
			if test.errContains == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			}
		})
	}
}

func getOutput(tb testing.TB, config string) *couchbase.Couchbase {
	tb.Helper()

	confSpec := couchbase.OutputConfig()
	env := service.NewEnvironment()

	pConf, err := confSpec.ParseYAML(config, env)
	require.NoError(tb, err)
	proc, err := couchbase.NewOutput(tb.Context(), pConf, service.MockResources())
	require.NoError(tb, err)
	require.NotNil(tb, proc)

	require.NoError(tb, proc.Connect(tb.Context()))

	return proc
}

func testCouchbaseOutputInsert(payload, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! json("id") }'
content: 'root = this'
operation: 'insert'
`, port, bucket, username, password)

	err := getOutput(t, config).WriteBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(payload)),
	})

	// batch processing should be fine and contain one message.
	assert.NoError(t, err)
}

func testCouchbaseOutputUpsert(payload, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! json("id") }'
content: 'root = this'
operation: 'upsert'
`, port, bucket, username, password)

	err := getOutput(t, config).WriteBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(payload)),
	})

	// batch processing should be fine and contain one message.
	assert.NoError(t, err)
}

func testCouchbaseOutputReplace(payload, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! json("id") }'
content: 'root = this'
operation: 'replace'
`, port, bucket, username, password)

	err := getOutput(t, config).WriteBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(payload)),
	})

	// batch processing should be fine and contain one message.
	assert.NoError(t, err)
}

func testCouchbaseOutputUpsertTTL(payload, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! json("id") }'
content: 'root = this'
operation: 'upsert'
ttl: 3s
`, port, bucket, username, password)

	err := getOutput(t, config).WriteBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(payload)),
	})

	// batch processing should be fine and contain one message.
	assert.NoError(t, err)
}

func testCouchbaseOutputRemove(uid, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! content() }'
operation: 'remove'
`, port, bucket, username, password)

	err := getOutput(t, config).WriteBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(uid)),
	})

	// batch processing should be fine and contain one message.
	assert.NoError(t, err)
}
