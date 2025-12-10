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

func TestProcessorConfigLinting(t *testing.T) {
	configTests := []struct {
		name        string
		config      string
		errContains string
	}{
		{
			name: "get content not required",
			config: `
couchbase:
  url: 'url'
  bucket: 'bucket'
  id: '${! json("id") }'
  operation: 'get'
`,
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
	}

	env := service.NewEnvironment()
	for _, test := range configTests {
		t.Run(test.name, func(t *testing.T) {
			strm := env.NewStreamBuilder()
			err := strm.AddProcessorYAML(test.config)
			if test.errContains == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			}
		})
	}
}

func getProc(tb testing.TB, config string) *couchbase.Processor {
	tb.Helper()

	confSpec := couchbase.ProcessorConfig()
	env := service.NewEnvironment()

	pConf, err := confSpec.ParseYAML(config, env)
	require.NoError(tb, err)
	proc, err := couchbase.NewProcessor(pConf, service.MockResources())
	require.NoError(tb, err)
	require.NotNil(tb, proc)

	return proc
}

func testCouchbaseProcessorInsert(payload, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! json("id") }'
content: 'root = this'
operation: 'insert'
`, port, bucket, username, password)

	msgOut, err := getProc(t, config).ProcessBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(payload)),
	})

	// batch processing should be fine and contain one message.
	assert.NoError(t, err)
	assert.Len(t, msgOut, 1)
	assert.Len(t, msgOut[0], 1)

	// check CAS
	cas, ok := msgOut[0][0].MetaGetMut(couchbase.MetaCASKey)
	assert.True(t, ok)
	assert.NotEmpty(t, cas)

	// message content should stay the same.
	dataOut, err := msgOut[0][0].AsBytes()
	assert.NoError(t, err)
	assert.JSONEq(t, payload, string(dataOut))
}

func testCouchbaseProcessorUpsert(payload, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! json("id") }'
content: 'root = this'
operation: 'upsert'
`, port, bucket, username, password)

	msgOut, err := getProc(t, config).ProcessBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(payload)),
	})

	// batch processing should be fine and contain one message.
	assert.NoError(t, err)
	assert.Len(t, msgOut, 1)
	assert.Len(t, msgOut[0], 1)

	// check CAS
	cas, ok := msgOut[0][0].MetaGetMut(couchbase.MetaCASKey)
	assert.True(t, ok)
	assert.NotEmpty(t, cas)

	// message content should stay the same.
	dataOut, err := msgOut[0][0].AsBytes()
	assert.NoError(t, err)
	assert.JSONEq(t, payload, string(dataOut))
}

func testCouchbaseProcessorReplace(payload, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! json("id") }'
content: 'root = this'
operation: 'replace'
`, port, bucket, username, password)

	msgOut, err := getProc(t, config).ProcessBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(payload)),
	})

	// batch processing should be fine and contain one message.
	assert.NoError(t, err)
	assert.Len(t, msgOut, 1)
	assert.Len(t, msgOut[0], 1)

	// check CAS
	cas, ok := msgOut[0][0].MetaGetMut(couchbase.MetaCASKey)
	assert.True(t, ok)
	assert.NotEmpty(t, cas)

	// message content should stay the same.
	dataOut, err := msgOut[0][0].AsBytes()
	assert.NoError(t, err)
	assert.JSONEq(t, payload, string(dataOut))
}

func testCouchbaseProcessorGet(uid, payload, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! content() }'
operation: 'get'
`, port, bucket, username, password)

	msgOut, err := getProc(t, config).ProcessBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(uid)),
	})

	// batch processing should be fine and contain one message.
	assert.NoError(t, err)
	assert.Len(t, msgOut, 1)
	assert.Len(t, msgOut[0], 1)

	// check CAS
	cas, ok := msgOut[0][0].MetaGetMut(couchbase.MetaCASKey)
	assert.True(t, ok)
	assert.NotEmpty(t, cas)

	// message should contain expected payload.
	dataOut, err := msgOut[0][0].AsBytes()
	assert.NoError(t, err)
	assert.JSONEq(t, payload, string(dataOut))
}

func testCouchbaseProcessorRemove(uid, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! content() }'
operation: 'remove'
`, port, bucket, username, password)

	msgOut, err := getProc(t, config).ProcessBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(uid)),
	})

	// batch processing should be fine and contain one message.
	assert.NoError(t, err)
	assert.Len(t, msgOut, 1)
	assert.Len(t, msgOut[0], 1)

	// check CAS
	cas, ok := msgOut[0][0].MetaGetMut(couchbase.MetaCASKey)
	assert.True(t, ok)
	assert.NotEmpty(t, cas)

	// message content should stay the same.
	dataOut, err := msgOut[0][0].AsBytes()
	assert.NoError(t, err)
	assert.Equal(t, uid, string(dataOut))
}

func testCouchbaseProcessorGetMissing(uid, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! content() }'
operation: 'get'
`, port, bucket, username, password)

	msgOut, err := getProc(t, config).ProcessBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(uid)),
	})

	// batch processing should be fine and contain one message.
	assert.NoError(t, err)
	assert.Len(t, msgOut, 1)
	assert.Len(t, msgOut[0], 1)

	// message should contain an error.
	assert.Error(t, msgOut[0][0].GetError())

	// message content should stay the same.
	dataOut, err := msgOut[0][0].AsBytes()
	assert.NoError(t, err)
	assert.Equal(t, uid, string(dataOut))
}

func testCouchbaseProcessorUpsertTTL(payload, bucket, port string, t *testing.T) {
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

	msgOut, err := getProc(t, config).ProcessBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(payload)),
	})

	// batch processing should be fine and contain one message.
	assert.NoError(t, err)
	assert.Len(t, msgOut, 1)
	assert.Len(t, msgOut[0], 1)

	// check CAS
	cas, ok := msgOut[0][0].MetaGetMut(couchbase.MetaCASKey)
	assert.True(t, ok)
	assert.NotEmpty(t, cas)

	// message content should stay the same.
	dataOut, err := msgOut[0][0].AsBytes()
	assert.NoError(t, err)
	assert.JSONEq(t, payload, string(dataOut))
}

func testCouchbaseProcessorCounter(uid, operation, value, expected, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! meta("id") }'
content: 'root = this.or(null)'
operation: '%s'
`, port, bucket, username, password, operation)

	msg := service.NewMessage([]byte(value))
	msg.MetaSetMut("id", uid)
	msgOut, err := getProc(t, config).ProcessBatch(context.Background(), service.MessageBatch{
		msg,
	})

	// batch processing should be fine and contain one message.
	assert.NoError(t, err)
	assert.Len(t, msgOut, 1)
	assert.Len(t, msgOut[0], 1)
	assert.NoError(t, msgOut[0][0].GetError())

	// check CAS
	cas, ok := msgOut[0][0].MetaGetMut(couchbase.MetaCASKey)
	assert.True(t, ok)
	assert.NotEmpty(t, cas)

	// message content should be the counter value
	dataOut, err := msgOut[0][0].AsBytes()
	assert.NoError(t, err)
	// The result of increment is the new value.
	assert.Equal(t, expected, string(dataOut))
}

func testCouchbaseProcessorCounterError(uid, operation, value, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! meta("id") }'
content: 'root = this.or(null)'
operation: '%s'
`, port, bucket, username, password, operation)

	msg := service.NewMessage([]byte(value))
	msg.MetaSetMut("id", uid)
	_, err := getProc(t, config).ProcessBatch(context.Background(), service.MessageBatch{
		msg,
	})

	// batch processing should not be fine.
	require.Error(t, err)
}
