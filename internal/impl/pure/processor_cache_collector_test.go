package pure_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"

	ipure "github.com/warpstreamlabs/bento/internal/impl/pure"
)

func cacheCollectorProc(confStr string) (service.BatchProcessor, *service.Resources, error) {
	pConf, err := ipure.CacheCollectorProcessorSpec().ParseYAML(confStr, nil)
	if err != nil {
		return nil, nil, err
	}
	mgr := service.MockResources(service.MockResourcesOptAddCache("test_cache"))
	proc, err := ipure.NewCacheCollectorFromConfig(pConf, mgr)
	return proc, mgr, err
}

func TestCacheCollectorProcessor_BasicAccumulationDeleteCache(t *testing.T) {
	spec := `
resource: test_cache
key: test_key
init: root = [{"id":"0"}]
append_check: true
append_map: |
  root = this.cached.append(this.current)
flush_check: "batch_index() == 2"
flush_map: |
  root.result = this
  root.count = this.length()
flush_deletes: true
`
	processor, mgr, err := cacheCollectorProc(spec)
	require.NoError(t, err)
	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"1"}`)),
		service.NewMessage([]byte(`{"id":"2"}`)),
		service.NewMessage([]byte(`{"id":"3"}`)),
	}

	results, err := processor.ProcessBatch(t.Context(), batch)

	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Len(t, results[0], 1)

	result, err := results[0][0].AsBytes()
	require.NoError(t, err)
	expected := `{"result":[{"id":"0"},{"id":"1"},{"id":"2"},{"id":"3"}],"count":4}`
	assert.JSONEq(t, expected, string(result))

	cerr := mgr.AccessCache(t.Context(), "test_cache", func(c service.Cache) {
		_, err = c.Get(t.Context(), "test_key")
	})

	require.NoError(t, cerr)
	require.Error(t, err)
}

func TestCacheCollectorProcessor_BasicAccumulatioKeepCache(t *testing.T) {
	spec := `
resource: test_cache
key: test_key
init: root = [{"id":"0"}]
append_check: true
append_map: |
  root = this.cached.append(this.current)
flush_check: "batch_index() == 2"
flush_map: |
  root.result = this
  root.count = this.length()
flush_deletes: false
`
	processor, mgr, err := cacheCollectorProc(spec)
	require.NoError(t, err)
	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"1"}`)),
		service.NewMessage([]byte(`{"id":"2"}`)),
		service.NewMessage([]byte(`{"id":"3"}`)),
	}

	results, err := processor.ProcessBatch(t.Context(), batch)

	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Len(t, results[0], 1)

	result, err := results[0][0].AsBytes()
	require.NoError(t, err)
	assert.JSONEq(t, `{"result":[{"id":"0"},{"id":"1"},{"id":"2"},{"id":"3"}],"count":4}`, string(result))

	var data []byte

	cerr := mgr.AccessCache(t.Context(), "test_cache", func(c service.Cache) {
		data, err = c.Get(t.Context(), "test_key")
	})
	require.NoError(t, cerr)
	require.NoError(t, err)

	assert.JSONEq(t, `[{"id":"0"},{"id":"1"},{"id":"2"},{"id":"3"}]`, string(data))
}

func TestCacheCollectorProcessor_WithDifferentKeys(t *testing.T) {
	t.Run("test_with_different_keys", func(t *testing.T) {
		spec := `
resource: test_cache
key: test_${! this.group }
init: root = []
append_check: root = this.end == false
append_map: root = this.cached.append(this.current.data)
flush_check: this.end
flush_deletes: true
`
		processor, _, err := cacheCollectorProc(spec)
		require.NoError(t, err)

		batch := service.MessageBatch{
			service.NewMessage([]byte(`{"group":"a","data":{"id":"a1"},"end":false}`)),
			service.NewMessage([]byte(`{"group":"a","data":{"id":"a2"},"end":false}`)),
			service.NewMessage([]byte(`{"group":"b","data":{"id":"b1"},"end":false}`)),
			service.NewMessage([]byte(`{"group":"a","data":{"id":"a3"},"end":false}`)),
			service.NewMessage([]byte(`{"group":"b","data":{"id":"b2"},"end":false}`)),
			service.NewMessage([]byte(`{"group":"b","data":{"id":"b3"},"end":false}`)),

			// end
			service.NewMessage([]byte(`{"group":"a","end":true}`)),
			service.NewMessage([]byte(`{"group":"b","end":true}`)),
		}

		results, err := processor.ProcessBatch(t.Context(), batch)

		require.NoError(t, err)
		require.Len(t, results, 1)
		require.Len(t, results[0], 2)

		resultGroupA, err := results[0][0].AsBytes()
		require.NoError(t, err)
		assert.JSONEq(t, `[{"id":"a1"},{"id":"a2"},{"id":"a3"}]`, string(resultGroupA))

		resultGroupB, err := results[0][1].AsBytes()
		require.NoError(t, err)
		assert.JSONEq(t, `[{"id":"b1"},{"id":"b2"},{"id":"b3"}]`, string(resultGroupB))
	})
}

func TestCacheCollectorProcessor_WithMeta(t *testing.T) {
	t.Run("access_meta", func(t *testing.T) {
		spec := `
resource: test_cache
key: test_key
init: root = []
append_check: true
append_map: |
  root = this.cached.append(metadata("meta_data"))
flush_check: true
flush_deletes: true
`
		processor, _, err := cacheCollectorProc(spec)
		require.NoError(t, err)
		msg := service.NewMessage([]byte(`{"meta":true}`))
		msg.MetaSet("meta_data", "meta_value")

		batch := service.MessageBatch{
			msg,
		}

		results, err := processor.ProcessBatch(t.Context(), batch)

		require.NoError(t, err)
		require.Len(t, results, 1)
		require.Len(t, results[0], 1)

		result, err := results[0][0].AsBytes()
		require.NoError(t, err)
		assert.JSONEq(t, `["meta_value"]`, string(result))
	})
}
