package pure_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"

	_ "github.com/warpstreamlabs/bento/internal/impl/pure"
)

func TestCacheSet(t *testing.T) {
	mgr := mock.NewManager()
	mgr.Caches["foocache"] = map[string]mock.CacheItem{}

	conf, err := testutil.ProcessorFromYAML(`
cache:
  operator: set
  key: ${!json("key")}
  value: ${!json("value")}
  resource: foocache
`)
	require.NoError(t, err)

	proc, err := mgr.NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	input := message.QuickBatch([][]byte{
		[]byte(`{"key":"1","value":"foo 1"}`),
		[]byte(`{"key":"2","value":"foo 2"}`),
		[]byte(`{"key":"1","value":"foo 3"}`),
	})

	output, res := proc.ProcessBatch(context.Background(), input)
	if res != nil {
		t.Fatal(res)
	}

	if len(output) != 1 {
		t.Fatalf("Wrong count of result messages: %v", len(output))
	}

	if exp, act := message.GetAllBytes(input), message.GetAllBytes(output[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result messages: %s != %s", act, exp)
	}

	actV, ok := mgr.Caches["foocache"]["1"]
	require.True(t, ok)
	assert.Equal(t, "foo 3", actV.Value)

	actV, ok = mgr.Caches["foocache"]["2"]
	require.True(t, ok)
	assert.Equal(t, "foo 2", actV.Value)
}

func TestCacheAdd(t *testing.T) {
	mgr := mock.NewManager()
	mgr.Caches["foocache"] = map[string]mock.CacheItem{}

	conf, err := testutil.ProcessorFromYAML(`
cache:
  key: ${!json("key")}
  value: ${!json("value")}
  resource: foocache
  operator: add
`)
	require.NoError(t, err)

	proc, err := mgr.NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	input := message.QuickBatch([][]byte{
		[]byte(`{"key":"1","value":"foo 1"}`),
		[]byte(`{"key":"2","value":"foo 2"}`),
		[]byte(`{"key":"1","value":"foo 3"}`),
	})

	output, res := proc.ProcessBatch(context.Background(), input)
	if res != nil {
		t.Fatal(res)
	}

	if len(output) != 1 {
		t.Fatalf("Wrong count of result messages: %v", len(output))
	}

	if exp, act := message.GetAllBytes(input), message.GetAllBytes(output[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result messages: %s != %s", act, exp)
	}

	assert.NoError(t, output[0].Get(0).ErrorGet())
	assert.NoError(t, output[0].Get(1).ErrorGet())
	assert.Error(t, output[0].Get(2).ErrorGet())

	actV, ok := mgr.Caches["foocache"]["1"]
	require.True(t, ok)
	assert.Equal(t, "foo 1", actV.Value)

	actV, ok = mgr.Caches["foocache"]["2"]
	require.True(t, ok)
	assert.Equal(t, "foo 2", actV.Value)
}

func TestCacheGet(t *testing.T) {
	mgr := mock.NewManager()
	mgr.Caches["foocache"] = map[string]mock.CacheItem{
		"1": {Value: "foo 1"},
		"2": {Value: "foo 2"},
	}

	conf, err := testutil.ProcessorFromYAML(`
cache:
  operator: get
  key: ${!json("key")}
  resource: foocache
`)
	require.NoError(t, err)

	proc, err := mgr.NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	input := message.QuickBatch([][]byte{
		[]byte(`{"key":"1"}`),
		[]byte(`{"key":"2"}`),
		[]byte(`{"key":"3"}`),
	})
	expParts := [][]byte{
		[]byte(`foo 1`),
		[]byte(`foo 2`),
		[]byte(`{"key":"3"}`),
	}

	output, res := proc.ProcessBatch(context.Background(), input)
	if res != nil {
		t.Fatal(res)
	}

	if len(output) != 1 {
		t.Fatalf("Wrong count of result messages: %v", len(output))
	}

	if exp, act := expParts, message.GetAllBytes(output[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result messages: %s != %s", act, exp)
	}

	assert.NoError(t, output[0].Get(0).ErrorGet())
	assert.NoError(t, output[0].Get(1).ErrorGet())
	assert.Error(t, output[0].Get(2).ErrorGet())
}

func TestCacheDelete(t *testing.T) {
	mgr := mock.NewManager()
	mgr.Caches["foocache"] = map[string]mock.CacheItem{
		"1": {Value: "foo 1"},
		"2": {Value: "foo 2"},
		"3": {Value: "foo 3"},
	}

	conf, err := testutil.ProcessorFromYAML(`
cache:
  operator: delete
  key: ${!json("key")}
  resource: foocache
`)
	require.NoError(t, err)

	proc, err := mgr.NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	input := message.QuickBatch([][]byte{
		[]byte(`{"key":"1"}`),
		[]byte(`{"key":"3"}`),
		[]byte(`{"key":"4"}`),
	})

	output, res := proc.ProcessBatch(context.Background(), input)
	if res != nil {
		t.Fatal(res)
	}

	if len(output) != 1 {
		t.Fatalf("Wrong count of result messages: %v", len(output))
	}

	if exp, act := message.GetAllBytes(input), message.GetAllBytes(output[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result messages: %s != %s", act, exp)
	}

	assert.NoError(t, output[0].Get(0).ErrorGet())
	assert.NoError(t, output[0].Get(1).ErrorGet())
	assert.NoError(t, output[0].Get(2).ErrorGet())

	_, ok := mgr.Caches["foocache"]["1"]
	require.False(t, ok)

	actV, ok := mgr.Caches["foocache"]["2"]
	require.True(t, ok)
	assert.Equal(t, "foo 2", actV.Value)

	_, ok = mgr.Caches["foocache"]["3"]
	require.False(t, ok)
}
