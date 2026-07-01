package config

import (
	"testing"
	"testing/fstest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
	"github.com/warpstreamlabs/bento/internal/manager"
	"github.com/warpstreamlabs/bento/internal/stream"
)

func newDummyReader(confFilePath string, resourcePaths []string, opts ...OptFunc) *Reader {
	rdr := NewReader(confFilePath, resourcePaths, opts...)
	rdr.changeDelayPeriod = 1 * time.Millisecond
	rdr.changeFlushPeriod = 1 * time.Millisecond
	rdr.filesRefreshPeriod = 1 * time.Millisecond
	return rdr
}

func TestCustomFileSync(t *testing.T) {
	testFS := &ifs.TestFS{MapFS: fstest.MapFS{
		"foo_main.yaml": &fstest.MapFile{
			Data: []byte(`
input:
  label: fooin
  inproc: foo

output:
  label: fooout
  inproc: bar
`),
		},
		"a.yaml": &fstest.MapFile{
			Data: []byte(`
processor_resources:
  - label: a
    mapping: 'root = content() + " a1"'
  - label: b
    mapping: 'root = content() + " b1"'
`),
		},
		"b.yaml": &fstest.MapFile{
			Data: []byte(`
processor_resources:
  - label: c
    mapping: 'root = content() + " c1"'
  - label: d
    mapping: 'root = content() + " d1"'
`),
		},
	}}
	rdr := newDummyReader("foo_main.yaml", []string{"a.yaml", "b.yaml"}, OptUseFS(testFS))

	conf, _, lints, lintWarns, err := rdr.Read()
	require.NoError(t, err)
	require.Empty(t, lints)
	require.Empty(t, lintWarns)

	assert.Equal(t, "fooin", conf.Input.Label)
	assert.Equal(t, "fooout", conf.Output.Label)

	assert.Len(t, conf.ResourceProcessors, 4)
	assert.Equal(t, "a", conf.ResourceProcessors[0].Label)
	assert.Equal(t, "b", conf.ResourceProcessors[1].Label)
	assert.Equal(t, "c", conf.ResourceProcessors[2].Label)
	assert.Equal(t, "d", conf.ResourceProcessors[3].Label)
}

func TestCustomFileChangeMain(t *testing.T) {
	testFS := &ifs.TestFS{MapFS: fstest.MapFS{
		"foo_main.yaml": &fstest.MapFile{
			Data: []byte(`
input:
  label: fooin
  inproc: foo

output:
  label: fooout
  inproc: bar

processor_resources:
  - label: a
    mapping: 'root = content() + " a1"'
  - label: b
    mapping: 'root = content() + " b1"'
`),
		},
		"bar_main.yaml": &fstest.MapFile{
			Data: []byte(`
input:
  label: foointwo
  inproc: foo

output:
  label: fooouttwo
  inproc: bar

processor_resources:
  - label: c
    mapping: 'root = content() + " c1"'
  - label: d
    mapping: 'root = content() + " d1"'
`),
		},
	}}
	rdr := newDummyReader("foo_main.yaml", nil, OptUseFS(testFS))

	conf, _, lints, lintWarns, err := rdr.Read()
	require.NoError(t, err)
	require.Empty(t, lints)
	require.Empty(t, lintWarns)

	assert.Equal(t, "fooin", conf.Input.Label)
	assert.Equal(t, "fooout", conf.Output.Label)

	assert.Len(t, conf.ResourceProcessors, 2)
	assert.Equal(t, "a", conf.ResourceProcessors[0].Label)
	assert.Equal(t, "b", conf.ResourceProcessors[1].Label)

	// Watch for configuration changes
	testMgr, err := manager.New(conf.ResourceConfig)
	require.NoError(t, err)

	changeChan := make(chan struct{})
	var updatedConf stream.Config
	require.NoError(t, rdr.SubscribeConfigChanges(func(conf *Type) error {
		updatedConf = conf.Config
		close(changeChan)
		return nil
	}))

	assert.True(t, testMgr.ProbeProcessor("a"))
	assert.True(t, testMgr.ProbeProcessor("b"))
	assert.False(t, testMgr.ProbeProcessor("c"))
	assert.False(t, testMgr.ProbeProcessor("d"))

	require.NoError(t, rdr.TriggerMainUpdate(testMgr, true, "bar_main.yaml"))

	// Wait for the config watcher to reload the config
	select {
	case <-changeChan:
	case <-time.After(time.Second * 5):
		require.FailNow(t, "Expected a config change to be triggered")
	}

	assert.Equal(t, "foointwo", updatedConf.Input.Label)
	assert.Equal(t, "fooouttwo", updatedConf.Output.Label)

	assert.False(t, testMgr.ProbeProcessor("a"))
	assert.False(t, testMgr.ProbeProcessor("b"))
	assert.True(t, testMgr.ProbeProcessor("c"))
	assert.True(t, testMgr.ProbeProcessor("d"))
}

func TestCustomFileStartEmpty(t *testing.T) {
	testFS := &ifs.TestFS{MapFS: fstest.MapFS{
		"foo_main.yaml": &fstest.MapFile{
			Data: []byte(`
input:
  label: fooin
  inproc: foo

output:
  label: fooout
  inproc: bar
`),
		},
		"a.yaml": &fstest.MapFile{
			Data: []byte(`
processor_resources:
  - label: a
    mapping: 'root = content() + " a1"'
  - label: b
    mapping: 'root = content() + " b1"'
`),
		},
		"b.yaml": &fstest.MapFile{
			Data: []byte(`
processor_resources:
  - label: c
    mapping: 'root = content() + " c1"'
  - label: d
    mapping: 'root = content() + " d1"'
`),
		},
	}}

	rdr := newDummyReader("", nil, OptUseFS(testFS))

	// Watch for configuration changes
	testMgr, err := manager.New(manager.ResourceConfig{})
	require.NoError(t, err)

	changeChan := make(chan struct{})
	var conf stream.Config
	require.NoError(t, rdr.SubscribeConfigChanges(func(c *Type) error {
		conf = c.Config
		close(changeChan)
		return nil
	}))

	require.NoError(t, rdr.TriggerResourceUpdate(testMgr, true, "a.yaml"))
	require.NoError(t, rdr.TriggerResourceUpdate(testMgr, true, "b.yaml"))

	require.NoError(t, rdr.TriggerMainUpdate(testMgr, true, "foo_main.yaml"))

	assert.Equal(t, "fooin", conf.Input.Label)
	assert.Equal(t, "fooout", conf.Output.Label)

	assert.True(t, testMgr.ProbeProcessor("a"))
	assert.True(t, testMgr.ProbeProcessor("b"))
	assert.True(t, testMgr.ProbeProcessor("c"))
	assert.True(t, testMgr.ProbeProcessor("d"))
}
