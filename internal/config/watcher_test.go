package config

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/manager"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/stream"
)

func TestReaderFileWatching(t *testing.T) {
	dummyConfig := []byte(`
input:
  generate:
    mapping: 'root = "foo"'
output:
  drop: {}
`)

	confDir := t.TempDir()

	// Create an empty config file in the config folder
	confFilePath := filepath.Join(confDir, "main.yaml")
	require.NoError(t, os.WriteFile(confFilePath, []byte{}, 0o644))

	rdr := newDummyReader(confFilePath, nil)

	changeChan := make(chan struct{})
	once := sync.Once{}
	var updatedConf stream.Config
	require.NoError(t, rdr.SubscribeConfigChanges(func(conf *Type) error {
		updatedConf = conf.Config
		once.Do(func() { close(changeChan) })
		return nil
	}))

	// Watch for configuration changes
	testMgr, err := manager.New(manager.ResourceConfig{})
	require.NoError(t, err)
	require.NoError(t, rdr.BeginFileWatching(testMgr, true))

	// Overwrite original config
	require.NoError(t, os.WriteFile(confFilePath, dummyConfig, 0o644))

	// Wait for the config watcher to reload the config
	select {
	case <-changeChan:
	case <-time.After(time.Second * 5):
		require.FailNow(t, "Expected a config change to be triggered")
	}

	assert.Equal(t, "generate", updatedConf.Input.Type)
	assert.Equal(t, "drop", updatedConf.Output.Type)
}

func TestReaderFileWatchingSymlinkReplace(t *testing.T) {
	dummyConfig := []byte(`
input:
  generate:
    mapping: 'root = "foo"'
output:
  drop: {}
`)

	rootDir := t.TempDir()

	// Create a config folder
	confDir := filepath.Join(rootDir, "config")
	require.NoError(t, os.Mkdir(confDir, 0o755))

	// Create a symlink to the config folder
	confDirSymlink := filepath.Join(rootDir, "symlink")
	require.NoError(t, os.Symlink(confDir, confDirSymlink))

	// Create an empty config file in the config folder through the symlink
	confFilePath := filepath.Join(confDirSymlink, "main.yaml")
	require.NoError(t, os.WriteFile(confFilePath, []byte{}, 0o644))

	rdr := newDummyReader(confFilePath, nil)

	changeChan := make(chan struct{})
	var updatedConf stream.Config
	require.NoError(t, rdr.SubscribeConfigChanges(func(conf *Type) error {
		updatedConf = conf.Config
		close(changeChan)
		return nil
	}))

	// Watch for configuration changes
	testMgr, err := manager.New(manager.ResourceConfig{})
	require.NoError(t, err)
	require.NoError(t, rdr.BeginFileWatching(testMgr, true))

	// Create a new config folder and place in it a new copy of the config file
	newConfDir := filepath.Join(rootDir, "config_new")
	require.NoError(t, os.Mkdir(newConfDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(newConfDir, "main.yaml"), dummyConfig, 0o644))

	// Create a symlink to the new config folder
	newConfDirSymlink := filepath.Join(rootDir, "symlink_new")
	require.NoError(t, os.Symlink(newConfDir, newConfDirSymlink))

	// Overwrite the original symlink with the new symlink
	require.NoError(t, os.Rename(newConfDirSymlink, confDirSymlink))

	// Remove the original config folder to trigger a config refresh
	require.NoError(t, os.RemoveAll(confDir))

	// Wait for the config watcher to reload the config
	select {
	case <-changeChan:
	case <-time.After(time.Second * 5):
		require.FailNow(t, "Expected a config change to be triggered")
	}

	assert.Equal(t, "generate", updatedConf.Input.Type)
	assert.Equal(t, "drop", updatedConf.Output.Type)
}

func TestWatcherErrors(t *testing.T) {
	errA1 := errors.New("test a")
	errB1 := errors.New("test b")

	errA2 := noReread(errA1)
	errB2 := noReread(errB1)

	assert.True(t, ShouldReread(errA1))
	assert.True(t, ShouldReread(errB1))

	assert.False(t, ShouldReread(errA2))
	assert.False(t, ShouldReread(errB2))
	assert.False(t, ShouldReread(nil))

	assert.Equal(t, errA1.Error(), errA2.Error())
	assert.Equal(t, errB1.Error(), errB2.Error())
}

// streamChangeRecorder collects the stream configs delivered by the config
// watcher and wakes the test on each delivery.
type streamChangeRecorder struct {
	mut     sync.Mutex
	confs   map[string]*stream.Config
	changed chan struct{}
}

func newStreamChangeRecorder() *streamChangeRecorder {
	return &streamChangeRecorder{
		confs:   map[string]*stream.Config{},
		changed: make(chan struct{}, 1),
	}
}

// update is the StreamUpdateFunc registered with the reader. It never blocks
// the watcher goroutine: the signal channel is buffered and coalesces, and
// requireLabels re-reads the collected state on every wake-up, so no delivery
// is lost.
func (s *streamChangeRecorder) update(id string, conf *stream.Config) error {
	s.mut.Lock()
	s.confs[id] = conf
	s.mut.Unlock()

	select {
	case s.changed <- struct{}{}:
	default:
	}
	return nil
}

func (s *streamChangeRecorder) label(id string) (string, bool) {
	s.mut.Lock()
	defer s.mut.Unlock()
	conf, exists := s.confs[id]
	if !exists || conf == nil {
		return "", false
	}
	return conf.Output.Label, true
}

func (s *streamChangeRecorder) matches(want map[string]string) bool {
	for id, wantLabel := range want {
		label, exists := s.label(id)
		if wantLabel == "" {
			if exists {
				return false
			}
			continue
		}
		if !exists || label != wantLabel {
			return false
		}
	}
	return true
}

// requireLabels waits for the watcher to deliver each stream's expected output
// label, where "" means the stream must have been removed. The watcher makes no
// promise of exactly one callback per file change, so the wanted state is
// checked after every delivery rather than counting deliveries.
func (s *streamChangeRecorder) requireLabels(t *testing.T, want map[string]string) {
	t.Helper()

	timeout := time.After(time.Second * 5)
	for !s.matches(want) {
		select {
		case <-s.changed:
		case <-timeout:
			for id, wantLabel := range want {
				label, exists := s.label(id)
				if wantLabel == "" {
					assert.False(t, exists, "stream %v should have been removed", id)
					continue
				}
				assert.True(t, exists, "stream %v should have been updated", id)
				assert.Equal(t, wantLabel, label, "stream %v", id)
			}
			require.FailNow(t, "Timed out waiting for stream config changes")
		}
	}
}

func TestReaderStreamDirectWatching(t *testing.T) {
	confDir := t.TempDir()

	// Create an empty config file in the config folder
	require.NoError(t, os.MkdirAll(filepath.Join(confDir, "inner"), 0o755))
	confAPath := filepath.Join(confDir, "inner", "a.yaml")
	confBPath := filepath.Join(confDir, "b.yaml")
	confCPath := filepath.Join(confDir, "c.yaml")

	require.NoError(t, os.WriteFile(confAPath, []byte(`output: { label: a1, drop: {} }`), 0o644))
	require.NoError(t, os.WriteFile(confBPath, []byte(`output: { label: b1, drop: {} }`), 0o644))
	require.NoError(t, os.WriteFile(confCPath, []byte(`output: { label: c1, drop: {} }`), 0o644))

	initConfs := map[string]stream.Config{}
	rdr := newDummyReader("", nil, OptSetStreamPaths(confAPath, confBPath, confCPath))

	lints, lintWarns, err := rdr.ReadStreams(initConfs)
	require.NoError(t, err)
	require.Empty(t, lints)
	require.Empty(t, lintWarns)

	assert.Equal(t, "a1", initConfs["a"].Output.Label)
	assert.Equal(t, "b1", initConfs["b"].Output.Label)
	assert.Equal(t, "c1", initConfs["c"].Output.Label)

	changes := newStreamChangeRecorder()
	require.NoError(t, rdr.SubscribeStreamChanges(changes.update))

	// Watch for configuration changes
	testMgr, err := manager.New(manager.ResourceConfig{})
	require.NoError(t, err)
	require.NoError(t, rdr.BeginFileWatching(testMgr, true))

	require.NoError(t, os.WriteFile(confAPath, []byte(`output: { label: a2, drop: {} }`), 0o644))
	require.NoError(t, os.WriteFile(confBPath, []byte(`output: { label: b2, drop: {} }`), 0o644))
	require.NoError(t, os.WriteFile(confCPath, []byte(`output: { label: c2, drop: {} }`), 0o644))

	changes.requireLabels(t, map[string]string{"a": "a2", "b": "b2", "c": "c2"})

	// Update two and delete one of the files
	require.NoError(t, os.WriteFile(confAPath, []byte(`output: { label: a3, drop: {} }`), 0o644))
	require.NoError(t, os.Remove(confBPath))
	require.NoError(t, os.WriteFile(confCPath, []byte(`output: { label: c3, drop: {} }`), 0o644))

	changes.requireLabels(t, map[string]string{"a": "a3", "b": "", "c": "c3"})
}

func TestReaderStreamWildcardWatching(t *testing.T) {
	confDir := t.TempDir()

	// Create an empty config file in the config folder
	require.NoError(t, os.MkdirAll(filepath.Join(confDir, "inner"), 0o755))
	confAPath := filepath.Join(confDir, "a.yaml")
	confBPath := filepath.Join(confDir, "b.yaml")
	confCPath := filepath.Join(confDir, "c.yaml")

	require.NoError(t, os.WriteFile(confAPath, []byte(`output: { label: a1, drop: {} }`), 0o644))
	require.NoError(t, os.WriteFile(confCPath, []byte(`output: { label: c1, drop: {} }`), 0o644))

	initConfs := map[string]stream.Config{}
	rdr := newDummyReader("", nil, OptSetStreamPaths(confDir+"/*.yaml"))

	lints, lintWarns, err := rdr.ReadStreams(initConfs)
	require.NoError(t, err)
	require.Empty(t, lints)
	require.Empty(t, lintWarns)

	assert.Equal(t, "a1", initConfs["a"].Output.Label)
	assert.NotContains(t, initConfs, "b")
	assert.Equal(t, "c1", initConfs["c"].Output.Label)

	changes := newStreamChangeRecorder()
	require.NoError(t, rdr.SubscribeStreamChanges(changes.update))

	// Watch for configuration changes
	testMgr, err := manager.New(manager.ResourceConfig{})
	require.NoError(t, err)
	require.NoError(t, rdr.BeginFileWatching(testMgr, true))

	require.NoError(t, os.WriteFile(confAPath, []byte(`output: { label: a2, drop: {} }`), 0o644))
	require.NoError(t, os.WriteFile(confBPath, []byte(`output: { label: b2, drop: {} }`), 0o644))
	require.NoError(t, os.WriteFile(confCPath, []byte(`output: { label: c2, drop: {} }`), 0o644))

	changes.requireLabels(t, map[string]string{"a": "a2", "b": "b2", "c": "c2"})

	// Update two and delete one of the files
	require.NoError(t, os.WriteFile(confAPath, []byte(`output: { label: a3, drop: {} }`), 0o644))
	require.NoError(t, os.WriteFile(confBPath, []byte(`output: { label: b3, drop: {} }`), 0o644))
	require.NoError(t, os.Remove(confCPath))

	changes.requireLabels(t, map[string]string{"a": "a3", "b": "b3", "c": ""})
}

func TestReaderStreamDirWatching(t *testing.T) {
	confDir := t.TempDir()

	// Create an empty config file in the config folder
	require.NoError(t, os.MkdirAll(filepath.Join(confDir, "inner"), 0o755))
	confAPath := filepath.Join(confDir, "inner", "a.yaml")
	confBPath := filepath.Join(confDir, "b.yaml")
	confCPath := filepath.Join(confDir, "c.yaml")

	require.NoError(t, os.WriteFile(confAPath, []byte(`output: { label: a1, drop: {} }`), 0o644))
	require.NoError(t, os.WriteFile(confCPath, []byte(`output: { label: c1, drop: {} }`), 0o644))

	initConfs := map[string]stream.Config{}
	rdr := newDummyReader("", nil, OptSetStreamPaths(confDir))

	lints, lintWarns, err := rdr.ReadStreams(initConfs)
	require.NoError(t, err)
	require.Empty(t, lints)
	require.Empty(t, lintWarns)

	assert.Equal(t, "a1", initConfs["inner_a"].Output.Label)
	assert.NotContains(t, initConfs, "b")
	assert.Equal(t, "c1", initConfs["c"].Output.Label)

	changes := newStreamChangeRecorder()
	require.NoError(t, rdr.SubscribeStreamChanges(changes.update))

	// Watch for configuration changes
	testMgr, err := manager.New(manager.ResourceConfig{})
	require.NoError(t, err)
	require.NoError(t, rdr.BeginFileWatching(testMgr, true))

	require.NoError(t, os.WriteFile(confAPath, []byte(`output: { label: a2, drop: {} }`), 0o644))
	require.NoError(t, os.WriteFile(confBPath, []byte(`output: { label: b2, drop: {} }`), 0o644))
	require.NoError(t, os.WriteFile(confCPath, []byte(`output: { label: c2, drop: {} }`), 0o644))

	changes.requireLabels(t, map[string]string{"inner_a": "a2", "b": "b2", "c": "c2"})

	// Update two and delete one of the files
	require.NoError(t, os.WriteFile(confAPath, []byte(`output: { label: a3, drop: {} }`), 0o644))
	require.NoError(t, os.WriteFile(confBPath, []byte(`output: { label: b3, drop: {} }`), 0o644))
	require.NoError(t, os.Remove(confCPath))

	changes.requireLabels(t, map[string]string{"inner_a": "a3", "b": "b3", "c": ""})
}

func TestReaderWatcherRace(t *testing.T) {
	t.Skip()
	confDir := t.TempDir()

	// Create an empty config file in the config folder
	require.NoError(t, os.MkdirAll(filepath.Join(confDir, "inner"), 0o755))
	confAPath := filepath.Join(confDir, "inner", "a.yaml")
	confBPath := filepath.Join(confDir, "b.yaml")
	confCPath := filepath.Join(confDir, "c.yaml")

	require.NoError(t, os.WriteFile(confAPath, []byte(`output: { label: a1, drop: {} }`), 0o644))
	require.NoError(t, os.WriteFile(confCPath, []byte(`output: { label: c1, drop: {} }`), 0o644))

	initConfs := map[string]stream.Config{}
	rdr := newDummyReader("", nil, OptSetStreamPaths(confDir))

	lints, lintWarns, err := rdr.ReadStreams(initConfs)
	require.NoError(t, err)
	require.Empty(t, lints)
	require.Empty(t, lintWarns)

	assert.Equal(t, "a1", initConfs["inner_a"].Output.Label)
	assert.NotContains(t, initConfs, "b")
	assert.Equal(t, "c1", initConfs["c"].Output.Label)

	time.Sleep(time.Second)

	// Update all files
	require.NoError(t, os.WriteFile(confAPath, []byte(`output: { label: a2, drop: {} }`), 0o644))
	require.NoError(t, os.WriteFile(confBPath, []byte(`output: { label: b2, drop: {} }`), 0o644))

	changes := newStreamChangeRecorder()
	require.NoError(t, rdr.SubscribeStreamChanges(changes.update))

	// Watch for configuration changes
	testMgr, err := manager.New(manager.ResourceConfig{})
	require.NoError(t, err)
	require.NoError(t, rdr.BeginFileWatching(testMgr, true))

	changes.requireLabels(t, map[string]string{"inner_a": "a2", "b": "b2", "c": ""})

	// Update two and delete one of the files
	require.NoError(t, os.WriteFile(confAPath, []byte(`output: { label: a3, drop: {} }`), 0o644))
	require.NoError(t, os.WriteFile(confBPath, []byte(`output: { label: b3, drop: {} }`), 0o644))
	require.NoError(t, os.Remove(confCPath))

	changes.requireLabels(t, map[string]string{"inner_a": "a3", "b": "b3", "c": ""})
}

func TestReaderResourceWildcardWatching(t *testing.T) {
	confDir := t.TempDir()

	// Create an empty config file in the config folder
	require.NoError(t, os.MkdirAll(filepath.Join(confDir, "inner"), 0o755))
	confAPath := filepath.Join(confDir, "a.yaml")
	confBPath := filepath.Join(confDir, "b.yaml")
	confCPath := filepath.Join(confDir, "c.yaml")

	procConfig := func(id, value string) []byte {
		return fmt.Appendf(nil, `
processor_resources:
  - label: %v
    mapping: 'root = content() + " %v"'
`, id, value)
	}

	require.NoError(t, os.WriteFile(confAPath, procConfig("a", "a1"), 0o644))
	require.NoError(t, os.WriteFile(confCPath, procConfig("c", "c1"), 0o644))

	rdr := newDummyReader("", []string{confDir + "/*.yaml"})

	conf, _, lints, lintWarns, err := rdr.Read()
	require.NoError(t, err)
	require.Empty(t, lints)
	require.Empty(t, lintWarns)

	require.Len(t, conf.ResourceProcessors, 2)
	require.Equal(t, "a", conf.ResourceProcessors[0].Label)
	require.Equal(t, "c", conf.ResourceProcessors[1].Label)

	// Ignore
	require.NoError(t, rdr.SubscribeConfigChanges(func(conf *Type) error {
		return nil
	}))

	// Watch for configuration changes
	testMgr, err := manager.New(manager.ResourceConfig{})
	require.NoError(t, err)
	require.NoError(t, rdr.BeginFileWatching(testMgr, true))

	require.NoError(t, os.WriteFile(confAPath, procConfig("a", "a2"), 0o644))
	require.NoError(t, os.WriteFile(confBPath, procConfig("b", "b2"), 0o644))
	require.NoError(t, os.WriteFile(confCPath, procConfig("c", "c2"), 0o644))

	tCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	require.Eventually(t, func() bool {
		return testMgr.ProbeProcessor("a") && testMgr.ProbeProcessor("b") && testMgr.ProbeProcessor("c")
	}, time.Second*5, time.Millisecond*100)

	runProc := func(name string) string {
		var res string
		require.NoError(t, testMgr.AccessProcessor(tCtx, name, func(p processor.V1) {
			resBatch, err := p.ProcessBatch(tCtx, message.Batch{message.NewPart([]byte("hello world"))})
			require.NoError(t, err)
			require.Len(t, resBatch, 1)
			require.Len(t, resBatch[0], 1)
			res = string(resBatch[0][0].AsBytes())
		}))
		return res
	}

	assert.Equal(t, "hello world a2", runProc("a"))
	assert.Equal(t, "hello world b2", runProc("b"))
	assert.Equal(t, "hello world c2", runProc("c"))

	// Update two and delete one of the files
	require.NoError(t, os.WriteFile(confAPath, procConfig("a", "a3"), 0o644))
	require.NoError(t, os.WriteFile(confBPath, procConfig("b", "b3"), 0o644))
	require.NoError(t, os.Remove(confCPath))

	require.Eventually(t, func() bool {
		return testMgr.ProbeProcessor("a") && testMgr.ProbeProcessor("b") && !testMgr.ProbeProcessor("c")
	}, time.Second*5, time.Millisecond*100)

	assert.Equal(t, "hello world a3", runProc("a"))
	assert.Equal(t, "hello world b3", runProc("b"))
}
