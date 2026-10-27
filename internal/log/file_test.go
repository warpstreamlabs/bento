package log

import (
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
)

func TestRotatingWriterDefaults(t *testing.T) {
	f := NewConfig().File
	f.Path = filepath.Join(t.TempDir(), "bento.log")
	f.Rotate = true
	f.RotateMaxAge = 7

	w := newRotatingWriter(f)
	t.Cleanup(func() { _ = w.Close() })

	assert.Equal(t, f.Path, w.Filename)
	assert.Equal(t, 10, w.MaxSize)
	assert.Equal(t, 7, w.MaxAge)
	assert.Equal(t, 1, w.MaxBackups)
	assert.True(t, w.Compress)
	assert.False(t, w.LocalTime)
}

func TestRotatingWriterMaxAgeRemovesOldBackups(t *testing.T) {
	dir := t.TempDir()

	oldBackup := filepath.Join(dir, "bento-2020-01-01T00-00-00.000.log.gz")
	require.NoError(t, os.WriteFile(oldBackup, []byte("old backup"), 0o666))

	f := NewConfig().File
	f.Path = filepath.Join(dir, "bento.log")
	f.Rotate = true
	f.RotateMaxAge = 7
	f.RotateMaxBackups = 200 // ensure file is deleted by MaxAge policy
	// Compression is covered by the end-to-end tests below; turning it off
	// here keeps this test to the max-age cleanup it's meant to check,
	// rather than also racing the cleanup against the mill goroutine still
	// gzipping the backup created by Rotate() below.
	f.RotateCompress = false

	w := newRotatingWriter(f)
	t.Cleanup(func() { _ = w.Close() })

	_, err := w.Write([]byte("a line\n"))
	require.NoError(t, err)

	require.NoError(t, w.Rotate())

	require.Eventually(t, func() bool {
		_, err := os.Stat(oldBackup)
		return os.IsNotExist(err)
	}, time.Second*5, time.Millisecond*20)
}

func TestRotatingWriterCustomValues(t *testing.T) {
	tests := []struct {
		name string
		file File
	}{
		{
			name: "small, few backups, uncompressed",
			file: File{
				Path:             "bento.log",
				RotateMaxSize:    5,
				RotateMaxAge:     30,
				RotateMaxBackups: 3,
				RotateCompress:   false,
			},
		},
		{
			name: "large, unlimited backups, compressed",
			file: File{
				Path:             "/var/log/bento.log",
				RotateMaxSize:    500,
				RotateMaxAge:     0,
				RotateMaxBackups: 0,
				RotateCompress:   true,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			w := newRotatingWriter(test.file)
			t.Cleanup(func() { _ = w.Close() })

			assert.Equal(t, test.file.Path, w.Filename)
			assert.Equal(t, test.file.RotateMaxSize, w.MaxSize)
			assert.Equal(t, test.file.RotateMaxAge, w.MaxAge)
			assert.Equal(t, test.file.RotateMaxBackups, w.MaxBackups)
			assert.Equal(t, test.file.RotateCompress, w.Compress)
		})
	}
}

func fileConfigFromYAML(t *testing.T, yamlStr string) Config {
	t.Helper()

	node, err := docs.UnmarshalYAML([]byte(yamlStr))
	require.NoError(t, err)

	pConf, err := Spec().ParsedConfigFromAny(node)
	require.NoError(t, err)

	conf, err := FromParsed(pConf)
	require.NoError(t, err)

	return conf
}

func TestFromParsedFileDefaults(t *testing.T) {
	conf := fileConfigFromYAML(t, `
file:
  path: bento.log
  rotate: true
`)

	assert.Equal(t, "bento.log", conf.File.Path)
	assert.True(t, conf.File.Rotate)
	assert.Equal(t, 0, conf.File.RotateMaxAge)
	assert.Equal(t, 10, conf.File.RotateMaxSize)
	assert.Equal(t, 1, conf.File.RotateMaxBackups)
	assert.True(t, conf.File.RotateCompress)
}

func TestFromParsedFileAllFields(t *testing.T) {
	conf := fileConfigFromYAML(t, `
file:
  path: bento.log
  rotate: true
  rotate_max_age_days: 14
  rotate_max_size_mb: 50
  rotate_max_backups: 5
  rotate_compress: false
`)

	assert.Equal(t, "bento.log", conf.File.Path)
	assert.True(t, conf.File.Rotate)
	assert.Equal(t, 14, conf.File.RotateMaxAge)
	assert.Equal(t, 50, conf.File.RotateMaxSize)
	assert.Equal(t, 5, conf.File.RotateMaxBackups)
	assert.False(t, conf.File.RotateCompress)
}

func TestFileRotationLint(t *testing.T) {
	node, err := docs.UnmarshalYAML([]byte(`
file:
  path: bento.log
  rotate: true
  rotate_max_size_mb: 0
  rotate_max_backups: -1
`))
	require.NoError(t, err)

	lintCtx := docs.NewLintContext(docs.NewLintConfig(docs.NewMappedDocsProvider()))
	lints := Spec().LintYAML(lintCtx, node)

	var messages []string
	for _, l := range lints {
		messages = append(messages, l.What)
	}

	assert.Contains(t, messages, `rotate_max_size_mb must be at least 1`)
	assert.Contains(t, messages, `rotate_max_backups cannot be negative`)
}

func TestNewRotationValidation(t *testing.T) {
	tests := []struct {
		name string
		file File
		err  string
	}{
		{
			name: "zero max size",
			file: File{
				RotateMaxSize:    0,
				RotateMaxBackups: 1,
			},
			err: "rotate_max_size_mb must be at least 1",
		},
		{
			name: "negative max backups",
			file: File{
				RotateMaxSize:    10,
				RotateMaxBackups: -1,
			},
			err: "rotate_max_backups cannot be negative",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conf := NewConfig()
			conf.File.Path = filepath.Join(t.TempDir(), "bento.log")
			conf.File.Rotate = true
			conf.File.RotateMaxSize = test.file.RotateMaxSize
			conf.File.RotateMaxBackups = test.file.RotateMaxBackups

			var buf bytes.Buffer
			_, err := New(&buf, ifs.OS(), conf)
			require.EqualError(t, err, test.err)
		})
	}
}

func TestRotatingWriterPrunesOldestBackups(t *testing.T) {
	dir := t.TempDir()

	backup01 := filepath.Join(dir, "bento-2020-01-01T00-00-00.000.log")
	backup02 := filepath.Join(dir, "bento-2020-01-02T00-00-00.000.log")
	backup03 := filepath.Join(dir, "bento-2020-01-03T00-00-00.000.log")
	require.NoError(t, os.WriteFile(backup01, []byte("backup one"), 0o666))
	require.NoError(t, os.WriteFile(backup02, []byte("backup two"), 0o666))
	require.NoError(t, os.WriteFile(backup03, []byte("backup three"), 0o666))

	w := newRotatingWriter(File{
		Path:             filepath.Join(dir, "bento.log"),
		RotateMaxBackups: 2,
		RotateCompress:   false,
		RotateMaxAge:     0,
		RotateMaxSize:    10,
	})
	t.Cleanup(func() { _ = w.Close() })

	_, err := w.Write([]byte("a line\n"))
	require.NoError(t, err)

	require.NoError(t, w.Rotate())

	var backups []string
	require.Eventually(t, func() bool {
		backups, _ = filepath.Glob(filepath.Join(dir, "bento-*.log"))
		return len(backups) == 2
	}, time.Second*5, time.Millisecond*20)

	assert.NoFileExists(t, backup01)
	assert.NoFileExists(t, backup02)
	assert.FileExists(t, backup03)

	var newBackup string
	for _, b := range backups {
		if b != backup03 {
			newBackup = b
		}
	}
	require.NotEmpty(t, newBackup, "expected a newly rotated backup alongside the planted one that survived")

	content, err := os.ReadFile(newBackup)
	require.NoError(t, err)
	assert.Equal(t, "a line\n", string(content))
}

func TestRotatingWriterCompressesBackups(t *testing.T) {
	dir := t.TempDir()

	w := newRotatingWriter(File{
		Path:             filepath.Join(dir, "bento.log"),
		RotateCompress:   true,
		RotateMaxBackups: 1,
	})
	t.Cleanup(func() { _ = w.Close() })

	_, err := w.Write([]byte("a line\n"))
	require.NoError(t, err)

	require.NoError(t, w.Rotate())

	var gzBackups []string
	require.Eventually(t, func() bool {
		gzBackups, _ = filepath.Glob(filepath.Join(dir, "bento-*.log.gz"))
		plain, _ := filepath.Glob(filepath.Join(dir, "bento-*.log"))
		// lumberjack only removes the uncompressed source once the gzip
		// write has fully succeeded, so this state means compression, not
		// just the rename, is done.
		return len(gzBackups) == 1 && len(plain) == 0
	}, time.Second*5, time.Millisecond*20)

	f, err := os.Open(gzBackups[0])
	require.NoError(t, err)
	defer f.Close()

	gzr, err := gzip.NewReader(f)
	require.NoError(t, err)
	defer gzr.Close()

	content, err := io.ReadAll(gzr)
	require.NoError(t, err)
	assert.Equal(t, "a line\n", string(content))
}

func TestRotatingWriterRotatesAtMaxSize(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "bento.log")

	w := newRotatingWriter(File{
		Path:             logPath,
		RotateMaxSize:    1,
		RotateCompress:   false,
		RotateMaxBackups: 0,
	})
	t.Cleanup(func() { _ = w.Close() })

	const chunkSize = 64 * 1024
	chunk := bytes.Repeat([]byte("x"), chunkSize)

	for range 16 {
		_, err := w.Write(chunk)
		require.NoError(t, err)
	}

	// lumberjack only rotates once size+len *exceeds* MaxSize, so exactly
	// filling the file shouldn't have rotated it yet.
	backups, err := filepath.Glob(filepath.Join(dir, "bento-*.log"))
	require.NoError(t, err)
	assert.Empty(t, backups)

	_, err = w.Write(chunk)
	require.NoError(t, err)

	// The rename happens synchronously inside Write, and with MaxBackups 0,
	// MaxAge 0 and compression off the mill goroutine has nothing to do, so
	// there's no asynchronous work left to wait for here.
	backups, err = filepath.Glob(filepath.Join(dir, "bento-*.log"))
	require.NoError(t, err)
	require.Len(t, backups, 1)

	info, err := os.Stat(backups[0])
	require.NoError(t, err)
	assert.EqualValues(t, 16*chunkSize, info.Size())

	info, err = os.Stat(logPath)
	require.NoError(t, err)
	assert.EqualValues(t, chunkSize, info.Size())
}
