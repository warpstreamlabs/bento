package log

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRotatingWriterDefaults(t *testing.T) {
	f := File{
		Path:         filepath.Join(t.TempDir(), "bento.log"),
		Rotate:       true,
		RotateMaxAge: 7,
	}

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

	w := newRotatingWriter(File{
		Path:         filepath.Join(dir, "bento.log"),
		Rotate:       true,
		RotateMaxAge: 7,
	})
	t.Cleanup(func() { _ = w.Close() })

	_, err := w.Write([]byte("a line\n"))
	require.NoError(t, err)

	require.NoError(t, w.Rotate())

	require.Eventually(t, func() bool {
		_, statErr := os.Stat(oldBackup)
		plainBackups, _ := filepath.Glob(filepath.Join(dir, "bento-*.log"))
		gzBackups, _ := filepath.Glob(filepath.Join(dir, "bento-*.log.gz"))
		// Compression is always on at this point (it isn't configurable
		// yet), so wait for the mill to finish gzipping the fresh backup
		// too, otherwise t.TempDir()'s cleanup can race its open file.
		return os.IsNotExist(statErr) && len(plainBackups) == 0 && len(gzBackups) == 1
	}, time.Second*5, time.Millisecond*20)
}
