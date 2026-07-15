package ifs

import (
	"errors"
	"io/fs"
	"path/filepath"
	"testing/fstest"
)

type TestFS struct {
	workDir string
	MapFS   fstest.MapFS
}

func (t TestFS) Chdir(dir string) FS {
	return TestFS{workDir: t.joinWithWorkdir(dir), MapFS: t.MapFS}
}

func (t TestFS) joinWithWorkdir(path string) string {
	if !filepath.IsAbs(path) {
		path = filepath.Join(t.workDir, path)
	}
	return path
}

func (t TestFS) Open(name string) (fs.File, error) {
	return t.MapFS.Open(t.joinWithWorkdir(name))
}

func (t TestFS) OpenFile(name string, flag int, perm fs.FileMode) (fs.File, error) {
	return nil, errors.New("not implemented")
}

func (t TestFS) Stat(name string) (fs.FileInfo, error) {
	return t.MapFS.Stat(t.joinWithWorkdir(name))
}

func (t TestFS) MkdirAll(path string, perm fs.FileMode) error {
	return errors.New("not implemented")
}

func (t TestFS) Remove(name string) error {
	return errors.New("not implemented")
}
