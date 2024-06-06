package service

import (
	"io"
	"io/fs"
	"os"

	"github.com/warpstreamlabs/bento/internal/filepath"
	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
)

// ReadFile opens a file from an fs.FS and reads all bytes. When the OpenFile
// method is available this will be used instead of Open with the RDONLY flag.
func ReadFile(f fs.FS, name string) ([]byte, error) {
	var i fs.File
	var err error
	if ef, ok := f.(ifs.FS); ok {
		i, err = ef.OpenFile(name, os.O_RDONLY, 0)
	} else {
		i, err = f.Open(name)
	}
	if err != nil {
		return nil, err
	}
	return io.ReadAll(i)
}

// Globs attempts to expand the glob patterns within of a series of paths and
// returns the resulting expanded slice or an error.
func Globs(f fs.FS, paths ...string) ([]string, error) {
	return filepath.Globs(f, paths)
}

// GlobsAndSuperPaths attempts to expand a list of paths, which may include glob
// patterns and super paths (the ... thing) to a list of explicit file paths.
// Extensions must be provided, and limit the file types that are captured with
// a super path.
func GlobsAndSuperPaths(f fs.FS, paths []string, extensions ...string) ([]string, error) {
	return filepath.GlobsAndSuperPaths(f, paths)
}

// OSFS provides an fs.FS implementation that simply calls into the os.
func OSFS() fs.FS {
	return ifs.OS()
}
