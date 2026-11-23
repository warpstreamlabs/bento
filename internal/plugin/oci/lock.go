package oci

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// lockFileName is the file in the store used to stop processes sharing the
// store from overwriting each other's changes. It isn't part of the OCI
// image layout, which tools ignore.
const lockFileName = "bento.lock"

// lockStore takes a lock on the store in dir, shared for reading and
// exclusive for writing, and returns a function that releases it. A store
// that can't be written to, such as a read-only volume, can still be read
// without a lock, as nothing can change it.
func lockStore(dir string, exclusive bool) (func(), error) {
	path := filepath.Join(dir, lockFileName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil && !exclusive {
		if f, err = os.Open(path); err != nil {
			if errors.Is(err, fs.ErrNotExist) || errors.Is(err, fs.ErrPermission) {
				return func() {}, nil
			}
			return nil, fmt.Errorf("failed to open plugin store lock: %w", err)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to open plugin store lock: %w", err)
	}
	if err := lockFile(f, exclusive); err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to lock plugin store: %w", err)
	}
	return func() {
		_ = unlockFile(f)
		_ = f.Close()
	}, nil
}
