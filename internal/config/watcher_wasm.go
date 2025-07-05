//go:build wasm

package config

import (
	"errors"

	"github.com/warpstreamlabs/bento/internal/bundle"
)

// ErrNoReread is an error type returned from update triggers that indicates an
// attempt should not be re-made unless the source file has been modified.
type ErrNoReread struct {
	wrapped error
}

func noReread(err error) error {
	return &ErrNoReread{wrapped: err}
}

// Error returns a human readable error string.
func (e *ErrNoReread) Error() string {
	return e.wrapped.Error()
}

// BeginFileWatching does nothing in WASM builds as it is not supported. Sorry!
func (r *Reader) BeginFileWatching(mgr bundle.NewManagement, strict bool) error {
	return errors.New("file watching is disabled in WASM builds")
}
