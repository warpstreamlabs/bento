//go:build wasm

package config

import (
	"errors"

	"github.com/warpstreamlabs/bento/internal/bundle"
)

// BeginFileWatching does nothing in WASM builds as it is not supported. Sorry!
func (r *Reader) BeginFileWatching(mgr bundle.NewManagement, strict bool) error {
	return errors.New("file watching is disabled in WASM builds")
}
