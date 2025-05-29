// Package service provides a high level API for registering custom plugin
// components and executing either a standard Bento CLI, or programmatically
// building isolated pipelines with a StreamBuilder API.
//
// An example Bento distribution containing component plugins and tests can be found at:
// https://github.com/warpstreamlabs/bento/tree/main/resources/plugin_example
//
// In order to add custom Bloblang functions and methods use the
// ./public/bloblang package.
package service

import (
	"context"
)

// Closer is implemented by components that support stopping and cleaning up
// their underlying resources.
type Closer interface {
	// Close the component, blocks until either the underlying resources are
	// cleaned up or the context is cancelled. Returns an error if the context
	// is cancelled.
	Close(ctx context.Context) error
}
