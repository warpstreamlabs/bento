package runtime

import (
	"context"
	"io/fs"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/docs"
)

// Plugin is the interface for a single plugin.
type Plugin interface {
	Name() string
	RegisterWith(env *bundle.Environment) error
	Spec() docs.ComponentSpec
}

// Runtime is the interface for a plugin runtime. A runtime owns the plugins it
// registers, which remain valid until Close is called.
type Runtime interface {
	Register(manifest *Manifest, source Source) (Plugin, error)
	Close(ctx context.Context) error
}

// Pool is an interface for a pool of plugin instances.
type Pool[T any] interface {
	Get(ctx context.Context) (T, error)
	Put(instance T)
	Close(ctx context.Context) error
}

//------------------------------------------------------------------------------

type Source interface{ isPluginSource() }

type DirSource string

func (DirSource) isPluginSource() {}

type FSSource struct{ FS fs.FS }

func (FSSource) isPluginSource() {}

type ByteSource []byte

func (ByteSource) isPluginSource() {}
