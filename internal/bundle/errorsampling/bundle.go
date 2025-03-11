package errorsampling

import (
	"github.com/warpstreamlabs/bento/internal/bloblang/query"
	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/errorhandling"
)

// ErrorSamplingBundle modifies a provided bundle environment to automatically log messages-level errors at the ERROR level.
// This can be used to selectively sample from a batch of failed messages without logging all errors.
func ErrorSamplingBundle(eConf errorhandling.Config, b *bundle.Environment) *bundle.Environment {
	errLogEnv := b.Clone()

	for _, spec := range b.ProcessorDocs() {
		_ = errLogEnv.ProcessorAdd(func(conf processor.Config, nm bundle.NewManagement) (processor.V1, error) {
			proc, err := b.ProcessorInit(conf, nm)
			if err != nil {
				return nil, err
			}

			key := nm.Label()
			if key == "" {
				key = "root." + query.SliceToDotPath(nm.Path()...)
			}
			logger := nm.Logger().With("path", key)

			if conf.Type != "" {
				logger = logger.With("plugin", conf.Type)
			}

			proc = wrapWithErrorLogger(proc, logger, eConf)
			return proc, err
		}, spec)
	}

	return errLogEnv
}
