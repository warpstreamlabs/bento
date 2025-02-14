package strict

import (
	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/processor"
)

// StrictBundle modifies a provided bundle environment so that all procesors
// will fail an entire batch if any any message-level error is encountered. These
// failed batches are nacked and/or reprocessed depending on your input.
func StrictBundle(b *bundle.Environment) *bundle.Environment {
	strictEnv := b.Clone()

	for _, spec := range b.ProcessorDocs() {
		_ = strictEnv.ProcessorAdd(func(conf processor.Config, nm bundle.NewManagement) (processor.V1, error) {
			proc, err := b.ProcessorInit(conf, nm)
			if err != nil {
				return nil, err
			}
			proc = wrapWithStrict(proc)
			return proc, err
		}, spec)
	}

	// TODO: Overwrite inputs for retry with backoff

	return strictEnv
}
