package strict

import (
	"sync/atomic"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/output"
	"github.com/warpstreamlabs/bento/internal/component/output/processors"
	"github.com/warpstreamlabs/bento/internal/component/processor"
)

// StrictBundle modifies a provided bundle environment so that all procesors
// will fail an entire batch if any any message-level error is encountered. These
// failed batches are nacked and/or reprocessed depending on your input.
func StrictBundle(b *bundle.Environment) *bundle.Environment {
	strictEnv := b.Clone()

	// Allows us to globally toggle strict mode for all processors in a thread-safe way.
	// TODO: Create a custom environment/NewManagement that can manage state better.
	var isStrictEnabled = &atomic.Bool{}
	isStrictEnabled.Store(true)

	for _, spec := range b.ProcessorDocs() {
		_ = strictEnv.ProcessorAdd(func(conf processor.Config, nm bundle.NewManagement) (processor.V1, error) {
			if isProcessorIncompatible(conf.Type) {
				nm.Logger().Warn("Disabling strict mode due to incompatible processor(s) of type '%s'", conf.Type)
				if isStrictEnabled.Load() {
					isStrictEnabled.Store(false)
				}
			}

			proc, err := b.ProcessorInit(conf, nm)
			if err != nil {
				return nil, err
			}

			// Pass global flag to all processors
			strictProc := wrapWithStrict(proc, setAtomicStrictFlag(isStrictEnabled))
			return strictProc, nil
		}, spec)
	}

	for _, spec := range b.OutputDocs() {
		_ = strictEnv.OutputAdd(func(conf output.Config, nm bundle.NewManagement, pcf ...processor.PipelineConstructorFunc) (output.Streamed, error) {
			if isOutputIncompatible(conf.Type) {
				nm.Logger().Warn("Disabling strict mode due to incompatible output(s) of type '%s'", conf.Type)
				if isStrictEnabled.Load() {
					isStrictEnabled.Store(false)
				}
			}

			pcf = processors.AppendFromConfig(conf, nm, pcf...)
			conf.Processors = nil

			o, err := b.OutputInit(conf, nm)
			if err != nil {
				return nil, err
			}

			return output.WrapWithPipelines(o, pcf...)

		}, spec)
	}

	// TODO: Overwrite inputs for retry with backoff

	return strictEnv
}
