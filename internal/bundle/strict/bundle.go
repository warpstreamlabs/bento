package strict

import (
	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/input"
	iprocessors "github.com/warpstreamlabs/bento/internal/component/input/processors"
	"github.com/warpstreamlabs/bento/internal/component/output"
	oprocessors "github.com/warpstreamlabs/bento/internal/component/output/processors"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/manager"
	"github.com/warpstreamlabs/bento/internal/pipeline"
	"github.com/warpstreamlabs/bento/internal/pipeline/constructor"
)

type strictModeEnabledKey struct{}

// isStrictModeEnabled returns whether the environment has been flagged as being strict
// with the key-value pair `"strict_mode_enabled": true`.
func isStrictModeEnabled(mgr bundle.NewManagement) bool {
	ienabled, exists := mgr.GetGeneric(strictModeEnabledKey{})
	if !exists {
		return false
	}
	enabled, ok := ienabled.(bool)
	if !ok {
		return false
	}
	return enabled
}

//------------------------------------------------------------------------------

// OptSetRetryModeFromManager returns a set of options that re-configure a manager to automatically
// retry failed/errored messages. It applies retryable configurations to all plugins within the bundle.Environment,
// affecting both components and resources, in addition to functions and methods of the bloblang.Environment.
// This ensures the manager operates in a mode suitable for retrying errored messages.
func OptSetRetryModeFromManager() []manager.OptFunc {
	return []manager.OptFunc{
		func(t *manager.Type) {
			blobEnv := StrictBloblangEnvironment(t)
			manager.OptSetBloblangEnvironment(blobEnv)(t)
		},
		func(t *manager.Type) {
			env := RetryBundle(t.Environment())
			manager.OptSetEnvironment(env)(t)
		},
	}
}

// OptSetStrictModeFromManager returns a set of options that re-configure a manager to automatically
// reject failed/errored messages. It applies strict rejection configurations to all plugins within the bundle.Environment,
// affecting both components and resources, in addition to functions and methods of the bloblang.Environment.
// This ensures the manager operates in a mode suitable for rejecting all errored messages.
func OptSetStrictModeFromManager() []manager.OptFunc {
	return []manager.OptFunc{
		func(t *manager.Type) {
			blobEnv := StrictBloblangEnvironment(t)
			manager.OptSetBloblangEnvironment(blobEnv)(t)
		},
		func(t *manager.Type) {
			env := StrictBundle(t.Environment())
			manager.OptSetEnvironment(env)(t)
		},
	}
}

//------------------------------------------------------------------------------

// StrictBundle modifies a provided bundle environment so that all procesors
// will fail an entire batch if any any message-level error is encountered. These
// failed batches are nacked and/or reprocessed depending on your input.
func StrictBundle(b *bundle.Environment) *bundle.Environment {
	strictEnv := b.Clone()

	for _, spec := range b.ProcessorDocs() {
		_ = strictEnv.ProcessorAdd(func(conf processor.Config, nm bundle.NewManagement) (processor.V1, error) {
			if isProcessorIncompatible(conf.Type) {
				nm.Logger().Warn("Disabling strict mode due to incompatible processor(s) of type '%s'", conf.Type)
				nm.SetGeneric(strictModeEnabledKey{}, false)
			} else {
				// For compatible processors, set true if not already set
				nm.GetOrSetGeneric(strictModeEnabledKey{}, true)
			}

			proc, err := b.ProcessorInit(conf, nm)
			if err != nil {
				return nil, err
			}

			strictProc := wrapWithStrict(proc, setEnabledFromManager(nm))
			return strictProc, nil
		}, spec)
	}

	for _, spec := range b.OutputDocs() {
		_ = strictEnv.OutputAdd(func(conf output.Config, nm bundle.NewManagement, pcf ...processor.PipelineConstructorFunc) (output.Streamed, error) {
			if isOutputIncompatible(conf.Type) {
				nm.Logger().Warn("Disabling strict mode due to incompatible output(s) of type '%s'", conf.Type)
				nm.SetGeneric(strictModeEnabledKey{}, false)
			} else {
				nm.GetOrSetGeneric(strictModeEnabledKey{}, true)
			}

			pcf = oprocessors.AppendFromConfig(conf, nm, pcf...)
			conf.Processors = nil

			o, err := b.OutputInit(conf, nm)
			if err != nil {
				return nil, err
			}

			return output.WrapWithPipelines(o, pcf...)

		}, spec)
	}

	return strictEnv
}

//------------------------------------------------------------------------------

// NewRetryFeedbackPipelineCtor wraps a processing pipeline with a FeedbackProcessor, where failed transactions will be
// re-routed back into a Bento pipeline (and therefore re-processed).
func NewRetryFeedbackPipelineCtor() func(conf pipeline.Config, mgr bundle.NewManagement) (processor.Pipeline, error) {
	return func(conf pipeline.Config, mgr bundle.NewManagement) (processor.Pipeline, error) {
		pipe, err := constructor.New(conf, mgr)
		if err != nil {
			return nil, err
		}

		if !isStrictModeEnabled(mgr) {
			return pipe, nil
		}

		return newFeedbackProcessor(pipe, mgr), nil
	}
}

// RetryBundle wraps input.processors and output.processors pipeline constructors with FeedbackProcessors for re-routing failed transactions
// back into a pipeline for retrying.
func RetryBundle(b *bundle.Environment) *bundle.Environment {
	retryEnv := StrictBundle(b)

	for _, spec := range b.InputDocs() {
		_ = retryEnv.InputAdd(func(conf input.Config, nm bundle.NewManagement) (input.Streamed, error) {
			pcf := iprocessors.AppendFromConfig(conf, nm)
			conf.Processors = nil

			if isStrictModeEnabled(nm) {
				// Wrap constructed pipeline with feedback processor
				for i, ctor := range pcf {
					pcf[i] = func() (processor.Pipeline, error) {
						pipe, err := ctor()
						if err != nil {
							return nil, err
						}
						return newFeedbackProcessor(pipe, nm), nil
					}
				}
			}

			i, err := b.InputInit(conf, nm)
			if err != nil {
				return nil, err
			}

			wi, err := input.WrapWithPipelines(i, pcf...)
			if err != nil {
				return nil, err
			}

			return wi, nil

		}, spec)
	}

	for _, spec := range b.OutputDocs() {
		_ = retryEnv.OutputAdd(func(conf output.Config, nm bundle.NewManagement, pcf ...processor.PipelineConstructorFunc) (output.Streamed, error) {
			pcf = oprocessors.AppendFromConfig(conf, nm, pcf...)
			conf.Processors = nil

			if !isStrictModeEnabled(nm) {
				for i, ctor := range pcf {
					pcf[i] = func() (processor.Pipeline, error) {
						pipe, err := ctor()
						if err != nil {
							return nil, err
						}
						return newFeedbackProcessor(pipe, nm), nil
					}
				}
			}

			i, err := b.OutputInit(conf, nm)
			if err != nil {
				return nil, err
			}

			wi, err := output.WrapWithPipelines(i, pcf...)
			if err != nil {
				return nil, err
			}

			return wi, nil
		}, spec)
	}

	return retryEnv
}
