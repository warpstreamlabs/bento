package strict

import (
	"context"
	"fmt"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/input"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/errorhandling"
	"github.com/warpstreamlabs/bento/internal/manager"
)

func NewErrorHandlingBundleFromConfig(ctx context.Context, cfg errorhandling.Config, b *bundle.Environment) *bundle.Environment {
	env := b.Clone()

	if cfg.ErrorSampleRate > 0 {
		env = ErrorSampleBundle(env, cfg.ErrorSampleRate)
	}

	switch cfg.Strategy {
	case "reject":
		env = StrictBundle(env)
	case "backoff":
		env = RetryBundle(ctx, env, cfg.MaxRetries)
	}

	return env

}

// WithStrictBundle modifies a provided bundle environment so that all procesors
// will fail an entire batch if any any message-level error is encountered. These
// failed batches are nacked and/or reprocessed depending on your input.
func StrictBundle(b *bundle.Environment) *bundle.Environment {
	env := b.Clone()
	for _, spec := range b.ProcessorDocs() {
		_ = env.ProcessorAdd(func(conf processor.Config, nm bundle.NewManagement) (processor.V1, error) {
			proc, err := b.ProcessorInit(conf, nm)
			if err != nil {
				return nil, err
			}

			proc = wrapWithStrict(proc)
			return proc, err
		}, spec)
	}

	return env
}
func ErrorSampleBundle(b *bundle.Environment, rate float64) *bundle.Environment {
	errorSampleEnv := b.Clone()

	for _, spec := range b.ProcessorDocs() {
		_ = errorSampleEnv.ProcessorAdd(func(conf processor.Config, nm bundle.NewManagement) (processor.V1, error) {
			proc, err := b.ProcessorInit(conf, nm)
			if err != nil {
				return nil, err
			}
			proc = wrapWithErrorSampling(proc, nm, rate)
			return proc, err
		}, spec)
	}

	return errorSampleEnv
}

// RetryBundle returns a modified environment to reprocess any batches containing any message-level errors
// through the entire pipeline. During retries, the input-layer will pause new message ingestion.
func RetryBundle(ctx context.Context, b *bundle.Environment, maxRetries int) *bundle.Environment {
	env := b.Clone()

	for _, spec := range b.InputDocs() {
		_ = env.InputAdd(func(conf input.Config, nm bundle.NewManagement) (input.Streamed, error) {
			i, err := b.InputInit(conf, nm)
			if err != nil {
				return nil, err
			}

			// Check if the type is an AsyncReader
			if rdr, ok := i.(*input.AsyncReader); ok {
				async := rdr.UnwrapAsyncReader()
				// If not already an AsyncPreserver, wrap with one and create
				// new AsyncReader.
				if _, isRetryInput := async.(*input.AsyncPreserver); !isRetryInput {

					wrapped := manager.WrapInput(i)
					if err := wrapped.CloseExistingInput(ctx, true); err != nil {
						return nil, fmt.Errorf("failed to close existing input %s: %w", spec.Name, err)
					}
					// NewAsyncReader will automatically start an input-ingestion goroutine.
					// Hence, we need to close the previous existing input prior to starting a new wrapped one.
					retryInput, err := input.NewAsyncReader(conf.Type, input.NewAsyncPreserver(async), nm)
					if err != nil {
						return nil, fmt.Errorf("failed to wrap input %s with retry: %w", spec.Name, err)
					}

					wrapped.SwapInput(retryInput)
					return wrapped, nil
				}
			}
			return i, err
		}, spec)
	}

	for _, spec := range b.ProcessorDocs() {
		_ = env.ProcessorAdd(func(conf processor.Config, nm bundle.NewManagement) (processor.V1, error) {
			proc, err := b.ProcessorInit(conf, nm)
			if err != nil {
				return nil, err
			}
			proc = wrapWithRetry(proc, nm, maxRetries)
			return proc, err
		}, spec)
	}

	return env
}
