package errorhandling

import "github.com/warpstreamlabs/bento/internal/docs"

func Spec() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldString(fieldStrategy, "The error handling strategy.").HasOptions("reject", "backoff").HasDefault(""),
		docs.FieldFloat(fieldErrorSampleRate, "Proportion of failed message payloads to randomly sample from and log.").HasDefault(0).
			LinterBlobl(`root = if this < 0 || this > 1 { "error_sample_rate should be between 0 and 1." }`),
		docs.FieldInt(fieldMaxRetries, "Maximum retries before dropping a message for a `backoff` strategy. `0` indicates indefinite retries.").HasDefault(0).
			LinterBlobl(`root = if this < 0 { "max_retries should be greater than or equal to 0. Set to 0 to indefinitely retry." }`),
	}
}
