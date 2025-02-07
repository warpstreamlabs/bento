package errorhandling

import "github.com/warpstreamlabs/bento/internal/docs"

func Spec() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldString(fieldStrategy, "The error handling strategy.").HasOptions("none", "reject").HasDefault("none"),
		docs.FieldObject(fieldLog, "Configuration for global logging message-level errors.").WithChildren(
			docs.FieldBool(fieldLogEnabled, "Whether to enable message-level error logging.").HasDefault(false),
			docs.FieldBool(fieldLogAddPayload, "Whether to add a failed message payload to an error log.").HasDefault(false),
			docs.FieldFloat(fieldLogSamplingRatio, "Sets the ratio of errored messages within a batch to sample.").HasDefault(1).
				LinterBlobl(`root = if this < 0 || this > 1 { "batch_proportion should be between 0 and 1." }`),
		),
	}
}
