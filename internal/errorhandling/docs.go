package errorhandling

import "github.com/warpstreamlabs/bento/internal/docs"

func Spec() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldString(fieldStrategy, "The error handling strategy.").HasOptions("none", "reject").HasDefault("none"),
	}
}
