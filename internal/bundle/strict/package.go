package strict

import "slices"

var incompatibleProcessors = []string{
	"try",
	"catch",
	"switch",
	"retry",
}

func isProcessorIncompatible(name string) bool {
	return slices.Contains(incompatibleProcessors, name)
}
