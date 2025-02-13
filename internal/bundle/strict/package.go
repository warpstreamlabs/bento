package strict

import "slices"

var incompatibleProcessors = []string{
	"try",
	"catch",
	"switch",
	"retry",
}

var incompatibleOutputs = []string{
	"reject_errored",
	"switch",
}

func isProcessorIncompatible(name string) bool {
	return slices.Contains(incompatibleProcessors, name)
}

func isOutputIncompatible(name string) bool {
	return slices.Contains(incompatibleOutputs, name)
}
