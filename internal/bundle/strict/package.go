package strict

import "slices"

var incompatibleProcessors = []string{
	"try",
	"catch",
	"retry",
}

var incompatibleOutputs = []string{
	"reject_errored",
}

var incompatibleBloblangFunctions = []string{
	"error",
	"errored",
}

var incompatibleBloblangMethods = []string{
	"catch",
}

func isProcessorIncompatible(name string) bool {
	return slices.Contains(incompatibleProcessors, name)
}

func isOutputIncompatible(name string) bool {
	return slices.Contains(incompatibleOutputs, name)
}

func isBloblangFunctionIncompatible(name string) bool {
	return slices.Contains(incompatibleBloblangFunctions, name)
}

func isBloblangMethodIncompatible(name string) bool {
	return slices.Contains(incompatibleBloblangMethods, name)
}
