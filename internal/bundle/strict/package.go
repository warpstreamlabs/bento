package strict

import "slices"

var incompatibleProcessors []string

func init() {
	incompatibleProcessors = []string{
		"try",
		"catch",
		"switch",
	}
}

func isProcessorIncompatible(name string) bool {
	return slices.Contains(incompatibleProcessors, name)

}
