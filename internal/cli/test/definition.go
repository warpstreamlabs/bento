package test

import (
	"fmt"
	"path/filepath"

	"github.com/warpstreamlabs/bento/internal/config/test"
	"github.com/warpstreamlabs/bento/internal/docs"
	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
	"github.com/warpstreamlabs/bento/internal/log"
)

// Execute the test definition.
func Execute(confSpec docs.FieldSpecs, cases []test.Case, testFilePath string, resourcesPaths []string, logger log.Modular) ([]CaseFailure, error) {
	procsProvider := NewProcessorsProvider(
		testFilePath,
		OptAddResourcesPaths(resourcesPaths),
		OptProcessorsProviderSetLogger(logger),
		OptSetConfigSpec(confSpec),
	)

	dir := filepath.Dir(testFilePath)

	var totalFailures []CaseFailure
	for i, c := range cases {
		cleanupEnv := setEnvironment(c.Environment)
		failures, err := ExecuteFrom(ifs.OS(), dir, c, procsProvider)
		if err != nil {
			cleanupEnv()
			return nil, fmt.Errorf("test case %v failed: %v", i, err)
		}
		totalFailures = append(totalFailures, failures...)
		cleanupEnv()
	}

	return totalFailures, nil
}
