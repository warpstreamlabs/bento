package test

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/fatih/color"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/config"
	"github.com/warpstreamlabs/bento/internal/config/test"
	"github.com/warpstreamlabs/bento/internal/docs"
	ifilepath "github.com/warpstreamlabs/bento/internal/filepath"
	"github.com/warpstreamlabs/bento/internal/filepath/ifs"
	"github.com/warpstreamlabs/bento/internal/log"
)

var (
	green  = color.New(color.FgGreen).SprintFunc()
	red    = color.New(color.FgRed).SprintFunc()
	yellow = color.New(color.FgYellow).SprintFunc()
)

//------------------------------------------------------------------------------

// GetPathPair returns the config path and expected accompanying test definition
// path for a given syntax and a path for either file.
func GetPathPair(fullPath, testSuffix string) (configPath, definitionPath string) {
	path, file := filepath.Split(fullPath)
	ext := filepath.Ext(file)
	filename := strings.TrimSuffix(file, ext)
	if strings.HasSuffix(filename, testSuffix) {
		definitionPath = filepath.Clean(fullPath)
		configPath = filepath.Join(path, strings.TrimSuffix(filename, testSuffix)+ext)
	} else {
		configPath = filepath.Clean(fullPath)
		definitionPath = filepath.Join(path, filename+testSuffix+ext)
	}
	return
}

func getDefinition(targetPath, definitionPath string) ([]test.Case, error) {
	if _, err := ifs.OS().Stat(targetPath); err != nil {
		return nil, fmt.Errorf("unable to access target config file '%v': %v", targetPath, err)
	}
	if _, err := ifs.OS().Stat(definitionPath); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("unable to access test definition file '%v': %v", definitionPath, err)
		}
		if !strings.HasSuffix(targetPath, ".yaml") && !strings.HasSuffix(targetPath, ".yml") {
			return nil, nil
		}
		definitionPath = targetPath
	}
	defBytes, err := ifs.ReadFile(ifs.OS(), definitionPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read test definition from '%v': %v", definitionPath, err)
	}

	node, err := docs.UnmarshalYAML(defBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse test definition from '%v': %v", definitionPath, err)
	}

	cases, err := test.FromAny(node)
	if err != nil {
		return nil, fmt.Errorf("failed to parse test definition from '%v': %v", definitionPath, err)
	}
	return cases, nil
}

// GetTestTargets searches for test definition targets in a path with a given
// test suffix.
func GetTestTargets(targetPaths []string, testSuffix string) (map[string][]test.Case, error) {
	targetPaths, err := ifilepath.GlobsAndSuperPaths(ifs.OS(), targetPaths, "yaml", "yml")
	if err != nil {
		return nil, err
	}

	targetDefinitions := map[string][]test.Case{}
	for _, tPath := range targetPaths {
		configPath, definitionPath := GetPathPair(tPath, testSuffix)
		def, err := getDefinition(configPath, definitionPath)
		if err != nil {
			return nil, err
		}
		if len(def) == 0 {
			continue
		}
		targetDefinitions[filepath.Clean(configPath)] = def
	}
	return targetDefinitions, nil
}

// Lints the config target of a test definition and either returns linting
// errors (false for failed) or returns an error.
func lintTarget(spec docs.FieldSpecs, path, testSuffix string) ([]docs.Lint, error) {
	confPath, _ := GetPathPair(path, testSuffix)

	// This is necessary as each test case can provide a different set of
	// environment variables, so in order to test env vars properly we would
	// need to lint for each case.
	skipEnvVarCheck := true
	_, lints, err := config.ReadYAMLFileLinted(ifs.OS(), spec, confPath, skipEnvVarCheck, docs.NewLintConfig(bundle.GlobalEnvironment))
	if err != nil {
		return nil, err
	}
	return lints, nil
}

//------------------------------------------------------------------------------

// RunAll executes the test command for a slice of paths. The path can either be
// a config file, a config files test definition file, a directory, or the
// wildcard pattern './...'.
func RunAll(paths []string, spec docs.FieldSpecs, testSuffix string, lint bool, logger log.Modular, resourcesPaths []string) bool {
	targets, err := GetTestTargets(paths, testSuffix)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to obtain test targets: %v\n", err)
		return false
	}
	if len(targets) == 0 {
		fmt.Printf("%v\n", yellow("No tests were found"))
		return false
	}

	type failedTarget struct {
		target string
		lints  []docs.Lint
		cases  []CaseFailure
	}
	fails := []failedTarget{}

	targetPaths := make([]string, 0, len(targets))
	for k := range targets {
		targetPaths = append(targetPaths, k)
	}
	sort.Strings(targetPaths)

	for _, target := range targetPaths {
		var lints []docs.Lint
		var failCases []CaseFailure
		if lint {
			if lints, err = lintTarget(spec, target, testSuffix); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to execute test target '%v': %v\n", target, err)
				return false
			}
		}
		if failCases, err = Execute(spec, targets[target], target, resourcesPaths, logger); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to execute test target '%v': %v\n", target, err)
			return false
		}
		if len(lints) > 0 || len(failCases) > 0 {
			fails = append(fails, failedTarget{
				target: target,
				lints:  lints,
				cases:  failCases,
			})
			fmt.Printf("Test '%v' %v\n", target, red("failed"))
		} else {
			fmt.Printf("Test '%v' %v\n", target, green("succeeded"))
		}
	}
	if len(fails) > 0 {
		fmt.Printf("\nFailures:\n\n")
		for i, fail := range fails {
			if i > 0 {
				fmt.Println("")
			}
			fmt.Printf("--- %v ---\n\n", fail.target)
			for _, lint := range fail.lints {
				fmt.Printf("Lint: %v\n", lint)
			}
			if len(fail.cases) > 0 {
				if len(fail.lints) > 0 {
					fmt.Println("")
				}
				var namePrev string
				for i, fail := range fail.cases {
					if namePrev != fail.Name {
						if i > 0 {
							fmt.Println("")
						}
						fmt.Printf("%v [line %v]:\n", fail.Name, fail.TestLine)
						namePrev = fail.Name
					}
					fmt.Println(fail.Reason)
				}
			}
		}
		return false
	}
	return true
}
