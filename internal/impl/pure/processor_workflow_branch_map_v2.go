package pure

import (
	"context"
	"fmt"
	"regexp"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/public/service"
)

type workflowBranchMapV2 struct {
	Branches     map[string]*Branch
	dependencies map[string][]string
}

// Locks all branches contained in the branch map and returns the latest DAG, a
// map of resources, and a func to unlock the resources that were locked. If
// any error occurs in locked each branch (the resource is missing, or the DAG
// is malformed) then an error is returned instead.
func (w *workflowBranchMapV2) LockV2() (branches map[string]*Branch, dependencies map[string][]string, unlockFn func(), err error) {
	return w.Branches, w.dependencies, func() {}, nil
}

func (w *workflowBranchMapV2) Close(ctx context.Context) error {
	for _, c := range w.Branches {
		if err := c.Close(ctx); err != nil {
			return err
		}
	}
	return nil
}

var processDAGStageNameV2 = regexp.MustCompile("[a-zA-Z0-9-_]+")

func newWorkflowBranchMapV2(conf *service.ParsedConfig, mgr bundle.NewManagement) (*workflowBranchMapV2, error) {
	branchObjMap, err := conf.FieldObjectMap(wflowProcFieldBranchesV2)
	if err != nil {
		return nil, err
	}

	branches := map[string]*Branch{}
	for k, v := range branchObjMap {
		if len(processDAGStageNameV2.FindString(k)) != len(k) {
			return nil, fmt.Errorf("workflow_v2 branch name '%v' contains invalid characters", k)
		}

		child, err := newBranchFromParsed(v, mgr.IntoPath("workflow_v2", "branches", k))
		if err != nil {
			return nil, err
		}
		branches[k] = child
	}

	dependencies := make(map[string][]string)

	for k, v := range branchObjMap {
		depList, _ := v.FieldStringList("dependency_list")
		dependencies[k] = append(dependencies[k], depList...)
		if len(depList) == 0 {
			dependencies[k] = nil
		}
	}

	for k, vs := range dependencies {
		seen := make(map[string]bool)
		for _, v := range vs {
			if _, exists := dependencies[v]; !exists {
				return nil, fmt.Errorf("dependency %q in the dependency_list of branch %q is not a branch", v, k)
			}
			if seen[v] {
				return nil, fmt.Errorf("dependency %q duplicated in dependency_list of branch %q", v, k)
			}
			seen[v] = true
		}
	}

	// check dependencies is a DAG
	isDag := validateDAG(dependencies)
	if !isDag {
		return nil, fmt.Errorf("dependency_lists have a cyclical dependency")
	}

	return &workflowBranchMapV2{
		Branches:     branches,
		dependencies: dependencies,
	}, nil
}

func validateDAG(graph map[string][]string) bool {
	// Status maps to track the state of each node:
	// 0 = unvisited, 1 = visiting, 2 = visited
	status := make(map[string]int)

	var dfs func(node string) bool

	dfs = func(node string) bool {
		if status[node] == 1 {
			return false
		}
		if status[node] == 2 {
			return true
		}

		status[node] = 1

		for _, neighbor := range graph[node] {
			if !dfs(neighbor) {
				return false
			}
		}

		status[node] = 2
		return true
	}

	for node := range graph {
		if status[node] == 0 {
			if !dfs(node) {
				return false
			}
		}
	}

	return true
}
