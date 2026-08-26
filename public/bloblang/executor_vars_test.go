package bloblang

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mappings without variable assignments share a single empty variables map, so anything that
// could write to it has to be proven absent rather than assumed.

func TestSharedEmptyVarsNotWrittenByAppliedMap(t *testing.T) {
	// The outer mapping assigns no variables, so it receives the shared map. The applied map
	// does assign one, and must not be able to reach the caller's variables: the apply method
	// replaces them first.
	exe, err := GlobalEnvironment().Parse(`map inner {
  let v = "written"
  root = $v
}
root.out = this.a.apply("inner")`)
	require.NoError(t, err)

	res, err := exe.Query(map[string]any{"a": "ignored"})
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"out": "written"}, res)

	assert.Empty(t, sharedEmptyVars, "applied map leaked a variable into the shared map")
}

func TestUndefinedVariableErrorUnchanged(t *testing.T) {
	// A mapping that reads a variable it never assigns still takes the shared map. An empty map
	// reports the variable as undefined; a nil map would report that variables themselves were
	// undefined, which would be a different error.
	exe, err := GlobalEnvironment().Parse(`root = $nope`)
	require.NoError(t, err)

	_, err = exe.Query(map[string]any{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "variable 'nope' undefined")
	assert.Empty(t, sharedEmptyVars)
}

func TestVarAssigningMappingGetsPrivateVars(t *testing.T) {
	exe, err := GlobalEnvironment().Parse(`let seen = this.v
root.v = $seen`)
	require.NoError(t, err)

	for _, v := range []string{"one", "two", "three"} {
		res, err := exe.Query(map[string]any{"v": v})
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"v": v}, res, "variables leaked between executions")
	}
	assert.Empty(t, sharedEmptyVars)
}

func TestVarsFreeMappingIsConcurrencySafe(t *testing.T) {
	// Run under -race: a write to the shared map from any path would be reported here.
	exe, err := GlobalEnvironment().Parse(`root.a = this.x
root.b = this.y`)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			for range 100 {
				res, err := exe.Query(map[string]any{"x": 1, "y": 2})
				assert.NoError(t, err)
				assert.Equal(t, map[string]any{"a": 1, "b": 2}, res)
			}
		})
	}
	wg.Wait()
	assert.Empty(t, sharedEmptyVars)
}

func TestOverlayVarsBehaviourUnchanged(t *testing.T) {
	exe, err := GlobalEnvironment().Parse(`let n = this.n
root.doubled = $n * 2`)
	require.NoError(t, err)

	onto := any(map[string]any{"kept": true})
	require.NoError(t, exe.Overlay(map[string]any{"n": 21}, &onto))
	assert.Equal(t, map[string]any{"kept": true, "doubled": int64(42)}, onto)
	assert.Empty(t, sharedEmptyVars)
}

// AssignsVariables drives the choice, so its verdict is asserted directly across the statement
// forms that can carry an assignment.
func TestAssignsVariablesDetection(t *testing.T) {
	tests := []struct {
		name    string
		mapping string
		want    bool
	}{
		{"no vars", `root = this.a`, false},
		{"reads a var only", `root = $nope`, false},
		{"top level let", `let a = 1
root = $a`, true},
		{"let inside root if", `root = if this.a == 1 {
  this.b
} else {
  this.c
}`, false},
		{"var assigned in an if statement", `if this.a == 1 {
  let x = 1
  root.v = $x
}`, true},
		{"only the applied map assigns", `map inner {
  let v = 1
  root = $v
}
root = this.apply("inner")`, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exe, err := GlobalEnvironment().Parse(test.mapping)
			require.NoError(t, err)
			assert.Equal(t, test.want, exe.assignsVars)
		})
	}
}
