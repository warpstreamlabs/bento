package parser

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseMappingImportShortCircuit verifies that ParseMapping's fast-path
// pre-check (skip attempting singleRootImport unless the input could
// plausibly be a `from "..."` single-root-import) does not change parsing
// behaviour for any of: a bare import, an import preceded by whitespace/
// comments, and inputs that must NOT be treated as an import (regular
// mappings, mappings that merely contain the substring "from" elsewhere,
// empty input, whitespace-only input).
func TestParseMappingImportShortCircuit(t *testing.T) {
	dir := t.TempDir()
	importedPath := filepath.Join(dir, "imported.blobl")
	require.NoError(t, os.WriteFile(importedPath, []byte(`root.imported = true`), 0o644))

	tests := map[string]struct {
		mapping   string
		expectErr bool
	}{
		"bare import": {
			mapping: `from "` + importedPath + `"`,
		},
		"import with leading whitespace": {
			mapping: "   \n  from \"" + importedPath + "\"",
		},
		"import with leading comment": {
			mapping: "# a comment\nfrom \"" + importedPath + "\"",
		},
		"import with leading blank lines and comments mixed": {
			mapping: "\n# one\n\n# two\n   from \"" + importedPath + "\"",
		},
		"regular mapping starting with root": {
			mapping: `root.foo = this.bar`,
		},
		"regular mapping mentioning from in a string": {
			mapping: `root.foo = "from the start"`,
		},
		"regular mapping mentioning from as an identifier-like substring": {
			mapping: `let fromage = this.bar
root.foo = $fromage`,
		},
		"single expression shorthand": {
			mapping: `this.foo.uppercase()`,
		},
		"let statement": {
			mapping: `let x = 5
root.foo = $x`,
		},
		"malformed import missing quotes": {
			mapping:   `from bad`,
			expectErr: true,
		},
		"empty input": {
			mapping:   ``,
			expectErr: true,
		},
		"whitespace only input": {
			mapping:   "   \n  \n",
			expectErr: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			exec, err := ParseMapping(GlobalContext(), test.mapping)
			if test.expectErr {
				assert.NotNil(t, err, "expected an error for mapping: %q", test.mapping)
				return
			}
			require.Nil(t, err, "unexpected error for mapping: %q", test.mapping)
			require.NotNil(t, exec)
		})
	}
}

// TestParseMappingImportShortCircuitMatchesUnconditionalAttempt is a
// differential test: it parses a table of mappings both via the public
// ParseMapping (which applies the fast-path pre-check) and via a direct,
// unconditional call chain equivalent to the pre-optimisation behaviour,
// and asserts both produce the same success/failure outcome. This guards
// against the pre-check silently diverging from ParseMapping's original
// unconditional-attempt semantics for any input shape.
func TestParseMappingImportShortCircuitMatchesUnconditionalAttempt(t *testing.T) {
	inputs := []string{
		`from "nonexistent.blobl"`,
		"  \n from \"nonexistent.blobl\"",
		"# comment\nfrom \"nonexistent.blobl\"",
		`root.foo = this.bar`,
		`this.foo.uppercase()`,
		``,
		"   ",
		`meta foo = this.bar`,
		`map foo { root = this }`,
		`from`,
		`fromfoo = this.bar`,
	}

	pCtx := GlobalContext()
	for _, in := range inputs {
		rn := []rune(in)

		// Unconditional attempt, mirroring ParseMapping's pre-optimisation
		// control flow exactly.
		var wantErr bool
		resDirectImport := singleRootImport(pCtx)(rn)
		if resDirectImport.Err != nil && resDirectImport.Err.IsFatal() {
			wantErr = true
		} else if resDirectImport.Err == nil && len(resDirectImport.Remaining) == 0 {
			wantErr = false
		} else {
			resExe := parseExecutor(pCtx)(rn)
			if resExe.Err != nil && resExe.Err.IsFatal() {
				wantErr = true
			} else {
				resSingle := singleRootMapping(pCtx)(rn)
				res := bestMatch(resExe, resSingle)
				wantErr = res.Err != nil
			}
		}

		_, err := ParseMapping(pCtx, in)
		gotErr := err != nil

		assert.Equal(t, wantErr, gotErr, "mismatch for input %q", in)
	}
}
