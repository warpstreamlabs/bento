package test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/fatih/color"
	"github.com/nsf/jsondiff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/v1/internal/bundle"
	"github.com/warpstreamlabs/bento/v1/internal/docs"
	"github.com/warpstreamlabs/bento/v1/internal/filepath/ifs"
	"github.com/warpstreamlabs/bento/v1/internal/message"
)

func condsFromYAML(t testing.TB, str string, args ...any) OutputConditionsMap {
	t.Helper()

	node, err := docs.UnmarshalYAML(fmt.Appendf(nil, str, args...))
	require.NoError(t, err)

	pConf, err := outputFields().ParsedConfigFromAny(node)
	require.NoError(t, err)

	m, err := OutputConditionsFromParsed(pConf)
	require.NoError(t, err)

	return m
}

func TestConditionUnmarshal(t *testing.T) {
	conds := condsFromYAML(t, `
content_equals: "foo bar"
metadata_equals:
  foo: bar
`)

	exp := OutputConditionsMap{
		"content_equals": ContentEqualsCondition("foo bar"),
		"metadata_equals": MetadataEqualsCondition{
			"foo": "bar",
		},
	}

	assert.Equal(t, exp, conds)
}

func TestBloblangConditionHappy(t *testing.T) {
	conds := condsFromYAML(t, `
bloblang: 'content() == "foo bar"'
`)

	assert.Empty(t, conds.CheckAll(ifs.OS(), "", message.NewPart([]byte("foo bar"))))
	assert.NotEmpty(t, conds.CheckAll(ifs.OS(), "", message.NewPart([]byte("bar baz"))))
}

func TestBloblangConditionSad(t *testing.T) {
	pConf, err := outputFields().ParsedConfigFromAny(map[string]any{
		"bloblang": "content() ==",
	})
	require.NoError(t, err)

	_, err = OutputConditionsFromParsed(pConf)
	require.EqualError(t, err, "bloblang: expected query, but reached end of input")
}

func TestConditionUnmarshalUnknownCond(t *testing.T) {
	node, err := docs.UnmarshalYAML([]byte(`
this_doesnt_exist: "foo bar"
metadata_equals:
  key:   "foo"
  value: "bar"
`))
	require.NoError(t, err)

	lints := outputFields().LintYAML(docs.NewLintContext(docs.NewLintConfig(bundle.GlobalEnvironment)), node)
	require.Len(t, lints, 1)
	assert.Equal(t, "(2,1) field this_doesnt_exist not recognised", lints[0].Error())
}

func TestConditionCheckAll(t *testing.T) {
	color.NoColor = true

	conds := OutputConditionsMap{
		"content_equals": ContentEqualsCondition("foo bar"),
		"metadata_equals": &MetadataEqualsCondition{
			"foo": "bar",
		},
	}

	part := message.NewPart([]byte("foo bar"))
	part.MetaSetMut("foo", "bar")
	errs := conds.CheckAll(ifs.OS(), "", part)
	require.Empty(t, errs)

	part = message.NewPart([]byte("nope"))
	errs = conds.CheckAll(ifs.OS(), "", part)
	require.Len(t, errs, 2)
	assert.Contains(t, "content_equals: content mismatch\n  expected: foo bar\n  received: nope", errs[0].Error())
	assert.Contains(t, "metadata_equals: metadata key 'foo' expected but not found", errs[1].Error())

	part = message.NewPart([]byte("foo bar"))
	part.MetaSetMut("foo", "wrong")
	errs = conds.CheckAll(ifs.OS(), "", part)
	if exp, act := 1, len(errs); exp != act {
		t.Fatalf("Wrong count of errors: %v != %v", act, exp)
	}
	if exp, act := "metadata_equals: metadata key 'foo' mismatch\n  expected: bar\n  received: wrong", errs[0].Error(); exp != act {
		t.Errorf("Wrong error: %v != %v", act, exp)
	}

	part = message.NewPart([]byte("wrong"))
	part.MetaSetMut("foo", "bar")
	errs = conds.CheckAll(ifs.OS(), "", part)
	if exp, act := 1, len(errs); exp != act {
		t.Fatalf("Wrong count of errors: %v != %v", act, exp)
	}
	if exp, act := "content_equals: content mismatch\n  expected: foo bar\n  received: wrong", errs[0].Error(); exp != act {
		t.Errorf("Wrong error: %v != %v", act, exp)
	}
}

func TestContentCondition(t *testing.T) {
	color.NoColor = true

	cond := ContentEqualsCondition("foo bar")

	type testCase struct {
		name     string
		input    string
		expected error
	}

	tests := []testCase{
		{
			name:     "positive 1",
			input:    "foo bar",
			expected: nil,
		},
		{
			name:     "negative 1",
			input:    "foo",
			expected: errors.New("content mismatch\n  expected: foo bar\n  received: foo"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			actErr := cond.Check(ifs.OS(), "", message.NewPart([]byte(test.input)))
			if test.expected == nil && actErr == nil {
				return
			}
			if test.expected == nil && actErr != nil {
				tt.Errorf("Wrong result, expected %v, received %v", test.expected, actErr)
				return
			}
			if test.expected != nil && actErr == nil {
				tt.Errorf("Wrong result, expected %v, received %v", test.expected, actErr)
				return
			}
			if exp, act := test.expected.Error(), actErr.Error(); exp != act {
				tt.Errorf("Wrong result, expected %v, received %v", exp, act)
			}
		})
	}
}

func TestContentMatchesCondition(t *testing.T) {
	color.NoColor = true

	matchPattern := "^foo [a-z]+ bar$"
	cond := ContentMatchesCondition(matchPattern)

	type testCase struct {
		name     string
		input    string
		expected error
	}

	tests := []testCase{
		{
			name:     "positive 1",
			input:    "foo and bar",
			expected: nil,
		},
		{
			name:     "negative 1",
			input:    "foo",
			expected: fmt.Errorf("pattern mismatch\n   pattern: %s\n  received: foo", matchPattern),
		},
		{
			name:     "negative 2",
			input:    "foo & bar",
			expected: fmt.Errorf("pattern mismatch\n   pattern: %s\n  received: foo & bar", matchPattern),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			actErr := cond.Check(ifs.OS(), "", message.NewPart([]byte(test.input)))
			if test.expected == nil && actErr == nil {
				return
			}
			if test.expected == nil && actErr != nil {
				tt.Errorf("Wrong result, expected %v, received %v", test.expected, actErr)
				return
			}
			if test.expected != nil && actErr == nil {
				tt.Errorf("Wrong result, expected %v, received %v", test.expected, actErr)
				return
			}
			if exp, act := test.expected.Error(), actErr.Error(); exp != act {
				tt.Errorf("Wrong result, expected %v, received %v", exp, act)
			}
		})
	}
}

func TestMetadataEqualsCondition(t *testing.T) {
	color.NoColor = true

	cond := MetadataEqualsCondition{
		"foo": "bar",
	}

	type testCase struct {
		name     string
		input    map[string]string
		expected error
	}

	tests := []testCase{
		{
			name: "positive 1",
			input: map[string]string{
				"foo": "bar",
			},
			expected: nil,
		},
		{
			name:     "negative 1",
			input:    map[string]string{},
			expected: errors.New("metadata key 'foo' expected but not found"),
		},
		{
			name: "negative 2",
			input: map[string]string{
				"foo": "not bar",
			},
			expected: errors.New("metadata key 'foo' mismatch\n  expected: bar\n  received: not bar"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			part := message.NewPart(nil)
			for k, v := range test.input {
				part.MetaSetMut(k, v)
			}
			actErr := cond.Check(ifs.OS(), "", part)
			if test.expected == nil && actErr == nil {
				return
			}
			if test.expected == nil && actErr != nil {
				tt.Errorf("Wrong result, expected %v, received %v", test.expected, actErr)
				return
			}
			if test.expected != nil && actErr == nil {
				tt.Errorf("Wrong result, expected %v, received %v", test.expected, actErr)
				return
			}
			if exp, act := test.expected.Error(), actErr.Error(); exp != act {
				tt.Errorf("Wrong result, expected %v, received %v", exp, act)
			}
		})
	}
}

func TestJSONEqualsCondition(t *testing.T) {
	color.NoColor = true

	cond := ContentJSONEqualsCondition(`{"foo":"bar","bim":"bam"}`)

	type testCase struct {
		name  string
		input string
	}

	tests := []testCase{
		{
			name:  "positive 1",
			input: `{"foo":"bar","bim":"bam"}`,
		},
		{
			name:  "positive 2",
			input: `{ "bim": "bam", "foo": "bar" }`,
		},
		{
			name:  "negative 1",
			input: "foo",
		},
		{
			name:  "negative 2",
			input: `{"foo":"bar"}`,
		},
	}

	jdopts := jsondiff.DefaultConsoleOptions()
	for _, test := range tests {
		var expected error
		diff, explanation := jsondiff.Compare([]byte(test.input), []byte(cond), &jdopts)
		if diff != jsondiff.FullMatch {
			expected = fmt.Errorf("JSON content mismatch\n%v", explanation)
		}

		t.Run(test.name, func(tt *testing.T) {
			actErr := cond.Check(ifs.OS(), "", message.NewPart([]byte(test.input)))
			if expected == nil && actErr == nil {
				return
			}
			if expected == nil && actErr != nil {
				tt.Errorf("Wrong result, expected %v, received %v", expected, actErr)
				return
			}
			if expected != nil && actErr == nil {
				tt.Errorf("Wrong result, expected %v, received %v", expected, actErr)
				return
			}
			if exp, act := expected.Error(), actErr.Error(); exp != act {
				tt.Errorf("Wrong result, expected %v, received %v", exp, act)
			}
		})
	}
}

func TestJSONContainsCondition(t *testing.T) {
	color.NoColor = true

	cond := ContentJSONContainsCondition(`{"foo":"bar","bim":"bam"}`)

	type testCase struct {
		name  string
		input string
	}

	tests := []testCase{
		{
			name:  "positive 1",
			input: `{"foo":"bar","bim":"bam"}`,
		},
		{
			name:  "positive 2",
			input: `{ "bim": "bam", "foo": "bar", "baz": [1, 2, 3] }`,
		},
		{
			name:  "negative 1",
			input: `{"foo":"baz","bim":"bam"}`,
		},
		{
			name:  "negative 2",
			input: `{"foo":"bar"}`,
		},
	}

	jdopts := jsondiff.DefaultConsoleOptions()
	for _, test := range tests {
		var expected error
		diff, explanation := jsondiff.Compare([]byte(test.input), []byte(cond), &jdopts)
		if diff != jsondiff.FullMatch && diff != jsondiff.SupersetMatch {
			expected = fmt.Errorf("JSON superset mismatch\n%v", explanation)
		}

		t.Run(test.name, func(tt *testing.T) {
			actErr := cond.Check(ifs.OS(), "", message.NewPart([]byte(test.input)))
			if expected == nil && actErr == nil {
				return
			}
			if expected == nil && actErr != nil {
				tt.Errorf("Wrong result, expected %v, received %v", expected, actErr)
				return
			}
			if expected != nil && actErr == nil {
				tt.Errorf("Wrong result, expected %v, received %v", expected, actErr)
				return
			}
			if exp, act := expected.Error(), actErr.Error(); exp != act {
				tt.Errorf("Wrong result, expected %v, received %v", exp, act)
			}
		})
	}
}

func TestFileEqualsCondition(t *testing.T) {
	color.NoColor = true

	tmpDir := t.TempDir()

	uppercasedPath := filepath.Join(tmpDir, "inner", "uppercased.txt")
	notUppercasedPath := filepath.Join(tmpDir, "not_uppercased.txt")

	require.NoError(t, os.MkdirAll(filepath.Dir(uppercasedPath), 0o755))
	require.NoError(t, os.WriteFile(uppercasedPath, []byte(`FOO BAR BAZ`), 0o644))
	require.NoError(t, os.WriteFile(notUppercasedPath, []byte(`foo bar baz`), 0o644))

	type testCase struct {
		name        string
		path        string
		input       string
		errContains string
	}

	tests := []testCase{
		{
			name:  "positive 1",
			path:  `./inner/uppercased.txt`,
			input: `FOO BAR BAZ`,
		},
		{
			name:  "positive 2",
			path:  `./not_uppercased.txt`,
			input: `foo bar baz`,
		},
		{
			name:        "negative 1",
			path:        `./inner/uppercased.txt`,
			input:       `foo bar baz`,
			errContains: "content mismatch",
		},
		{
			name:        "negative 2",
			path:        `./not_uppercased.txt`,
			input:       `FOO BAR BAZ`,
			errContains: "content mismatch",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			actErr := FileEqualsCondition(test.path).Check(ifs.OS(), tmpDir, message.NewPart([]byte(test.input)))
			if test.errContains == "" {
				assert.NoError(t, actErr)
			} else {
				assert.Contains(t, actErr.Error(), test.errContains)
			}
		})
	}
}

func TestFileJSONEqualsCondition(t *testing.T) {
	color.NoColor = true

	tmpDir := t.TempDir()

	// Contents of both files are unordered.
	unformattedPath := filepath.Join(tmpDir, "inner", "unformatted.json")
	formattedPath := filepath.Join(tmpDir, "formatted.json")

	require.NoError(t, os.MkdirAll(filepath.Dir(unformattedPath), 0o755))
	require.NoError(t, os.WriteFile(unformattedPath, []byte(`{"id":123456,"name":"Bento"}`), 0o644))
	require.NoError(t, os.WriteFile(formattedPath, []byte(
		`{
    "id": 123456,
    "name": "Bento"
}`), 0o644))

	type testCase struct {
		name        string
		path        string
		input       string
		errContains string
	}

	tests := []testCase{
		{
			name:  "positive 1",
			path:  `./inner/unformatted.json`,
			input: `{"name":"Bento","id":123456}`,
		},
		{
			name:  "positive 2",
			path:  `./formatted.json`,
			input: `{"name":"Bento","id":123456}`,
		},
		{
			name:        "negative 1",
			path:        `./inner/unformatted.json`,
			input:       `{"name":"Bento"}`,
			errContains: "content mismatch",
		},
		{
			name:        "negative 2",
			path:        `./formatted.json`,
			input:       `{"name":"Bento"}`,
			errContains: "content mismatch",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			testPath := filepath.Join(tmpDir, test.path)
			actErr := FileJSONEqualsCondition(testPath).Check(ifs.OS(), "", message.NewPart([]byte(test.input)))
			if test.errContains == "" {
				assert.NoError(t, actErr)
			} else {
				assert.Contains(t, actErr.Error(), test.errContains)
			}
		})
	}
}

func TestFileJSONContainsCondition(t *testing.T) {
	color.NoColor = true

	tmpDir := t.TempDir()

	// Contents of both files are unordered.
	unformattedPath := filepath.Join(tmpDir, "inner", "unformatted.json")
	formattedPath := filepath.Join(tmpDir, "formatted.json")

	require.NoError(t, os.MkdirAll(filepath.Dir(unformattedPath), 0o755))
	require.NoError(t, os.WriteFile(unformattedPath, []byte(`{"id":123456,"name":"Bento"}`), 0o644))
	require.NoError(t, os.WriteFile(formattedPath, []byte(
		`{
    "id": 123456,
    "name": "Bento"
}`), 0o644))

	type testCase struct {
		name        string
		path        string
		input       string
		errContains string
	}

	tests := []testCase{
		{
			name:  "positive 1",
			path:  `./inner/unformatted.json`,
			input: `{"name":"Bento","id":123456}`,
		},
		{
			name:  "positive 2",
			path:  `./formatted.json`,
			input: `{"name":"Bento","id":123456}`,
		},
		{
			name:  "positive 3",
			path:  `./inner/unformatted.json`,
			input: `{"name":"Bento","id":123456,"file":"test"}`,
		},
		{
			name:        "negative 1",
			path:        `./inner/unformatted.json`,
			input:       `{"name":"Bento", "file":"test"}`,
			errContains: "JSON superset mismatch",
		},
		{
			name:        "negative 2",
			path:        `./formatted.json`,
			input:       `{"file":"test"}`,
			errContains: "JSON superset mismatch",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			testPath := filepath.Join(tmpDir, test.path)
			actErr := FileJSONContainsCondition(testPath).Check(ifs.OS(), "", message.NewPart([]byte(test.input)))
			if test.errContains == "" {
				assert.NoError(t, actErr)
			} else {
				assert.Contains(t, actErr.Error(), test.errContains)
			}
		})
	}
}
