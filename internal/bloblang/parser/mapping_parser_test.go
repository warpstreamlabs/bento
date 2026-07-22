package parser

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/message"
)

func TestMappingErrors(t *testing.T) {
	dir := t.TempDir()

	badMapFile := filepath.Join(dir, "bad_map.blobl")
	noMapsFile := filepath.Join(dir, "no_maps.blobl")
	goodMapFile := filepath.Join(dir, "good_map.blobl")

	require.NoError(t, os.WriteFile(badMapFile, []byte(`not a map bruh`), 0o777))
	require.NoError(t, os.WriteFile(noMapsFile, []byte(`foo = "this is valid but has no maps"`), 0o777))
	require.NoError(t, os.WriteFile(goodMapFile, []byte(`map foo { foo = "this is valid" }`), 0o777))

	tests := map[string]struct {
		mapping     string
		errContains string
	}{
		"bad variable name": {
			mapping:     `let foo+bar = baz`,
			errContains: "line 1 char 8: expected whitespace",
		},
		"bad meta name": {
			mapping:     `meta foo+bar = baz`,
			errContains: "line 1 char 9: expected =",
		},
		"no mappings": {
			mapping:     ``,
			errContains: `line 1 char 1: expected import, map, or assignment`,
		},
		"no mappings 2": {
			mapping: `
   `,
			errContains: `line 2 char 4: expected import, map, or assignment`,
		},
		"comment with no mapping": {
			mapping:     `# foobar`,
			errContains: `line 1 char 1: expected import, map, or assignment`,
		},
		"double mapping": {
			mapping:     `foo = bar bar = baz`,
			errContains: `line 1 char 11: expected line break`,
		},
		"double mapping line breaks": {
			mapping: `

foo = bar bar = baz

`,
			errContains: `line 3 char 11: expected line break`,
		},
		"double mapping line 2": {
			mapping: `let a = "a"
foo = bar bar = baz`,
			errContains: `line 2 char 11: expected line break`,
		},
		"double mapping line 3": {
			mapping: `let a = "a"
foo = bar bar = baz
	let a = "a"`,
			errContains: "line 2 char 11: expected line break",
		},
		"bad mapping": {
			mapping:     `foo wat bar`,
			errContains: `line 1 char 5: expected =`,
		},
		"bad char": {
			mapping:     `!foo = bar`,
			errContains: "line 1 char 6: expected the mapping to end here as the beginning is shorthand for `root = !foo`, but this shorthand form cannot be followed with more assignments",
		},
		"bad inline query": {
			mapping: `content().uppercase().lowercase()
meta foo = "bar"`,
			errContains: "line 2 char 1: expected the mapping to end here as the beginning is shorthand for `root = content().up...`, but this shorthand form cannot be followed with more assignments",
		},
		"bad char 2": {
			mapping: `let foo = bar
!foo = bar`,
			errContains: `line 2 char 1: expected import, map, or assignment`,
		},
		"bad char 3": {
			mapping: `let foo = bar
!foo = bar
this = that`,
			errContains: `line 2 char 1: expected import, map, or assignment`,
		},
		"bad query": {
			mapping:     `foo = blah.`,
			errContains: `line 1 char 12: required: expected method or field path`,
		},
		"bad variable assign": {
			mapping:     `let = blah`,
			errContains: `line 1 char 5: required: expected variable name`,
		},
		"double map definition": {
			mapping: `map foo {
  foo = bar
}
map foo {
  foo = bar
}
foo = bar.apply("foo")`,
			errContains: `line 4 char 1: map name collision: foo`,
		},
		"map contains meta assignment": {
			mapping: `map foo {
  meta foo = "bar"
}
foo = bar.apply("foo")`,
			errContains: `line 2 char 3: setting meta fields is not allowed within this block`,
		},
		"no name map definition": {
			mapping: `map {
  foo = bar
}
foo = bar.apply("foo")`,
			errContains: `line 1 char 5: required: expected map name`,
		},
		"no file import": {
			mapping: `import "this file doesn't exist (I hope)"

foo = bar.apply("from_import")`,
			errContains: `this file doesn't exist (I hope): no such file or directory`,
		},
		"no file directly imported mapping": {
			mapping:     `from "this file doesn't exist (I hope)"`,
			errContains: `this file doesn't exist (I hope): no such file or directory`,
		},
		"bad file import": {
			mapping: fmt.Sprintf(`import "%v"

foo = bar.apply("from_import")`, badMapFile),
			errContains: fmt.Sprintf(`line 1 char 1: failed to parse import '%v': line 1 char 5: expected =`, badMapFile),
		},
		"bad file directly imported mapping": {
			mapping:     fmt.Sprintf(`from "%v"`, badMapFile),
			errContains: fmt.Sprintf(`line 1 char 1: failed to parse import '%v': line 1 char 5: expected =`, badMapFile),
		},
		"unexpected content after directly imported mapping": {
			mapping: fmt.Sprintf(`from "%v"
root.foo = "not allowed"`, goodMapFile),
			errContains: `line 1 char 1: unexpected content after single root import: root.foo = "not allowed"`,
		},
		"no maps file import": {
			mapping: fmt.Sprintf(`import "%v"

foo = bar.apply("from_import")`, noMapsFile),
			errContains: fmt.Sprintf(`line 1 char 1: no maps to import from '%v'`, noMapsFile),
		},
		"colliding maps file import": {
			mapping: fmt.Sprintf(`map "foo" { this = that }

import "%v"

foo = bar.apply("foo")`, goodMapFile),
			errContains: fmt.Sprintf(`line 3 char 1: map name collisions from import '%v': [foo]`, goodMapFile),
		},
		"quotes at root": {
			mapping: `
"root.something" = 5 + 2`,
			errContains: "line 2 char 18: expected the mapping to end here as the beginning is shorthand for `root = \"root.someth...`, but this shorthand form cannot be followed with more assignments",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			exec, err := ParseMapping(GlobalContext(), test.mapping)
			require.NotNil(t, err)
			assert.Contains(t, err.ErrorAtPosition([]rune(test.mapping)), test.errContains)
			assert.Nil(t, exec)
		})
	}
}

func TestMappings(t *testing.T) {
	dir := t.TempDir()

	goodMapFile := filepath.Join(dir, "foo_map.blobl")
	require.NoError(t, os.WriteFile(goodMapFile, []byte(`map foo {
  foo = "this is valid"
  nested = this
}`), 0o777))

	directMapFile := filepath.Join(dir, "direct_map.blobl")
	require.NoError(t, os.WriteFile(directMapFile, []byte(`root.nested = this`), 0o777))

	type part struct {
		Content string
		Meta    map[string]any
	}

	tests := map[string]struct {
		index   int
		input   []part
		mapping string
		output  part
	}{
		"compressed arithmetic": {
			mapping: `this.foo+this.bar`,
			input: []part{
				{Content: `{"foo":5,"bar":3}`},
			},
			output: part{
				Content: `8`,
			},
		},
		"compressed arithmetic 2": {
			mapping: `this.foo-this.bar`,
			input: []part{
				{Content: `{"foo":5,"bar":3}`},
			},
			output: part{
				Content: `2`,
			},
		},
		"simple json map": {
			mapping: `foo = foo + 2
bar = "test1"
zed = deleted()`,
			input:  []part{{Content: `{"foo":10,"zed":"gone"}`}},
			output: part{Content: `{"bar":"test1","foo":12}`},
		},
		"simple json map 2": {
			mapping: `
foo = foo + 2

bar = "test1"

zed = deleted()
`,
			input:  []part{{Content: `{"foo":10,"zed":"gone"}`}},
			output: part{Content: `{"bar":"test1","foo":12}`},
		},
		"simple json map 3": {
			mapping: `
  foo = foo + 2

   bar = "test1"

zed = deleted()
  `,
			input:  []part{{Content: `{"foo":10,"zed":"gone"}`}},
			output: part{Content: `{"bar":"test1","foo":12}`},
		},
		"simple root query": {
			mapping: `{"result": foo + 2}`,
			input:   []part{{Content: `{"foo":10}`}},
			output:  part{Content: `{"result":12}`},
		},
		"simple root query 2": {
			mapping: `foo.bar`,
			input:   []part{{Content: `{"foo":{"bar":10}}`}},
			output:  part{Content: `10`},
		},
		"simple root query 3": {
			mapping: `root = foo.bar`,
			input:   []part{{Content: `{"foo":{"bar":10}}`}},
			output:  part{Content: `10`},
		},
		"simple json map with comments": {
			mapping: `
# Here's a comment
foo = foo + 2 # And here

bar = "test1"         # And one here

# And here
zed = deleted()
`,
			input:  []part{{Content: `{"foo":10,"zed":"gone"}`}},
			output: part{Content: `{"bar":"test1","foo":12}`},
		},
		"test mapping metadata and json": {
			mapping: `meta foo = foo
bar.baz = meta("bar baz")
meta "bar baz" = deleted()`,
			input: []part{
				{
					Content: `{"foo":"bar"}`,
					Meta: map[string]any{
						"bar baz": "test1",
					},
				},
			},
			output: part{
				Content: `{"bar":{"baz":"test1"}}`,
				Meta: map[string]any{
					"foo": "bar",
				},
			},
		},
		"test mapping metadata empty key": {
			mapping: `meta "" = foo.bar
meta "bar baz" = "test1"`,
			input: []part{
				{Content: `{"foo":{"bar":"baz"}}`},
			},
			output: part{
				Content: `{"foo":{"bar":"baz"}}`,
				Meta: map[string]any{
					"":        "baz",
					"bar baz": "test1",
				},
			},
		},
		"test mapping metadata and json 2": {
			mapping: `meta = foo
meta "bar baz" = "test1"`,
			input: []part{
				{Content: `{"foo":{"bar":"baz"}}`},
			},
			output: part{
				Content: `{"foo":{"bar":"baz"}}`,
				Meta: map[string]any{
					"bar":     "baz",
					"bar baz": "test1",
				},
			},
		},
		"test mapping delete and json": {
			mapping: `meta foo = foo
bar.baz = meta("bar baz")
meta = deleted()`,
			input: []part{
				{
					Content: `{"foo":"bar"}`,
					Meta: map[string]any{
						"bar baz": "test1",
					},
				},
			},
			output: part{
				Content: `{"bar":{"baz":"test1"}}`,
			},
		},
		"test variables and json": {
			mapping: `let foo = foo
let "bar baz" = "test1"
bar.baz = var("bar baz")`,
			input: []part{
				{Content: `{"foo":"bar"}`},
			},
			output: part{
				Content: `{"bar":{"baz":"test1"}}`,
			},
		},
		"map json root": {
			mapping: `root = {
  "foo": "this is a literal map"
}`,
			input:  []part{{Content: `{"zed":"gone"}`}},
			output: part{Content: `{"foo":"this is a literal map"}`},
		},
		"map json root 2": {
			mapping: `root = {
  "foo": "this is a literal map"
}
bar = "this is another thing"`,
			input:  []part{{Content: `{"zed":"gone"}`}},
			output: part{Content: `{"bar":"this is another thing","foo":"this is a literal map"}`},
		},
		"test mapping metadata without json": {
			mapping: `meta foo = "foo"
meta bar = 5 + 2`,
			input: []part{
				{Content: `this isn't json`},
			},
			output: part{
				Content: `this isn't json`,
				Meta: map[string]any{
					"foo": "foo",
					"bar": int64(7),
				},
			},
		},
		"field called root": {
			mapping: `root.root = "not set at root"`,
			input: []part{
				{Content: `this isn't json`},
			},
			output: part{
				Content: `{"root":"not set at root"}`,
			},
		},
		"quoted paths": {
			mapping: `
meta "foo bar" = "hello world"
root."bar baz".test = 5 + 2`,
			input: []part{
				{Content: `this isn't json`},
			},
			output: part{
				Content: `{"bar baz":{"test":7}}`,
				Meta: map[string]any{
					"foo bar": "hello world",
				},
			},
		},
		"test mapping raw content": {
			mapping: `meta content = content()
foo = "static"`,
			input: []part{
				{Content: `hello world`},
			},
			output: part{
				Content: `{"foo":"static"}`,
				Meta: map[string]any{
					"content": []byte(`hello world`),
				},
			},
		},
		"test mapping raw json content": {
			mapping: `meta content = content()
foo = "static"`,
			input: []part{
				{Content: `{"foo":{"bar":"baz"}}`},
			},
			output: part{
				Content: `{"foo":"static"}`,
				Meta: map[string]any{
					"content": []byte(`{"foo":{"bar":"baz"}}`),
				},
			},
		},
		"test mapping to string": {
			mapping: `root = "static string"`,
			input: []part{
				{Content: `{"this":"is a json doc"}`},
			},
			output: part{
				Content: `static string`,
			},
		},
		"test map without mapping": {
			mapping: `map foo {
  foo = "static foo"
}`,
			input: []part{
				{Content: `{"foo":"bar"}`},
			},
			output: part{
				Content: `{"foo":"bar"}`,
			},
		},
		"test maps": {
			mapping: `map foo {
  foo = "static foo"
  bar = this
  applied = ["foo"]
}
root = this.apply("foo")`,
			input: []part{
				{Content: `{"outer":{"inner":"hello world"}}`},
			},
			output: part{
				Content: `{"applied":["foo"],"bar":{"outer":{"inner":"hello world"}},"foo":"static foo"}`,
			},
		},
		"test nested maps": {
			mapping: `map foo {
  let tmp = this.apply("bar")
  foo = var("tmp")
  applied = var("tmp").applied.merge("foo")
  foo.applied = deleted()
}
map bar {
  static = "this is valid"
  bar = this
  applied = ["bar"]
}
root = this.apply("foo")`,
			input: []part{
				{Content: `{"outer":{"inner":"hello world"}}`},
			},
			output: part{
				Content: `{"applied":["bar","foo"],"foo":{"bar":{"outer":{"inner":"hello world"}},"static":"this is valid"}}`,
			},
		},
		"test single root mapping": {
			mapping: `"foo" == "bar"`,
			input: []part{
				{Content: ``},
			},
			output: part{
				Content: `false`,
			},
		},
		"test single root mapping with blobl shebang": {
			mapping: `#!blobl
"foo" == "bar"`,
			input: []part{
				{Content: ``},
			},
			output: part{
				Content: `false`,
			},
		},
		"test imported map": {
			mapping: fmt.Sprintf(`import "%v"

root = this.apply("foo")`, goodMapFile),
			input: []part{
				{Content: `{"outer":{"inner":"hello world"}}`},
			},
			output: part{
				Content: `{"foo":"this is valid","nested":{"outer":{"inner":"hello world"}}}`,
			},
		},
		"test imported map with blobl shebang": {
			mapping: fmt.Sprintf(`#!blobl
import "%v"

root = this.apply("foo")`, goodMapFile),
			input: []part{
				{Content: `{"outer":{"inner":"hello world"}}`},
			},
			output: part{
				Content: `{"foo":"this is valid","nested":{"outer":{"inner":"hello world"}}}`,
			},
		},
		"test directly imported mapping": {
			mapping: fmt.Sprintf(`from "%v"`, directMapFile),
			input: []part{
				{Content: `{"inner":"hello world"}`},
			},
			output: part{
				Content: `{"nested":{"inner":"hello world"}}`,
			},
		},
		"test directly imported mapping with blobl shebang": {
			mapping: fmt.Sprintf(`#!blobl

from "%v"`, directMapFile),
			input: []part{
				{Content: `{"inner":"hello world"}`},
			},
			output: part{
				Content: `{"nested":{"inner":"hello world"}}`,
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			msg := message.QuickBatch(nil)
			for _, p := range test.input {
				part := message.NewPart([]byte(p.Content))
				for k, v := range p.Meta {
					part.MetaSetMut(k, v)
				}
				msg = append(msg, part)
			}
			if test.output.Meta == nil {
				test.output.Meta = map[string]any{}
			}

			exec, perr := ParseMapping(GlobalContext(), test.mapping)
			require.Nil(t, perr)

			resPart, err := exec.MapPart(test.index, msg)
			require.NoError(t, err)

			newPart := part{
				Content: string(resPart.AsBytes()),
				Meta:    map[string]any{},
			}
			_ = resPart.MetaIterMut(func(k string, v any) error {
				newPart.Meta[k] = v
				return nil
			})

			assert.Equal(t, test.output, newPart)
		})
	}
}

func BenchmarkMappingParser(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := ParseMapping(GlobalContext(), `
root.foo = this.foo.uppercase()
root.bar = this.bar.lowercase()
root.baz = this.baz.slice(0, 10)
`)
		if err != nil {
			b.Error(err.Error())
		}
	}
}

// BenchmarkMappingParserReusedContext measures parsing performance when the
// same Context (and therefore the same parser cache) is reused across
// parses, which reflects how a long-running service typically uses a single
// bloblang Environment/Context to parse many mappings over its lifetime.
func BenchmarkMappingParserReusedContext(b *testing.B) {
	pCtx := GlobalContext()
	for i := 0; i < b.N; i++ {
		_, err := ParseMapping(pCtx, `
root.foo = this.foo.uppercase()
root.bar = this.bar.lowercase()
root.baz = this.baz.slice(0, 10)
`)
		if err != nil {
			b.Error(err.Error())
		}
	}
}

// complexMapping exercises deeply recursive/branching syntax: nested lambda
// expressions (map_each/filter/sort_by), multiple if/else if/else chains,
// a match expression, and many chained method/function calls across many
// assignment statements. This is representative of real-world mappings that
// are far more demanding on the parser than a handful of flat assignments.
const complexMapping = `
let threshold = 10

root.id = this.id
root.name = this.name.trim().capitalize()
root.tags = this.tags.map_each(t -> t.lowercase().trim())
root.active_items = this.items.filter(item -> item.status == "active" && item.qty > 0)
root.sorted_items = this.items.filter(item -> item.qty > 0).sort_by(item -> item.priority)
root.total = this.items.map_each(item -> item.price * item.qty).sum()
root.grouped = this.items.map_each(entry -> {
  "id": entry.id,
  "label": entry.name.uppercase(),
  "flag": entry.qty > $threshold,
})

root.status = match this.status {
  this == "pending" => "PENDING"
  this == "active" => "ACTIVE"
  this == "done" => "DONE"
  _ => "UNKNOWN"
}

if this.type == "a" {
  root.category = "Alpha"
} else if this.type == "b" {
  root.category = "Beta"
} else if this.type == "c" {
  root.category = "Gamma"
} else {
  root.category = "Other"
}

root.nested = this.groups.map_each(group -> group.members.filter(m -> m.active).map_each(m -> m.name.uppercase()))
root.summary = "%s has %v active items worth %v".format(this.name, root.active_items.length(), root.total)
meta processed = true
`

// BenchmarkMappingParserComplex measures parsing performance for a mapping
// with realistic complexity: nested lambdas, match/if branching, chained
// method calls and many statements. This is the scenario where the
// queryParser cache is expected to matter most, since deep recursion is
// exactly where the pre-cache implementation rebuilt combinator trees
// repeatedly.
func BenchmarkMappingParserComplex(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := ParseMapping(GlobalContext(), complexMapping)
		if err != nil {
			b.Error(err.Error())
		}
	}
}

// BenchmarkMappingParserComplexReusedContext is the reused-Context
// counterpart to BenchmarkMappingParserComplex, reflecting a long-running
// service parsing many complex mappings against a single Environment.
func BenchmarkMappingParserComplexReusedContext(b *testing.B) {
	pCtx := GlobalContext()
	for i := 0; i < b.N; i++ {
		_, err := ParseMapping(pCtx, complexMapping)
		if err != nil {
			b.Error(err.Error())
		}
	}
}

// complexMappingInput is realistic JSON input for complexMapping, used to
// exercise the executor (not just the parser) against real data: multiple
// items with varying status/qty/priority, nested groups with active/inactive
// members, and a status value that exercises every match case.
const complexMappingInput = `{
  "id": "order-42",
  "name": "  acme order  ",
  "status": "active",
  "type": "b",
  "tags": [" URGENT ", " Fragile "],
  "items": [
    {"id": "i1", "name": "widget", "status": "active", "qty": 3, "price": 10.5, "priority": 2},
    {"id": "i2", "name": "gadget", "status": "inactive", "qty": 0, "price": 5.0, "priority": 1},
    {"id": "i3", "name": "gizmo", "status": "active", "qty": 1, "price": 20.0, "priority": 5}
  ],
  "groups": [
    {"members": [{"name": "alice", "active": true}, {"name": "bob", "active": false}]},
    {"members": [{"name": "carol", "active": true}]}
  ]
}`

// TestComplexMappingExecutesCorrectly parses complexMapping and executes it
// against real message data, asserting on the actual output content and
// metadata rather than just checking for a nil parse error. This is the
// gap left by the pure-parsing benchmarks/tests: this test exercises the
// full parse -> build executor -> Exec pipeline with real business logic
// (map_each/filter/sort_by/match/if-else/format), and is used to prove the
// cache changes don't alter runtime behaviour, not just parse success.
func TestComplexMappingExecutesCorrectly(t *testing.T) {
	exec, perr := ParseMapping(GlobalContext(), complexMapping)
	require.Nil(t, perr)

	msg := message.QuickBatch([][]byte{[]byte(complexMappingInput)})

	resPart, err := exec.MapPart(0, msg)
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(resPart.AsBytes(), &got))

	assert.Equal(t, "order-42", got["id"])
	assert.Equal(t, "Acme Order", got["name"])
	assert.Equal(t, []any{"urgent", "fragile"}, got["tags"])
	assert.Equal(t, "ACTIVE", got["status"])
	assert.Equal(t, "Beta", got["category"])

	activeItems, ok := got["active_items"].([]any)
	require.True(t, ok)
	assert.Len(t, activeItems, 2, "only i1 and i3 are status=active with qty>0")

	sortedItems, ok := got["sorted_items"].([]any)
	require.True(t, ok)
	require.Len(t, sortedItems, 2)
	firstSorted := sortedItems[0].(map[string]any)
	assert.Equal(t, "i1", firstSorted["id"], "i2 is filtered out by qty>0; remaining i1(priority=2) sorts before i3(priority=5)")

	// total = sum(price*qty) across ALL items (10.5*3 + 5.0*0 + 20.0*1) = 51.5
	assert.InDelta(t, 51.5, got["total"], 0.0001)

	grouped, ok := got["grouped"].([]any)
	require.True(t, ok)
	require.Len(t, grouped, 3)
	firstGroup := grouped[0].(map[string]any)
	assert.Equal(t, "i1", firstGroup["id"])
	assert.Equal(t, "WIDGET", firstGroup["label"])
	assert.Equal(t, false, firstGroup["flag"], "qty=3 is not > threshold=10")

	nested, ok := got["nested"].([]any)
	require.True(t, ok)
	require.Len(t, nested, 2)
	assert.Equal(t, []any{"ALICE"}, nested[0], "only alice is active in group 1")
	assert.Equal(t, []any{"CAROL"}, nested[1], "only carol is active in group 2")

	assert.Contains(t, got["summary"], "acme order")
	assert.Contains(t, got["summary"], "2 active items")

	processedMeta, exists := resPart.MetaGetMut("processed")
	require.True(t, exists)
	assert.Equal(t, true, processedMeta)
}

// BenchmarkMappingExecComplex measures the full end-to-end cost of parsing
// AND executing complexMapping against real input data, as opposed to the
// parse-only benchmarks above. The queryParser cache only affects the parse
// phase, so this benchmark shows how much of the total (parse + execute)
// cost the cache improvement actually addresses in a realistic workload.
func BenchmarkMappingExecComplex(b *testing.B) {
	inputBytes := []byte(complexMappingInput)
	for i := 0; i < b.N; i++ {
		exec, perr := ParseMapping(GlobalContext(), complexMapping)
		if perr != nil {
			b.Fatal(perr.Error())
		}
		msg := message.QuickBatch([][]byte{inputBytes})
		if _, err := exec.MapPart(0, msg); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMappingExecComplexReusedContext is the reused-Context counterpart
// to BenchmarkMappingExecComplex.
func BenchmarkMappingExecComplexReusedContext(b *testing.B) {
	pCtx := GlobalContext()
	inputBytes := []byte(complexMappingInput)
	for i := 0; i < b.N; i++ {
		exec, perr := ParseMapping(pCtx, complexMapping)
		if perr != nil {
			b.Fatal(perr.Error())
		}
		msg := message.QuickBatch([][]byte{inputBytes})
		if _, err := exec.MapPart(0, msg); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMappingExecOnlyComplex measures ONLY the execution cost (parsing
// once outside the loop), isolating how much of the total pipeline cost is
// unaffected by the parser cache. Comparing this against
// BenchmarkMappingExecComplex shows the parse:execute cost ratio.
func BenchmarkMappingExecOnlyComplex(b *testing.B) {
	exec, perr := ParseMapping(GlobalContext(), complexMapping)
	if perr != nil {
		b.Fatal(perr.Error())
	}
	inputBytes := []byte(complexMappingInput)
	for i := 0; i < b.N; i++ {
		msg := message.QuickBatch([][]byte{inputBytes})
		if _, err := exec.MapPart(0, msg); err != nil {
			b.Fatal(err)
		}
	}
}
