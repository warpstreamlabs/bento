package field

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/bloblang/query"
	"github.com/warpstreamlabs/bento/internal/message"
)

func literalResolver(v any) Resolver {
	return NewQueryResolver(query.NewLiteralFunction("literal", v))
}

// valueCases covers every branch of value.IToString/value.IToBytes, since
// String and Bytes resolve through those two functions respectively and must
// not disagree.
func valueCases() []struct {
	name string
	val  any
} {
	return []struct {
		name string
		val  any
	}{
		{"string", "hello world"},
		{"empty string", ""},
		{"bytes", []byte("some bytes")},
		{"invalid utf8 bytes", []byte{0xff, 0xfe, 0x00, 0x41}},
		{"int64", int64(-42)},
		{"uint64", uint64(18446744073709551615)},
		{"float64", float64(1234.5678)},
		{"float64 integral", float64(42)},
		{"json.Number", json.Number("123.450")},
		{"bool true", true},
		{"bool false", false},
		{"nil", nil},
		{"time", time.Date(2026, 10, 22, 13, 14, 15, 123456789, time.UTC)},
		{"object", map[string]any{"b": 2, "a": 1}},
		{"array", []any{1, "two", true, nil}},
	}
}

// TestExpressionStringMatchesBytes is the core guard for resolving strings
// without going through []byte: for any value, the two paths must produce
// identical output.
func TestExpressionStringMatchesBytes(t *testing.T) {
	msg := message.QuickBatch([][]byte{[]byte("{}")})

	for _, tc := range valueCases() {
		t.Run(tc.name, func(t *testing.T) {
			// Single resolver, which takes the fast path in both resolve and
			// resolveString.
			e := NewExpression(literalResolver(tc.val))

			gotStr, err := e.String(0, msg)
			require.NoError(t, err)
			gotBytes, err := e.Bytes(0, msg)
			require.NoError(t, err)
			assert.Equal(t, string(gotBytes), gotStr, "single resolver")

			// Multiple resolvers, which take the concatenating path.
			multi := NewExpression(
				StaticResolver("prefix:"),
				literalResolver(tc.val),
				StaticResolver(":suffix"),
			)

			gotStr, err = multi.String(0, msg)
			require.NoError(t, err)
			gotBytes, err = multi.Bytes(0, msg)
			require.NoError(t, err)
			assert.Equal(t, string(gotBytes), gotStr, "multiple resolvers")
		})
	}
}

// TestExpressionStringPreservesRawBytes guards the specific risk of moving to
// a string path: Go strings are byte sequences, so bytes that are not valid
// UTF-8 must survive unchanged rather than being replaced.
func TestExpressionStringPreservesRawBytes(t *testing.T) {
	raw := []byte{0x00, 0xff, 0xfe, 0x80, 0x41, 0x42}
	msg := message.QuickBatch([][]byte{[]byte("{}")})

	e := NewExpression(literalResolver(raw))
	got, err := e.String(0, msg)
	require.NoError(t, err)
	assert.Equal(t, raw, []byte(got))
	assert.Len(t, got, len(raw))
}

// TestResolveStringEscapedMatchesBytes checks the escaped variants agree too,
// since ResolveString previously ignored the escaped argument entirely.
func TestResolveStringEscapedMatchesBytes(t *testing.T) {
	msg := message.QuickBatch([][]byte{[]byte("{}")})

	cases := []any{
		"with \"quotes\"",
		"with\nnewline\tand tab",
		"with \\ backslash",
		"plain",
		"",
		int64(7),
		nil,
		map[string]any{"k": "v \"quoted\""},
	}

	for _, v := range cases {
		r := literalResolver(v)

		gotStr, err := r.ResolveString(0, msg, true)
		require.NoError(t, err)
		gotBytes, err := r.ResolveBytes(0, msg, true)
		require.NoError(t, err)
		assert.Equal(t, string(gotBytes), gotStr)
	}
}

// TestStaticResolverEscapedUnchanged documents that static segments are not
// escaped, which is pre-existing behaviour shared by both paths.
func TestStaticResolverEscapedUnchanged(t *testing.T) {
	s := StaticResolver(`a "static" string`)

	gotStr, err := s.ResolveString(0, nil, true)
	require.NoError(t, err)
	gotBytes, err := s.ResolveBytes(0, nil, true)
	require.NoError(t, err)

	assert.Equal(t, `a "static" string`, gotStr)
	assert.Equal(t, string(gotBytes), gotStr)
}

// TestExpressionSizeHint pins the presize calculation, which is only a
// performance hint but should not silently drift.
func TestExpressionSizeHint(t *testing.T) {
	// All-static expressions collapse and never concatenate at resolve time.
	collapsed := NewExpression(StaticResolver("abc"), StaticResolver("de"))
	assert.Empty(t, collapsed.resolvers)
	assert.Equal(t, "abcde", collapsed.static)

	mixed := NewExpression(
		StaticResolver("abcde"),
		literalResolver("x"),
		StaticResolver("fg"),
	)
	assert.Equal(t, 7+dynamicSizeHint, mixed.sizeHint)
	assert.Equal(t, 1, mixed.NumDynamicExpressions())
}

func TestEscapeStringMatchesEscapeBytes(t *testing.T) {
	for _, in := range []string{"", "a", `"`, "\n", `nothing to escape`, "tab\there"} {
		assert.Equal(t, string(escapeBytes([]byte(in))), escapeString(in), "input %q", in)
	}
}
