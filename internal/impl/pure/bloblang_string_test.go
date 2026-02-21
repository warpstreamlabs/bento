package pure

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/bloblang/query"
	"github.com/warpstreamlabs/bento/internal/value"
)

func TestParseUrlencoded(t *testing.T) {
	testCases := []struct {
		name   string
		method string
		target any
		args   []any
		exp    any
	}{
		{
			name:   "simple parsing",
			method: "parse_form_url_encoded",
			target: "username=example",
			args:   []any{},
			exp:    map[string]any{"username": "example"},
		},
		{
			name:   "parsing multiple values under the same key",
			method: "parse_form_url_encoded",
			target: "usernames=userA&usernames=userB",
			args:   []any{},
			exp:    map[string]any{"usernames": []any{"userA", "userB"}},
		},
		{
			name:   "decodes data correctly",
			method: "parse_form_url_encoded",
			target: "email=example%40email.com",
			args:   []any{},
			exp:    map[string]any{"email": "example@email.com"},
		},
	}

	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			targetClone := value.IClone(test.target)
			argsClone := value.IClone(test.args).([]any)

			fn, err := query.InitMethodHelper(test.method, query.NewLiteralFunction("", targetClone), argsClone...)
			require.NoError(t, err)

			res, err := fn.Exec(query.FunctionContext{
				Maps:     map[string]query.Function{},
				Index:    0,
				MsgBatch: nil,
			})
			require.NoError(t, err)

			assert.Equal(t, test.exp, res)
			assert.Equal(t, test.target, targetClone)
			assert.Equal(t, test.args, argsClone)
		})
	}
}

func TestIsUnicode(t *testing.T) {
	testCases := []struct {
		name   string
		method string
		target any
		args   []any
		exp    any
	}{
		{
			name:   "valid utf8 string",
			method: "is_unicode",
			target: []byte("hello world"),
			args:   []any{},
			exp:    true,
		},
		{
			name:   "valid utf8 with unicode characters",
			method: "is_unicode",
			target: []byte("héllo wörld"),
			args:   []any{},
			exp:    true,
		},
		{
			name:   "empty bytes",
			method: "is_unicode",
			target: []byte{},
			args:   []any{},
			exp:    true,
		},
		{
			name:   "invalid utf8 bytes",
			method: "is_unicode",
			target: []byte{0xff, 0xfe},
			args:   []any{},
			exp:    false,
		},
		{
			name:   "utf16 le bom",
			method: "is_unicode",
			target: []byte{0xff, 0xfe, 0x00},
			args:   []any{},
			exp:    false,
		},
		{
			name:   "lone continuation byte",
			method: "is_unicode",
			target: []byte{0x80},
			args:   []any{},
			exp:    false,
		},
		{
			name:   "truncated multibyte sequence",
			method: "is_unicode",
			target: []byte{0xc3}, // start of a 2-byte sequence with no second byte
			args:   []any{},
			exp:    false,
		},
		{
			name:   "valid string appended with non-unicode",
			method: "is_unicode",
			target: []byte("test6￾"),
			args:   []any{},
			exp:    false,
		},
	}

	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			targetClone := value.IClone(test.target)
			argsClone := value.IClone(test.args).([]any)

			fn, err := query.InitMethodHelper(test.method, query.NewLiteralFunction("", targetClone), argsClone...)
			require.NoError(t, err)

			res, err := fn.Exec(query.FunctionContext{
				Maps:     map[string]query.Function{},
				Index:    0,
				MsgBatch: nil,
			})
			require.NoError(t, err)
			assert.Equal(t, test.exp, res)
			assert.Equal(t, test.target, targetClone)
			assert.Equal(t, test.args, argsClone)
		})
	}
}
