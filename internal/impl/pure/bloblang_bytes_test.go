package pure

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/bloblang/query"
	"github.com/warpstreamlabs/bento/internal/value"
)

func TestParseBytes(t *testing.T) {
	testCases := []struct {
		name   string
		method string
		target any
		args   []any
		exp    any
	}{
		{
			name:   "parses decimal megabytes",
			method: "parse_bytes",
			target: "10MB",
			args:   []any{},
			exp:    uint64(10000000),
		},
		{
			name:   "parses binary mebibytes",
			method: "parse_bytes",
			target: "10MiB",
			args:   []any{},
			exp:    uint64(10485760),
		},
		{
			name:   "parses plain bytes",
			method: "parse_bytes",
			target: "512B",
			args:   []any{},
			exp:    uint64(512),
		},
		{
			name:   "parses a bare number as bytes",
			method: "parse_bytes",
			target: "1024",
			args:   []any{},
			exp:    uint64(1024),
		},
	}

	for _, test := range testCases {
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

func TestParseBytesError(t *testing.T) {
	fn, err := query.InitMethodHelper("parse_bytes", query.NewLiteralFunction("", "not-a-size"))
	if err != nil {
		// A Static method may constant-fold a literal argument at parse
		// time, surfacing the error here rather than at Exec.
		return
	}

	_, err = fn.Exec(query.FunctionContext{
		Maps:     map[string]query.Function{},
		Index:    0,
		MsgBatch: nil,
	})
	require.Error(t, err)
}
