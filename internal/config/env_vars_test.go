package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnvSwapping(t *testing.T) {
	envFn := func(s string) (string, bool) {
		switch s {
		case "BENTO_TEST_FOO":
			return "", true
		case "BENTO.TEST.FOO":
			return "testfoo", true
		case "BENTO.TEST.BAR":
			return "test\nbar", true
		}
		return "", false
	}

	tests := map[string]struct {
		result      string
		errContains string
	}{
		"foo ${DOES_NOT_EXIST:} baz":                                             {result: "foo  baz"},
		"${DOES_NOT_EXIST:}":                                                     {result: ""},
		"${BENTO_TEST_FOO:}":                                                     {result: ""},
		"${BENTO.TEST.FOO:}":                                                     {result: "testfoo"},
		"foo ${BENTO_TEST_FOO:bar} baz":                                          {result: "foo bar baz"},
		"foo ${BENTO.TEST.FOO:bar} baz":                                          {result: "foo testfoo baz"},
		"foo ${BENTO.TEST.FOO} baz":                                              {result: "foo testfoo baz"},
		"foo ${BENTO_TEST_FOO:http://bar.com} baz":                               {result: "foo http://bar.com baz"},
		"foo ${BENTO_TEST_FOO:http://bar.com?wat=nuh} baz":                       {result: "foo http://bar.com?wat=nuh baz"},
		"foo ${BENTO_TEST_FOO:http://bar.com#wat} baz":                           {result: "foo http://bar.com#wat baz"},
		"foo ${BENTO_TEST_FOO:tcp://*:2020} baz":                                 {result: "foo tcp://*:2020 baz"},
		"foo ${BENTO_TEST_FOO:bar} http://bar.com baz":                           {result: "foo bar http://bar.com baz"},
		"foo ${BENTO_TEST_FOO} http://bar.com baz":                               {result: "foo  http://bar.com baz"},
		"foo ${BENTO_TEST_FOO:wat@nuh.com} baz":                                  {result: "foo wat@nuh.com baz"},
		"foo ${} baz":                                                            {result: "foo ${} baz"},
		"foo ${BENTO_TEST_FOO:foo,bar} baz":                                      {result: "foo foo,bar baz"},
		"foo ${BENTO_TEST_FOO} baz":                                              {result: "foo  baz"},
		"foo ${BENTO_TEST_FOO:${!metadata:foo}} baz":                             {result: "foo ${!metadata:foo} baz"},
		"foo ${BENTO_TEST_FOO:${!metadata:foo}${!metadata:bar}} baz":             {result: "foo ${!metadata:foo}${!metadata:bar} baz"},
		"foo ${BENTO_TEST_FOO:${!count:foo}-${!timestamp_unix_nano}.tar.gz} baz": {result: "foo ${!count:foo}-${!timestamp_unix_nano}.tar.gz baz"},
		"foo ${{BENTO_TEST_FOO:bar}} baz":                                        {result: "foo ${BENTO_TEST_FOO:bar} baz"},
		"foo ${{BENTO_TEST_FOO}} baz":                                            {result: "foo ${BENTO_TEST_FOO} baz"},
		"foo ${BENTO.TEST.BAR} baz":                                              {result: "foo test\\nbar baz"},
		"foo ${BENTO_TEST_THIS_DOESNT_EXIST_LOL} baz":                            {errContains: "required environment variables were not set: [BENTO_TEST_THIS_DOESNT_EXIST_LOL]"},
		"foo ${BENTO_TEST_NOPE_A} baz ${BENTO_TEST_NOPE_B} buz":                  {errContains: "required environment variables were not set: [BENTO_TEST_NOPE_A BENTO_TEST_NOPE_B]"},
		"foo ${DOES_NOT_EXIST::} baz":                                            {result: "foo : baz"},
	}

	for in, exp := range tests {
		out, err := ReplaceEnvVariables([]byte(in), envFn)
		if exp.errContains != "" {
			require.Error(t, err)
			assert.Contains(t, err.Error(), exp.errContains)
		} else {
			require.NoError(t, err)
			assert.Equal(t, exp.result, string(out))
		}
	}
}
