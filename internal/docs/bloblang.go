package docs

import (
	"github.com/warpstreamlabs/bento/v1/public/bloblang"
)

// LintBloblangMapping is function for linting a config field expected to be a
// bloblang mapping.
func LintBloblangMapping(ctx LintContext, line, col int, v any) []Lint {
	str, ok := v.(string)
	if !ok {
		return nil
	}
	if str == "" {
		return nil
	}
	_, err := ctx.conf.BloblangEnv.Parse(str)
	if err == nil {
		return nil
	}
	if mErr, ok := err.(*bloblang.ParseError); ok {
		lint := NewLintError(line+mErr.Line-1, LintBadBloblang, mErr)
		lint.Column = col + mErr.Column
		return []Lint{lint}
	}
	return []Lint{NewLintError(line, LintBadBloblang, err)}
}

// LintBloblangField is function for linting a config field expected to be an
// interpolation string.
func LintBloblangField(ctx LintContext, line, col int, v any) []Lint {
	str, ok := v.(string)
	if !ok {
		return nil
	}
	if str == "" {
		return nil
	}
	err := ctx.conf.BloblangEnv.CheckInterpolatedString(str)
	if err == nil {
		return nil
	}
	if mErr, ok := err.(*bloblang.ParseError); ok {
		lint := NewLintError(line+mErr.Line-1, LintBadBloblang, mErr)
		lint.Column = col + mErr.Column
		return []Lint{lint}
	}
	return []Lint{NewLintError(line, LintBadBloblang, err)}
}
