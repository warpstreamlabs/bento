package editor

import (
	"github.com/warpstreamlabs/bento/internal/bloblang"
)

type Editor struct {
	env           *bloblang.Environment
	syntax        *Syntax
	formatter     *formatter
	autocompleter *autocompleter
}

func New(env *bloblang.Environment) *Editor {
	r := newRenderer(docsBaseURL)
	syntax := generateSyntax(env, r)
	return &Editor{
		env:           env,
		syntax:        &syntax,
		formatter:     newFormatter(),
		autocompleter: newAutocompleter(&syntax, r),
	}
}

func (e *Editor) Execute(input, mapping string) *ExecutionResult {
	return executeMapping(e.env, input, mapping)
}

func (e *Editor) Syntax() *Syntax {
	return e.syntax
}

func (e *Editor) Format(source string) (string, error) {
	return e.formatter.Format(source)
}

func (e *Editor) Autocomplete(req AutocompletionRequest) []AutocompletionItem {
	return e.autocompleter.Generate(req)
}
