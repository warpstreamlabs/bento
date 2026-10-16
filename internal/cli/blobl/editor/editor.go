package editor

import (
	"github.com/warpstreamlabs/bento/internal/bloblang"
)

type Editor struct {
	env           *bloblang.Environment
	syntax        *Syntax
	formatter     *Formatter
	autocompleter *Autocompleter
}

func New(env *bloblang.Environment) *Editor {
	renderer := NewRenderer(docsBaseURL)
	syntax := GenerateSyntax(env, renderer)
	return &Editor{
		env:           env,
		syntax:        &syntax,
		formatter:     NewFormatter(),
		autocompleter: NewAutocompleter(&syntax, renderer),
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
