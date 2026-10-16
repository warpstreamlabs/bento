package editor

import (
	"github.com/warpstreamlabs/bento/internal/bloblang/query"
)

// Autocompleter returns every candidate for the context and leaves filtering
// by what has been typed to the editor, which matches fuzzily.
type Autocompleter struct {
	functions []AutocompletionItem // includes keywords
	methods   []AutocompletionItem
}

func NewAutocompleter(syntax *Syntax, r *Renderer) *Autocompleter {
	return &Autocompleter{
		functions: append(functionCompletions(syntax), keywordCompletions(r)...),
		methods:   methodCompletions(syntax),
	}
}

// Generate returns completions that are shared between calls
// and must not be modified.
func (a *Autocompleter) Generate(req AutocompletionRequest) []AutocompletionItem {
	if req.IsMethodContext {
		return a.methods
	}
	return a.functions
}

func functionCompletions(syntax *Syntax) []AutocompletionItem {
	completions := make([]AutocompletionItem, 0, len(syntax.Functions))
	for name, spec := range syntax.Functions {
		completions = append(completions, buildAutocompletionItem(
			name, "function", spec.Category, spec.Status, spec.Params,
		))
	}
	return completions
}

func methodCompletions(syntax *Syntax) []AutocompletionItem {
	completions := make([]AutocompletionItem, 0, len(syntax.Methods))
	for name, spec := range syntax.Methods {
		category := "general"
		if len(spec.Categories) > 0 {
			category = spec.Categories[0].Category
		}
		completions = append(completions, buildAutocompletionItem(
			name, "method", category, spec.Status, spec.Params,
		))
	}
	return completions
}

func keywordCompletions(r *Renderer) []AutocompletionItem {
	keywords := []struct {
		name        string
		description string
		score       statusPriority
	}{
		{"root", "The root of the output document", 950},
		{"this", "The current context value", 950},
		{"if", "Conditional expression", 900},
		{"match", "Pattern matching expression", 900},
		{"let", "Variable assignment", 880},
		{"map", "Create named mapping", 870},
		{"else", "Alternative branch", 850},
		{"import", "Import external mapping", 800},
		{"meta", "Access message metadata", 820},
		{"deleted", "Delete the current field", 750},
	}

	var completions []AutocompletionItem

	for _, keyword := range keywords {
		completions = append(completions, AutocompletionItem{
			Caption: keyword.name,
			Value:   keyword.name,
			Meta:    "keyword",
			Type:    "keyword",
			Score:   keyword.score,
			DocHTML: r.Keyword(keyword.name, keyword.description),
		})
	}

	return completions
}

func buildAutocompletionItem(name, itemType, category string, status query.Status, params query.Params) AutocompletionItem {
	if category == "" {
		category = "general"
	}

	item := AutocompletionItem{
		Caption: name,
		Meta:    category,
		Type:    itemType,
		Score:   getSpecScore(status),
	}

	// A snippet puts the cursor between the parentheses.
	if hasParameters(params) {
		item.Snippet = name + "($1)"
	} else {
		item.Value = name + "()"
	}

	return item
}

func getSpecScore(status query.Status) statusPriority {
	switch status {
	case query.StatusStable:
		return Stable
	case query.StatusBeta:
		return Beta
	case query.StatusExperimental:
		return Experimental
	}
	return Unknown
}

func hasParameters(params query.Params) bool {
	return len(params.Definitions) > 0 || params.Variadic
}
