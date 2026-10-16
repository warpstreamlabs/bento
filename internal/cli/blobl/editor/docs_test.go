package editor

import (
	"testing"
)

func TestMarkdownToHTML(t *testing.T) {
	tests := []struct {
		name string
		md   string
		want string
	}{
		{
			name: "inline code",
			md:   "Use `this.foo` here.",
			want: "Use <code>this.foo</code> here.",
		},
		{
			name: "html is escaped",
			md:   "Returns <nil> & `a < b`.",
			want: "Returns &lt;nil&gt; &amp; <code>a &lt; b</code>.",
		},
		{
			name: "links become their text",
			md:   "See [field paths](/docs/configuration/field_paths), [merge](#merge) and [JSON Schema](https://json-schema.org/).",
			want: "See field paths, merge and JSON Schema.",
		},
		{
			name: "reference links become their text",
			md:   "Read [error handling][error_handling] and [`encode`][methods.encode].",
			want: "Read error handling and <code>encode</code>.",
		},
		{
			name: "admonitions are dropped",
			md:   "Before.\n\n:::caution\nCareful.\n:::\n",
			want: "Before.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := string(markdownToHTML(tt.md)); got != tt.want {
				t.Errorf("got  %q\nwant %q", got, tt.want)
			}
		})
	}
}

func TestRenderer(t *testing.T) {
	r := NewRenderer("https://example.com/docs/")

	fn := r.Spec(SpecDoc{Name: "now"})
	checkDocHTML(t, fn, []string{`data-docs-url="https://example.com/docs/functions#now"`}, nil)

	method := r.Spec(SpecDoc{Name: "trim", IsMethod: true})
	checkDocHTML(t, method, []string{`data-docs-url="https://example.com/docs/methods#trim"`}, nil)

	keyword := r.Keyword("root", "The <root>")
	checkDocHTML(t, keyword, []string{`<strong>root</strong>`, `The &lt;root&gt;`}, nil)
}
