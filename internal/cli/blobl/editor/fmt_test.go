package editor

import (
	"testing"

	"github.com/warpstreamlabs/bento/internal/bloblang"
	"github.com/warpstreamlabs/bento/internal/bloblang/query"
)

func TestFormatter_Format(t *testing.T) {
	f := newFormatter()

	tests := []struct {
		name string
		src  string
		want string
	}{
		{
			name: "spacing around operators",
			src:  `root.foo=this.bar+1*2`,
			want: `root.foo = this.bar + 1 * 2`,
		},
		{
			name: "method calls and arguments",
			src:  `root.x = this .y.   uppercase( ).replace_all( "a" , "b" )`,
			want: `root.x = this.y.uppercase().replace_all("a", "b")`,
		},
		{
			name: "named arguments",
			src:  `root.v = range(start:0,stop:this.max)`,
			want: `root.v = range(start: 0, stop: this.max)`,
		},
		{
			name: "unary operators",
			src:  "root.a = - 1\nroot.b = ! this.x\nroot.c = 5 - -2\nroot.d = (-this.n)",
			want: "root.a = -1\nroot.b = !this.x\nroot.c = 5 - -2\nroot.d = (-this.n)",
		},
		{
			name: "lambda",
			src:  `root.nums = this.nums.map_each(num->num-10)`,
			want: `root.nums = this.nums.map_each(num -> num - 10)`,
		},
		{
			name: "match block",
			src:  "root.type = match this.type {\n\"doc\" => \"document\"\n      _=>this\n}",
			want: "root.type = match this.type {\n  \"doc\" => \"document\"\n  _ => this\n}",
		},
		{
			name: "inline if",
			src:  `root.sound = if this.type=="cat" {this.cat.meow} else {"silence"}`,
			want: `root.sound = if this.type == "cat" { this.cat.meow } else { "silence" }`,
		},
		{
			name: "if statement",
			src:  "if this.x {\nroot.a = 1\n} else if this.y {\nroot.a = 2\n}",
			want: "if this.x {\n  root.a = 1\n} else if this.y {\n  root.a = 2\n}",
		},
		{
			name: "several brackets opened on one line indent once",
			src:  "root = this.apply({\n\"a\": [\n1,\n2,\n],\n})",
			want: "root = this.apply({\n  \"a\": [\n    1,\n    2,\n  ],\n})",
		},
		{
			name: "method chain across lines",
			src: `root.about = "%s 🍱 is a %s %s".format(
				this.name.capitalize(),
				this.features.join(" & "),
				this.type. split("_").join(" ")
			)`,
			want: `root.about = "%s 🍱 is a %s %s".format(
  this.name.capitalize(),
  this.features.join(" & "),
  this.type.split("_").join(" ")
)`,
		},
		{
			name: "indentation is rewritten",
			src:  "root = match {\n\t\t\tthis.a > 1 => \"big\"\n _ => \"small\"\n    }",
			want: "root = match {\n  this.a > 1 => \"big\"\n  _ => \"small\"\n}",
		},
		{
			name: "surrounding whitespace is trimmed",
			src:  "\n\t\t\troot.stars = \"★\". repeat((this.stars / 100) )\n\t\t",
			want: `root.stars = "★".repeat((this.stars / 100))`,
		},
		{
			name: "comments and blank lines",
			src:  "\n\n# header   \n\n\n\nroot = this    # trailing\nlet x = 1\n\n\n",
			want: "# header\n\nroot = this # trailing\nlet x = 1",
		},
		{
			name: "strings are left alone",
			src:  "root.a = \"this . should { not . change\"\nroot.b = \"\"\"multiple\n   lines  \"\"\"",
			want: "root.a = \"this . should { not . change\"\nroot.b = \"\"\"multiple\n   lines  \"\"\"",
		},
		{
			name: "variables and metadata",
			src:  "let foo = \"yo\"\nroot.a = $foo\nroot.b = @kafka_topic\nroot.c = @\nroot.d = @\"foo bar\"\nmeta bar = \"x\"\nmeta = deleted( )",
			want: "let foo = \"yo\"\nroot.a = $foo\nroot.b = @kafka_topic\nroot.c = @\nroot.d = @\"foo bar\"\nmeta bar = \"x\"\nmeta = deleted()",
		},
		{
			name: "keywords used as names",
			src:  "root.a = meta( \"kafka_topic\" )\nroot.b = this.things.map(x -> x)\nroot.if = this.match.map[0]",
			want: "root.a = meta(\"kafka_topic\")\nroot.b = this.things.map(x -> x)\nroot.if = this.match.map[0]",
		},
		{
			name: "names starting with digits",
			src:  "let 0x = this.0abc\nroot.a = $0x",
			want: "let 0x = this.0abc\nroot.a = $0x",
		},
		{
			name: "coalesce and quoted paths",
			src:  `root.type = this.thing.(article|comment|this).type + root."foo.bar".baz`,
			want: `root.type = this.thing.(article | comment | this).type + root."foo.bar".baz`,
		},
		{
			name: "literals",
			src:  "root.a = [1,2,3]\nroot.b = {}\nroot.c = {\"k\":this.v}\nroot.d = this.items[0]\nroot.e = [{\"a\":1}]",
			want: "root.a = [1, 2, 3]\nroot.b = {}\nroot.c = { \"k\": this.v }\nroot.d = this.items[0]\nroot.e = [{ \"a\": 1 }]",
		},
		{
			name: "slices",
			src:  "root.a = this.s[1 : 3]\nroot.b = this.s[:3]",
			want: "root.a = this.s[1:3]\nroot.b = this.s[:3]",
		},
		{
			name: "unknown functions and methods",
			src:  `root = not_a_function( ).not_a_method( )`,
			want: `root = not_a_function().not_a_method()`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := f.Format(tt.src)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("got:\n%s\nwant:\n%s", got, tt.want)
			}
			if again, err := f.Format(got); err != nil || again != got {
				t.Fatalf("not idempotent, second pass gave %v:\n%s", err, again)
			}
		})
	}
}

func TestFormatter_FormatErrors(t *testing.T) {
	f := newFormatter()

	for _, src := range []string{
		"",
		"root = a & b",
		"root = \"unterminated",
		"root = $",
		"root = foo)",
		"root = foo(]",
		"root = match this {\n  _ => 1",
	} {
		if got, err := f.Format(src); err == nil {
			t.Errorf("Format(%q) = %q, want an error", src, got)
		}
	}
}

// Respacing glues "! =" into "!=" and "- >" into "->".
func TestSameTokens(t *testing.T) {
	f := newFormatter()

	for _, src := range []string{"root = ! = 1", "root = - > 1"} {
		got, err := f.format(src)
		if err != nil {
			t.Fatal(err)
		}
		if sameTokens(src, got) {
			t.Errorf("sameTokens(%q, %q): want an error", src, got)
		}
	}
}

// Catches places where the lexer's view of Bloblang has drifted from the parser's.
func TestFormatter_DocExamples(t *testing.T) {
	env := bloblang.GlobalEnvironment()
	f := newFormatter()

	var examples []query.ExampleSpec
	env.WalkFunctions(func(_ string, spec query.FunctionSpec) {
		examples = append(examples, spec.Examples...)
	})
	env.WalkMethods(func(_ string, spec query.MethodSpec) {
		examples = append(examples, spec.Examples...)
		for _, cat := range spec.Categories {
			examples = append(examples, cat.Examples...)
		}
	})

	for _, ex := range examples {
		if _, err := env.NewMapping(ex.Mapping); err != nil {
			continue // not every example is a complete mapping on its own
		}
		got, err := f.Format(ex.Mapping)
		if err != nil {
			t.Errorf("Format(%q): %v", ex.Mapping, err)
			continue
		}
		if _, err := env.NewMapping(got); err != nil {
			t.Errorf("Format(%q) no longer parses:\n%s\n%v", ex.Mapping, got, err)
			continue
		}
		if again, err := f.Format(got); err != nil || again != got {
			t.Errorf("Format(%q) is not idempotent, second pass gave %v:\n%s", ex.Mapping, err, again)
		}
	}
}
