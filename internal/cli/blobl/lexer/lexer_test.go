package lexer

import (
	"slices"
	"testing"
)

type lexeme struct {
	tok Token
	lit string
}

// Positions are left out, as TestPositions covers them.
func lexAll(src string) []lexeme {
	var out []lexeme
	for lx := range NewLexer(src).All() {
		out = append(out, lexeme{tok: lx.Tok, lit: lx.Lit})
	}
	return out
}

func TestAllBreak(t *testing.T) {
	l := NewLexer("root = this")
	for lx := range l.All() {
		if lx.Tok != ROOT {
			t.Fatalf("first token: got %v, want %v", lx.Tok, ROOT)
		}
		break
	}
	if _, tok, _ := l.Lex(); tok != ASSIGN {
		t.Fatalf("after break: got %v, want %v", tok, ASSIGN)
	}
}

func TestLex(t *testing.T) {
	tests := []struct {
		name string
		src  string
		want []lexeme
	}{
		{
			name: "assignment",
			src:  `root.x = this.y`,
			want: []lexeme{
				{tok: ROOT, lit: "root"}, {tok: DOT, lit: "."}, {tok: IDENT, lit: "x"},
				{tok: ASSIGN, lit: "="}, {tok: THIS, lit: "this"}, {tok: DOT, lit: "."},
				{tok: IDENT, lit: "y"},
			},
		},
		{
			name: "operators",
			src:  `== != >= <= && || => -> = ! > < | + - * / % , : ( ) { } [ ]`,
			want: []lexeme{
				{tok: EQ, lit: "=="}, {tok: NEQ, lit: "!="}, {tok: GTE, lit: ">="}, {tok: LTE, lit: "<="},
				{tok: AND, lit: "&&"}, {tok: OR, lit: "||"},
				{tok: MATCH_ARROW, lit: "=>"}, {tok: LAMBDA_ARROW, lit: "->"},
				{tok: ASSIGN, lit: "="}, {tok: NOT, lit: "!"}, {tok: GT, lit: ">"}, {tok: LT, lit: "<"},
				{tok: PIPE, lit: "|"}, {tok: ADD, lit: "+"}, {tok: SUB, lit: "-"},
				{tok: MUL, lit: "*"}, {tok: DIV, lit: "/"}, {tok: MOD, lit: "%"},
				{tok: COMMA, lit: ","}, {tok: COLON, lit: ":"},
				{tok: LPAREN, lit: "("}, {tok: RPAREN, lit: ")"}, {tok: LBRACE, lit: "{"},
				{tok: RBRACE, lit: "}"}, {tok: LBRACKET, lit: "["}, {tok: RBRACKET, lit: "]"},
			},
		},
		{
			name: "operators without spaces",
			src:  `a==>b`,
			want: []lexeme{
				{tok: IDENT, lit: "a"}, {tok: EQ, lit: "=="}, {tok: GT, lit: ">"},
				{tok: IDENT, lit: "b"},
			},
		},
		{
			name: "lone ampersand",
			src:  `a & b`,
			want: []lexeme{
				{tok: IDENT, lit: "a"}, {tok: ILLEGAL, lit: "&"}, {tok: IDENT, lit: "b"},
			},
		},
		{
			name: "numbers",
			src:  `42 3.14 3.string() 1.2.3`,
			want: []lexeme{
				{tok: INT, lit: "42"}, {tok: FLOAT, lit: "3.14"},
				{tok: INT, lit: "3"}, {tok: DOT, lit: "."}, {tok: IDENT, lit: "string"},
				{tok: LPAREN, lit: "("}, {tok: RPAREN, lit: ")"},
				{tok: FLOAT, lit: "1.2"}, {tok: DOT, lit: "."}, {tok: INT, lit: "3"},
			},
		},
		{
			name: "strings",
			src:  `"a\"b" "c\\"`,
			want: []lexeme{{tok: STRING, lit: `"a\"b"`}, {tok: STRING, lit: `"c\\"`}},
		},
		{
			name: "unterminated string",
			src:  `"abc`,
			want: []lexeme{{tok: ILLEGAL, lit: `"abc`}},
		},
		{
			name: "unterminated escape",
			src:  `"abc\`,
			want: []lexeme{{tok: ILLEGAL, lit: `"abc\`}},
		},
		{
			name: "keywords",
			src:  `let match if else map import meta deleted lettuce`,
			want: []lexeme{
				{tok: LET, lit: "let"}, {tok: MATCH, lit: "match"}, {tok: IF, lit: "if"},
				{tok: ELSE, lit: "else"}, {tok: MAP, lit: "map"},
				{tok: IMPORT, lit: "import"}, {tok: META, lit: "meta"},
				{tok: IDENT, lit: "deleted"}, {tok: IDENT, lit: "lettuce"},
			},
		},
		{
			name: "keywords used as names",
			src:  `this.if meta( .map( if (`,
			want: []lexeme{
				{tok: THIS, lit: "this"}, {tok: DOT, lit: "."}, {tok: IDENT, lit: "if"},
				{tok: IDENT, lit: "meta"}, {tok: LPAREN, lit: "("},
				{tok: DOT, lit: "."}, {tok: IDENT, lit: "map"}, {tok: LPAREN, lit: "("},
				{tok: IF, lit: "if"}, {tok: LPAREN, lit: "("},
			},
		},
		{
			name: "names starting with digits",
			src:  `this.0abc`,
			want: []lexeme{
				{tok: THIS, lit: "this"}, {tok: DOT, lit: "."}, {tok: IDENT, lit: "0abc"},
			},
		},
		{
			name: "variables and metadata",
			src:  `$foo $0x $ @ @kafka_topic @"foo bar"`,
			want: []lexeme{
				{tok: VARIABLE, lit: "$foo"}, {tok: VARIABLE, lit: "$0x"}, {tok: ILLEGAL, lit: "$"},
				{tok: METADATA, lit: "@"}, {tok: METADATA, lit: "@kafka_topic"},
				{tok: METADATA, lit: `@"foo bar"`},
			},
		},
		{
			name: "names are ascii",
			src:  `héllo _x1 a€`,
			want: []lexeme{
				{tok: IDENT, lit: "h"}, {tok: ILLEGAL, lit: "é"}, {tok: IDENT, lit: "llo"},
				{tok: IDENT, lit: "_x1"}, {tok: IDENT, lit: "a"}, {tok: ILLEGAL, lit: "€"},
			},
		},
		{
			name: "non-ascii digit",
			src:  `٣`,
			want: []lexeme{{tok: ILLEGAL, lit: "٣"}},
		},
		{
			name: "line breaks",
			src:  "  \n\t \n\nroot",
			want: []lexeme{{tok: NEWLINE, lit: "\n\t \n\n"}, {tok: ROOT, lit: "root"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := lexAll(tt.src); !slices.Equal(got, tt.want) {
				t.Errorf("got  %v\nwant %v", got, tt.want)
			}
		})
	}
}

func TestPositions(t *testing.T) {
	l := NewLexer("let x = \"a\nb\"\n  root")
	want := []Position{
		{1, 1}, // let
		{1, 5}, // x
		{1, 7}, // =
		{1, 9}, // "a\nb"
		{2, 3}, // NEWLINE
		{3, 3}, // root
		{3, 7}, // EOF
	}
	for i, w := range want {
		if pos, tok, _ := l.Lex(); pos != w {
			t.Errorf("token %d (%v): got %v, want %v", i, tok, pos, w)
		}
	}
}
