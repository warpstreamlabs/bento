package lexer

import "fmt"

type Token int

const (
	EOF Token = iota
	ILLEGAL

	// Trivia: kept as tokens so a formatter can preserve them. A parser can
	// skip COMMENT and treat NEWLINE as a statement separator.
	COMMENT // # ... (to end of line)
	NEWLINE // a run of line breaks, including blank lines

	// Identifiers and Literals
	IDENT    // function, method and map names, path segments
	VARIABLE // $name
	METADATA // @name, @"name", or a bare @
	INT      // integer literals
	FLOAT    // floating-point literals
	STRING   // "..." or """..."""

	// Operators and punctuation. Everything in this range is lexed by
	// longest match against its string in tokens, so adding one here and
	// in tokens is all that's needed.
	operator_beg

	// Arithmetic Operators
	ADD // +
	SUB // -
	MUL // *
	DIV // /
	MOD // %

	// Comparison & Logical Operators
	AND // &&
	OR  // ||
	EQ  // ==
	NEQ // !=
	GTE // >=
	LTE // <=
	GT  // >
	LT  // <
	NOT // !

	// Assignment, Piping & Arrows
	ASSIGN       // =
	PIPE         // |
	MATCH_ARROW  // =>
	LAMBDA_ARROW // ->

	// Delimiters & Punctuation
	COMMA    // ,
	DOT      // .
	COLON    // :
	LPAREN   // (
	RPAREN   // )
	LBRACE   // {
	RBRACE   // }
	LBRACKET // [
	RBRACKET // ]

	operator_end

	// Bloblang Keywords
	keyword_beg
	ROOT   // root
	THIS   // this
	IF     // if
	MATCH  // match
	LET    // let
	MAP    // map
	ELSE   // else
	IMPORT // import
	META   // meta
	keyword_end
)

var tokens = [...]string{
	EOF:     "EOF",
	ILLEGAL: "ILLEGAL",

	COMMENT: "COMMENT",
	NEWLINE: "NEWLINE",

	IDENT:    "IDENT",
	VARIABLE: "VARIABLE",
	METADATA: "METADATA",
	INT:      "INT",
	FLOAT:    "FLOAT",
	STRING:   "STRING",

	ADD: "+",
	SUB: "-",
	MUL: "*",
	DIV: "/",
	MOD: "%",

	AND: "&&",
	OR:  "||",
	EQ:  "==",
	NEQ: "!=",
	GTE: ">=",
	LTE: "<=",
	GT:  ">",
	LT:  "<",
	NOT: "!",

	ASSIGN:       "=",
	PIPE:         "|",
	MATCH_ARROW:  "=>",
	LAMBDA_ARROW: "->",

	COMMA:    ",",
	DOT:      ".",
	COLON:    ":",
	LPAREN:   "(",
	RPAREN:   ")",
	LBRACE:   "{",
	RBRACE:   "}",
	LBRACKET: "[",
	RBRACKET: "]",

	ROOT:   "root",
	THIS:   "this",
	IF:     "if",
	MATCH:  "match",
	LET:    "let",
	MAP:    "map",
	ELSE:   "else",
	IMPORT: "import",
	META:   "meta",
}

func (t Token) String() string {
	if t >= 0 && int(t) < len(tokens) {
		return tokens[t]
	}
	return fmt.Sprintf("Token(%d)", t)
}

var (
	keywords  = tokenTable(keyword_beg, keyword_end)
	operators = tokenTable(operator_beg, operator_end)
	maxOpLen  = longestKey(operators)
)

func tokenTable(beg, end Token) map[string]Token {
	m := make(map[string]Token, end-beg-1)
	for t := beg + 1; t < end; t++ {
		m[tokens[t]] = t
	}
	return m
}

func longestKey(m map[string]Token) (n int) {
	for s := range m {
		n = max(n, len(s))
	}
	return n
}
