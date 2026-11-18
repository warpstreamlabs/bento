package lexer

import (
	"fmt"
	"iter"
	"strings"
	"unicode"
	"unicode/utf8"
)

const eof = -1

// Position is a 1-based line and column, with columns counted in runes.
type Position struct {
	line   int
	column int
}

func (p Position) String() string {
	return fmt.Sprintf("%d:%d", p.line, p.column)
}

type Lexer struct {
	src  string
	off  int      // byte offset of the next unread rune
	pos  Position // position of the next unread rune
	last Token    // last token lexed, other than COMMENT and NEWLINE
}

func NewLexer(src string) *Lexer {
	return &Lexer{src: src, pos: Position{line: 1, column: 1}}
}

func (l *Lexer) Equals(other *Lexer) bool {
	next1, stop1 := iter.Pull(l.All())
	defer stop1()

	next2, stop2 := iter.Pull(other.All())
	defer stop2()

	for {
		a, ok1 := next1()
		for ok1 && a.Tok == NEWLINE {
			a, ok1 = next1()
		}
		b, ok2 := next2()
		for ok2 && b.Tok == NEWLINE {
			b, ok2 = next2()
		}

		if !ok1 || !ok2 {
			return ok1 == ok2
		}
		if a.Tok != b.Tok || a.Lit != b.Lit {
			return false
		}
	}
}

// Lex returns the next token. Comment literals have trailing whitespace removed.
func (l *Lexer) Lex() (Position, Token, string) {
	l.acceptWhile(isBlank)
	start, begin := l.pos, l.off

	var tok Token
	switch r := l.peek(); {
	case r == eof:
		tok = EOF
	case r == '\n':
		l.acceptWhile(unicode.IsSpace)
		tok = NEWLINE
	case r == '#':
		l.acceptWhile(isNotNewline)
		tok = COMMENT
	case r == '"':
		tok = l.lexString()
	case r == '$': // $name
		l.next()
		tok = VARIABLE
		if !isWordPart(l.peek()) {
			tok = ILLEGAL
		}
		l.acceptWhile(isWordPart)
	case r == '@': // @, @name or @"name"
		l.next()
		tok = METADATA
		if l.peek() != '"' {
			l.acceptWhile(isWordPart)
		} else if l.lexString() == ILLEGAL {
			tok = ILLEGAL
		}
	case isDigit(r):
		tok = l.lexNumber()
	case isWordPart(r):
		l.acceptWhile(isWordPart)
		tok = l.lookupWord(l.src[begin:l.off])
	default:
		tok = l.lexOperator()
	}

	lit := l.src[begin:l.off]
	if tok == COMMENT {
		lit = strings.TrimRightFunc(lit, unicode.IsSpace)
	}
	if tok != COMMENT && tok != NEWLINE {
		l.last = tok
	}
	return start, tok, lit
}

type Lexeme struct {
	Pos Position
	Tok Token
	Lit string
}

// All yields the remaining tokens, stopping before EOF. Breaking out of the loop
// leaves the rest for the next call to Lex or All.
func (l *Lexer) All() iter.Seq[Lexeme] {
	return func(yield func(Lexeme) bool) {
		for {
			pos, tok, lit := l.Lex()
			if tok == EOF || !yield(Lexeme{pos, tok, lit}) {
				return
			}
		}
	}
}

// Keywords are always followed by a space and are never path segments, so a
// word after a '.' or right before a '(' is a name: this.if, meta(), .map().
func (l *Lexer) lookupWord(word string) Token {
	if tok, ok := keywords[word]; ok && l.last != DOT && l.peek() != '(' {
		return tok
	}
	return IDENT
}

func (l *Lexer) lexOperator() Token {
	for n := min(maxOpLen, len(l.src)-l.off); n > 0; n-- {
		if tok, ok := operators[l.src[l.off:l.off+n]]; ok {
			l.advance(n)
			return tok
		}
	}
	l.next()
	return ILLEGAL
}

// A '.' is only part of a number when a digit follows it, so 3.string() lexes as
// INT DOT IDENT. Digits followed by letters are a name, as in this.0abc.
func (l *Lexer) lexNumber() Token {
	l.acceptWhile(isDigit)
	switch {
	case isWordPart(l.peek()):
		l.acceptWhile(isWordPart)
		return IDENT
	case l.peek() == '.' && l.off+1 < len(l.src) && isDigit(rune(l.src[l.off+1])):
		l.next()
		l.acceptWhile(isDigit)
		return FLOAT
	}
	return INT
}

func (l *Lexer) lexString() Token {
	const raw = `"""`
	if strings.HasPrefix(l.src[l.off:], raw) {
		l.advance(len(raw))
		end := strings.Index(l.src[l.off:], raw)
		if end < 0 {
			l.advance(len(l.src) - l.off)
			return ILLEGAL
		}
		l.advance(end + len(raw))
		return STRING
	}

	l.next() // opening quote
	for {
		switch l.next() {
		case '"':
			return STRING
		case '\\':
			if l.next() == eof {
				return ILLEGAL
			}
		case eof:
			return ILLEGAL
		}
	}
}

func (l *Lexer) next() rune {
	if l.off >= len(l.src) {
		return eof
	}
	r, w := utf8.DecodeRuneInString(l.src[l.off:])
	l.off += w
	if r == '\n' {
		l.pos.line++
		l.pos.column = 1
	} else {
		l.pos.column++
	}
	return r
}

func (l *Lexer) peek() rune {
	if l.off >= len(l.src) {
		return eof
	}
	r, _ := utf8.DecodeRuneInString(l.src[l.off:])
	return r
}

// advance consumes n bytes, keeping the position up to date.
func (l *Lexer) advance(n int) {
	for end := l.off + n; l.off < end; {
		l.next()
	}
}

func (l *Lexer) acceptWhile(pred func(rune) bool) {
	for pred(l.peek()) {
		l.next()
	}
}

func isWordPart(r rune) bool {
	return r == '_' || isDigit(r) || ('a' <= r && r <= 'z') || ('A' <= r && r <= 'Z')
}

func isDigit(r rune) bool {
	return '0' <= r && r <= '9'
}

func isBlank(r rune) bool {
	return r != '\n' && unicode.IsSpace(r)
}

func isNotNewline(r rune) bool {
	return r != '\n' && r != eof
}
