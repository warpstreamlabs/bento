package editor

import (
	"errors"
	"fmt"
	"strings"

	"github.com/warpstreamlabs/bento/internal/cli/blobl/lexer"
)

const defaultIndentSize = 2

type Formatter struct {
	indentSize int
}

func NewFormatter() *Formatter {
	return &Formatter{indentSize: defaultIndentSize}
}

func (f *Formatter) Format(source string) (string, error) {
	if source == "" {
		return "", errors.New("mapping cannot be empty")
	}

	out, err := f.format(source)
	if err != nil {
		return "", err
	}

	// Here we just re-run the lexer against the original + formatted changes
	// to ensure the lexer produces the same tokens for both.
	if !sameTokens(source, out) {
		return "", errors.New("formatting changed the token stream")
	}

	return out, nil
}

func (f *Formatter) format(source string) (string, error) {
	lines, err := splitLines(source)
	if err != nil {
		return "", err
	}
	return f.print(lines)
}

type line struct {
	tokens     []lexer.Lexeme
	afterBlank bool
}

func splitLines(source string) ([]line, error) {
	var (
		lines []line
		cur   line
	)
	for lx := range lexer.NewLexer(source).All() {
		switch lx.Tok {
		case lexer.ILLEGAL:
			return nil, fmt.Errorf("%v: unexpected %q", lx.Pos, lx.Lit)
		case lexer.NEWLINE:
			if len(cur.tokens) > 0 {
				lines = append(lines, cur)
			}
			// NOTE: A NEWLINE token is a whole run of line breaks.
			cur = line{afterBlank: len(lines) > 0 && strings.Count(lx.Lit, "\n") > 1}
		default:
			cur.tokens = append(cur.tokens, lx)
		}
	}
	if len(cur.tokens) > 0 {
		lines = append(lines, cur)
	}
	return lines, nil
}

type opener struct {
	lexer.Lexeme
	indent int // indent level of the line it was opened on
}

func (f *Formatter) print(lines []line) (string, error) {
	var (
		b    strings.Builder
		open []opener
	)
	for i, ln := range lines {
		if i > 0 {
			b.WriteByte('\n')
		}
		if ln.afterBlank {
			b.WriteByte('\n')
		}
		indent := indentFor(open, ln.tokens[0].Tok)
		b.WriteString(strings.Repeat(" ", indent*f.indentSize))

		for j, t := range ln.tokens {
			if j > 0 && spaceBefore(ln.tokens, j, innermost(open)) {
				b.WriteByte(' ')
			}
			b.WriteString(t.Lit)

			if isOpener(t.Tok) {
				open = append(open, opener{t, indent})
			} else if opening, ok := openerOf[t.Tok]; ok {
				if innermost(open) != opening {
					return "", fmt.Errorf("%v: unexpected %q", t.Pos, t.Lit)
				}
				open = open[:len(open)-1]
			}
		}
	}
	if n := len(open); n > 0 {
		return "", fmt.Errorf("%v: unclosed %q", open[n-1].Pos, open[n-1].Lit)
	}
	return b.String(), nil
}

var openerOf = map[lexer.Token]lexer.Token{lexer.RPAREN: lexer.LPAREN, lexer.RBRACE: lexer.LBRACE, lexer.RBRACKET: lexer.LBRACKET}

func isOpener(tok lexer.Token) bool {
	return tok == lexer.LPAREN || tok == lexer.LBRACE || tok == lexer.LBRACKET
}

func isCloser(tok lexer.Token) bool {
	_, ok := openerOf[tok]
	return ok
}

func innermost(open []opener) lexer.Token {
	if len(open) == 0 {
		return lexer.EOF
	}
	return open[len(open)-1].Tok
}

func indentFor(open []opener, first lexer.Token) int {
	if len(open) == 0 {
		return 0
	}
	indent := open[len(open)-1].indent
	if isCloser(first) {
		return indent
	}
	return indent + 1
}

func spaceBefore(tokens []lexer.Lexeme, i int, inside lexer.Token) bool {
	prev, cur := tokens[i-1].Tok, tokens[i].Tok
	switch {
	case cur == lexer.COMMENT:
		return true
	case isUnary(tokens, i-1): // -1, !this.x
		return false
	case prev == lexer.LPAREN, prev == lexer.LBRACKET, prev == lexer.DOT:
		return false
	case cur == lexer.RPAREN, cur == lexer.RBRACKET, cur == lexer.COMMA, cur == lexer.DOT, cur == lexer.COLON:
		return false
	case cur == lexer.LPAREN && prev == lexer.IDENT: // call: foo(
		return false
	case cur == lexer.LBRACKET && endsOperand(prev): // index: foo[0]
		return false
	case prev == lexer.LBRACE && cur == lexer.RBRACE: // empty object: {}
		return false
	case prev == lexer.COLON && inside == lexer.LBRACKET: // slice: foo[1:3]
		return false
	}
	return true
}

func isUnary(tokens []lexer.Lexeme, i int) bool {
	tok := tokens[i].Tok
	return (tok == lexer.SUB || tok == lexer.NOT) && (i == 0 || !endsOperand(tokens[i-1].Tok))
}

func endsOperand(tok lexer.Token) bool {
	switch tok {
	case lexer.IDENT, lexer.VARIABLE, lexer.METADATA, lexer.INT, lexer.FLOAT, lexer.STRING, lexer.THIS, lexer.ROOT, lexer.RPAREN, lexer.RBRACKET, lexer.RBRACE:
		return true
	}
	return false
}

func sameTokens(before, after string) bool {
	return lexer.NewLexer(before).Equals(lexer.NewLexer(after))
}
