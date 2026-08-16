// Package query implements kafman's search language for Kafka records.
//
// The design follows yozefu's: clauses may appear in any order, so
// `from begin limit 10 key == "x"` and `key == "x" limit 10 from begin` are the
// same query. That matters because people type these interactively and should
// not have to remember a canonical order.
//
// A parse error carries the position and the unconsumed tail, which is what lets
// the input line underline exactly the part it could not understand instead of
// showing a modal.
package query

import (
	"fmt"
	"strings"
	"unicode"
)

// tokenKind classifies a token.
type tokenKind int

const (
	tokenEOF tokenKind = iota
	tokenIdent
	tokenString
	tokenNumber
	tokenOperator
	tokenLParen
	tokenRParen
)

func (k tokenKind) String() string {
	switch k {
	case tokenEOF:
		return "end of input"
	case tokenIdent:
		return "name"
	case tokenString:
		return "string"
	case tokenNumber:
		return "number"
	case tokenOperator:
		return "operator"
	case tokenLParen:
		return "("
	case tokenRParen:
		return ")"
	}
	return "token"
}

// token is one lexical unit with the byte range it came from, so an error can
// point back at the exact text.
type token struct {
	kind  tokenKind
	text  string
	start int
	end   int
}

// Error is a parse failure that knows where it happened.
type Error struct {
	Message string

	// Position is the byte offset where the problem starts.
	Position int

	// Tail is the part of the input that could not be understood. The query
	// input renders it underlined, in place.
	Tail string
}

func (e *Error) Error() string {
	if e.Tail == "" {
		return e.Message
	}
	return fmt.Sprintf("%s: %q", e.Message, e.Tail)
}

// lexer turns a query string into tokens.
type lexer struct {
	input  string
	pos    int
	tokens []token
}

// operator characters that can start a multi-character operator.
const operatorChars = "=!<>~&|"

func lex(input string) ([]token, error) {
	l := &lexer{input: input}
	if err := l.run(); err != nil {
		return nil, err
	}
	return l.tokens, nil
}

func (l *lexer) run() error {
	for {
		l.skipSpace()
		if l.pos >= len(l.input) {
			l.emit(token{kind: tokenEOF, start: l.pos, end: l.pos})
			return nil
		}

		start := l.pos
		c := l.input[l.pos]

		switch {
		case c == '(':
			l.pos++
			l.emit(token{kind: tokenLParen, text: "(", start: start, end: l.pos})

		case c == ')':
			l.pos++
			l.emit(token{kind: tokenRParen, text: ")", start: start, end: l.pos})

		case c == '"' || c == '\'':
			if err := l.lexString(c); err != nil {
				return err
			}

		case isDigit(c):
			l.lexNumber()

		case c == '-':
			// A lone minus, as in `from end - 500`. It is lexed separately from
			// the operator run so it cannot merge with a neighbouring `>` or `=`.
			l.pos++
			l.emit(token{kind: tokenOperator, text: "-", start: start, end: l.pos})

		case strings.IndexByte(operatorChars, c) >= 0:
			l.lexOperator()

		case isIdentStart(c):
			l.lexIdent()

		default:
			return &Error{
				Message:  "unexpected character",
				Position: start,
				Tail:     l.input[start:],
			}
		}
	}
}

func (l *lexer) emit(t token) { l.tokens = append(l.tokens, t) }

func (l *lexer) skipSpace() {
	for l.pos < len(l.input) && unicode.IsSpace(rune(l.input[l.pos])) {
		l.pos++
	}
}

// lexString reads a quoted string. Both quote styles are accepted because both
// are natural to type, and a shell already ate one of them half the time.
func (l *lexer) lexString(quote byte) error {
	start := l.pos
	l.pos++ // opening quote

	var b strings.Builder
	for l.pos < len(l.input) {
		c := l.input[l.pos]
		switch {
		case c == '\\' && l.pos+1 < len(l.input):
			l.pos++
			b.WriteByte(l.input[l.pos])
			l.pos++
		case c == quote:
			l.pos++
			l.emit(token{kind: tokenString, text: b.String(), start: start, end: l.pos})
			return nil
		default:
			b.WriteByte(c)
			l.pos++
		}
	}

	return &Error{
		Message:  "unterminated string",
		Position: start,
		Tail:     l.input[start:],
	}
}

// lexNumber reads an integer or float, allowing "_" as a digit separator so
// large offsets stay readable: 3_460_112.
func (l *lexer) lexNumber() {
	start := l.pos
	if l.input[l.pos] == '-' {
		l.pos++
	}
	for l.pos < len(l.input) {
		c := l.input[l.pos]
		if !isDigit(c) && c != '_' && c != '.' {
			break
		}
		l.pos++
	}
	text := strings.ReplaceAll(l.input[start:l.pos], "_", "")
	l.emit(token{kind: tokenNumber, text: text, start: start, end: l.pos})
}

func (l *lexer) lexOperator() {
	start := l.pos
	for l.pos < len(l.input) && strings.IndexByte(operatorChars, l.input[l.pos]) >= 0 {
		l.pos++
	}
	l.emit(token{kind: tokenOperator, text: l.input[start:l.pos], start: start, end: l.pos})
}

// lexIdent reads a name, which may carry a JSON path: value.items[0].sku.
func (l *lexer) lexIdent() {
	start := l.pos
	for l.pos < len(l.input) {
		c := l.input[l.pos]
		if !isIdentChar(c) {
			break
		}
		l.pos++
	}
	l.emit(token{kind: tokenIdent, text: l.input[start:l.pos], start: start, end: l.pos})
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func isIdentStart(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

// isIdentChar allows the characters of a JSON path, so `value.a[0].b` lexes as
// one token and the parser can split it.
func isIdentChar(c byte) bool {
	return isIdentStart(c) || isDigit(c) || c == '.' || c == '[' || c == ']' || c == '-'
}
