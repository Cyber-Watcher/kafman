package query

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Parse turns a query string into a Query.
//
// An empty string is a valid query that matches everything, because that is
// what an empty input line should mean.
func Parse(input string) (*Query, error) {
	tokens, err := lex(input)
	if err != nil {
		return nil, err
	}

	p := &parser{tokens: tokens, input: input}
	query, err := p.parse()
	if err != nil {
		return nil, err
	}
	query.Source = strings.TrimSpace(input)
	return query, nil
}

type parser struct {
	tokens []token
	input  string
	pos    int
}

func (p *parser) parse() (*Query, error) {
	query := &Query{From: From{Kind: FromEnd}}

	// Clauses in any order. Each iteration takes whichever clause comes next,
	// and anything that is not a clause keyword is part of the filter.
	for !p.atEOF() {
		before := p.pos

		switch {
		case p.acceptKeyword("from"):
			from, err := p.parseFrom()
			if err != nil {
				return nil, err
			}
			query.From = from

		case p.acceptKeyword("limit"):
			limit, err := p.parseLimit()
			if err != nil {
				return nil, err
			}
			query.Limit = limit

		case p.acceptKeyword("order", "by"), p.acceptKeyword("sort", "by"):
			order, err := p.parseOrder()
			if err != nil {
				return nil, err
			}
			query.Order = order

		case p.acceptKeyword("where"):
			// Noise word: accepted because it reads naturally, ignored otherwise.

		default:
			expr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			if query.Expr == nil {
				query.Expr = expr
			} else {
				// Two filters side by side, as in `key == "a" partition == 1`,
				// read as a conjunction.
				query.Expr = &AndExpr{Left: query.Expr, Right: expr}
			}
		}

		if p.pos == before {
			// Nothing consumed: without this the loop would spin forever on an
			// input the parser cannot advance past.
			return nil, p.errorHere("unexpected input")
		}
	}

	return query, nil
}

// ---------------------------------------------------------------- clauses ---

func (p *parser) parseFrom() (From, error) {
	switch {
	case p.acceptKeyword("beginning"), p.acceptKeyword("begin"), p.acceptKeyword("start"):
		return From{Kind: FromBeginning}, nil

	case p.acceptKeyword("end"), p.acceptKeyword("now"):
		// `from end - 500` is the last 500 records of each partition.
		if p.acceptOperator("-") {
			n, err := p.expectNumber()
			if err != nil {
				return From{}, err
			}
			return From{Kind: FromLastN, N: int64(n)}, nil
		}
		// `from end -500`, where the minus stuck to the number.
		if current := p.peek(); current.kind == tokenNumber && strings.HasPrefix(current.text, "-") {
			p.next()
			n, err := strconv.ParseInt(current.text, 10, 64)
			if err != nil {
				return From{}, p.errorAt(current, "invalid count")
			}
			return From{Kind: FromLastN, N: -n}, nil
		}
		return From{Kind: FromEnd}, nil
	}

	current := p.peek()
	switch current.kind {
	case tokenNumber:
		p.next()
		offset, err := strconv.ParseInt(current.text, 10, 64)
		if err != nil {
			return From{}, p.errorAt(current, "invalid offset")
		}
		return From{Kind: FromOffset, Offset: offset}, nil

	case tokenString:
		p.next()
		at, err := parseTime(current.text)
		if err != nil {
			return From{}, p.errorAt(current, err.Error())
		}
		return From{Kind: FromTime, Time: at}, nil
	}

	return From{}, p.errorHere("expected beginning, end, an offset or a time after from")
}

func (p *parser) parseLimit() (int, error) {
	n, err := p.expectNumber()
	if err != nil {
		return 0, err
	}
	if n < 1 {
		return 0, p.errorHere("limit must be at least 1")
	}
	return int(n), nil
}

func (p *parser) parseOrder() (*Order, error) {
	current := p.peek()
	if current.kind != tokenIdent {
		return nil, p.errorHere("expected a field name after order by")
	}
	p.next()

	variable, err := parseVariable(current.text)
	if err != nil {
		return nil, p.errorAt(current, err.Error())
	}

	order := &Order{Var: variable}
	switch {
	case p.acceptKeyword("desc"), p.acceptKeyword("descending"):
		order.Descending = true
	case p.acceptKeyword("asc"), p.acceptKeyword("ascending"):
		order.Descending = false
	}
	return order, nil
}

// ------------------------------------------------------------ expressions ---

// parseExpr handles `or`, the loosest binding.
func (p *parser) parseExpr() (Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.acceptKeyword("or") || p.acceptOperator("||") {
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &OrExpr{Left: left, Right: right}
	}
	return left, nil
}

func (p *parser) parseAnd() (Expr, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	for p.acceptKeyword("and") || p.acceptOperator("&&") {
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left = &AndExpr{Left: left, Right: right}
	}
	return left, nil
}

func (p *parser) parseUnary() (Expr, error) {
	if p.acceptOperator("!") || p.acceptKeyword("not") {
		inner, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &NotExpr{Inner: inner}, nil
	}
	return p.parseAtom()
}

func (p *parser) parseAtom() (Expr, error) {
	if p.accept(tokenLParen) {
		inner, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if !p.accept(tokenRParen) {
			return nil, p.errorHere("expected a closing parenthesis")
		}
		return &GroupExpr{Inner: inner}, nil
	}

	current := p.peek()
	if current.kind != tokenIdent {
		return nil, p.errorHere(fmt.Sprintf("expected a field name, found %s", current.kind))
	}
	p.next()

	variable, err := parseVariable(current.text)
	if err != nil {
		return nil, p.errorAt(current, err.Error())
	}

	if p.acceptKeyword("between") {
		return p.parseBetween(variable)
	}
	return p.parseComparison(variable)
}

func (p *parser) parseBetween(variable Variable) (Expr, error) {
	low, err := p.expectTime()
	if err != nil {
		return nil, err
	}
	if !p.acceptKeyword("and") {
		return nil, p.errorHere("expected and between the two bounds")
	}
	high, err := p.expectTime()
	if err != nil {
		return nil, err
	}
	if high.Before(low) {
		low, high = high, low
	}
	return &BetweenExpr{Var: variable, Low: low, High: high}, nil
}

func (p *parser) parseComparison(variable Variable) (Expr, error) {
	op, err := p.parseOperator()
	if err != nil {
		return nil, err
	}

	current := p.peek()
	switch current.kind {
	case tokenNumber:
		p.next()
		value, err := strconv.ParseFloat(current.text, 64)
		if err != nil {
			return nil, p.errorAt(current, "invalid number")
		}
		return &Comparison{Var: variable, Op: op, Num: value, IsNum: true}, nil

	case tokenString:
		p.next()
		comparison := &Comparison{Var: variable, Op: op, Str: current.text}
		// A timestamp comparison takes a string, which may be a fuzzy date.
		if variable.Name == VarTimestamp {
			at, err := parseTime(current.text)
			if err != nil {
				return nil, p.errorAt(current, err.Error())
			}
			comparison.Time, comparison.IsTime = at, true
		}
		return comparison, nil

	case tokenIdent:
		// An unquoted word: accepted, because quoting every value gets tiring
		// and the intent is unambiguous.
		p.next()
		return &Comparison{Var: variable, Op: op, Str: current.text}, nil
	}

	return nil, p.errorHere("expected a value after " + op.String())
}

func (p *parser) parseOperator() (Operator, error) {
	// Word operators first: they are two tokens in the case of "starts with".
	switch {
	case p.acceptKeyword("contains"), p.acceptKeyword("contain"),
		p.acceptKeyword("includes"), p.acceptKeyword("include"):
		return OpContains, nil
	case p.acceptKeyword("starts", "with"), p.acceptKeyword("start", "with"):
		return OpStartsWith, nil
	}

	current := p.peek()
	if current.kind != tokenOperator {
		return 0, p.errorHere("expected a comparison operator")
	}
	p.next()

	switch current.text {
	case "==", "=":
		return OpEqual, nil
	case "!=", "<>":
		return OpNotEqual, nil
	case ">":
		return OpGreater, nil
	case ">=":
		return OpGreaterEqual, nil
	case "<":
		return OpLess, nil
	case "<=":
		return OpLessEqual, nil
	case "~=", "=~":
		return OpContains, nil
	}
	return 0, p.errorAt(current, "unknown operator")
}

// ------------------------------------------------------------- primitives ---

func (p *parser) peek() token {
	if p.pos >= len(p.tokens) {
		return token{kind: tokenEOF, start: len(p.input), end: len(p.input)}
	}
	return p.tokens[p.pos]
}

func (p *parser) next() token {
	current := p.peek()
	if p.pos < len(p.tokens) {
		p.pos++
	}
	return current
}

func (p *parser) atEOF() bool { return p.peek().kind == tokenEOF }

func (p *parser) accept(kind tokenKind) bool {
	if p.peek().kind == kind {
		p.next()
		return true
	}
	return false
}

// acceptKeyword matches a sequence of identifier tokens, case-insensitively.
// It consumes nothing unless the whole sequence matches.
func (p *parser) acceptKeyword(words ...string) bool {
	saved := p.pos
	for _, word := range words {
		current := p.peek()
		if current.kind != tokenIdent || !strings.EqualFold(current.text, word) {
			p.pos = saved
			return false
		}
		p.next()
	}
	return true
}

func (p *parser) acceptOperator(text string) bool {
	current := p.peek()
	if current.kind == tokenOperator && current.text == text {
		p.next()
		return true
	}
	return false
}

func (p *parser) expectNumber() (float64, error) {
	current := p.peek()
	if current.kind != tokenNumber {
		return 0, p.errorHere("expected a number")
	}
	p.next()

	value, err := strconv.ParseFloat(current.text, 64)
	if err != nil {
		return 0, p.errorAt(current, "invalid number")
	}
	return value, nil
}

func (p *parser) expectTime() (time.Time, error) {
	current := p.peek()
	if current.kind != tokenString {
		return time.Time{}, p.errorHere("expected a quoted time")
	}
	p.next()

	at, err := parseTime(current.text)
	if err != nil {
		return time.Time{}, p.errorAt(current, err.Error())
	}
	return at, nil
}

// errorHere reports a problem at the current token, carrying the rest of the
// input so the query line can underline it.
func (p *parser) errorHere(message string) error {
	return p.errorAt(p.peek(), message)
}

func (p *parser) errorAt(at token, message string) error {
	position := min(at.start, len(p.input))
	return &Error{
		Message:  message,
		Position: position,
		Tail:     strings.TrimSpace(p.input[position:]),
	}
}

// parseVariable splits a name into a variable and its JSON path, resolving
// aliases along the way.
func parseVariable(text string) (Variable, error) {
	// A header is addressed as headers.name or h.name, and the rest of the
	// string is the header name rather than a JSON path.
	head, rest, hasRest := strings.Cut(text, ".")
	name := strings.ToLower(head)
	if canonical, ok := aliases[name]; ok {
		name = canonical
	}

	switch name {
	case VarHeader:
		if !hasRest || rest == "" {
			return Variable{}, fmt.Errorf("a header needs a name, as in headers.trace-id")
		}
		return Variable{Name: VarHeader, Path: []string{rest}}, nil

	case VarTopic, VarPartition, VarOffset, VarKey, VarTimestamp, VarSize:
		if hasRest {
			return Variable{}, fmt.Errorf("%s has no fields to address", name)
		}
		return Variable{Name: name}, nil

	case VarValue:
		if !hasRest {
			return Variable{Name: VarValue}, nil
		}
		return Variable{Name: VarValue, Path: splitPath(rest)}, nil
	}

	return Variable{}, fmt.Errorf("unknown field %q; try topic, partition, offset, key, value, timestamp, size or headers.<name>", head)
}

// splitPath turns `items[0].sku` into ["items", "0", "sku"].
func splitPath(path string) []string {
	replaced := strings.NewReplacer("[", ".", "]", "").Replace(path)

	var out []string
	for _, part := range strings.Split(replaced, ".") {
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

func trimFloat(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }
