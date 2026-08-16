package query

import (
	"strings"
	"time"
)

// Variable is a field of a record that a query can test.
type Variable struct {
	Name string

	// Path is the JSON path inside the value, for `value.items[0].sku`.
	Path []string
}

// Canonical variable names.
const (
	VarTopic     = "topic"
	VarPartition = "partition"
	VarOffset    = "offset"
	VarKey       = "key"
	VarValue     = "value"
	VarTimestamp = "timestamp"
	VarSize      = "size"
	VarHeader    = "headers"
)

// aliases maps the short forms people actually type to canonical names.
// Borrowed from yozefu, where they are the difference between a language you
// use interactively and one you tolerate.
var aliases = map[string]string{
	"t":  VarTopic,
	"p":  VarPartition,
	"o":  VarOffset,
	"k":  VarKey,
	"v":  VarValue,
	"ts": VarTimestamp,
	"si": VarSize,
	"h":  VarHeader,
}

// numericVars are compared as numbers; everything else as strings.
var numericVars = map[string]bool{
	VarPartition: true,
	VarOffset:    true,
	VarSize:      true,
}

// String renders the variable the way it would be typed.
func (v Variable) String() string {
	if len(v.Path) == 0 {
		return v.Name
	}
	return v.Name + "." + strings.Join(v.Path, ".")
}

// Numeric reports whether the variable is compared numerically.
func (v Variable) Numeric() bool {
	// A JSON path can point at a number, but which is only known at evaluation
	// time, so paths are resolved dynamically rather than classified here.
	return numericVars[v.Name] && len(v.Path) == 0
}

// Operator is a comparison.
type Operator int

// Comparison operators.
const (
	OpEqual Operator = iota
	OpNotEqual
	OpGreater
	OpGreaterEqual
	OpLess
	OpLessEqual
	OpContains
	OpStartsWith
)

// String renders the operator in its canonical spelling.
func (o Operator) String() string {
	switch o {
	case OpEqual:
		return "=="
	case OpNotEqual:
		return "!="
	case OpGreater:
		return ">"
	case OpGreaterEqual:
		return ">="
	case OpLess:
		return "<"
	case OpLessEqual:
		return "<="
	case OpContains:
		return "contains"
	case OpStartsWith:
		return "starts with"
	}
	return "?"
}

// Expr is a boolean expression over a record.
type Expr interface {
	// Match reports whether the record satisfies the expression.
	Match(r Record) bool

	// String renders the expression, used in tests and in the query history.
	String() string
}

// AndExpr is a conjunction.
type AndExpr struct{ Left, Right Expr }

func (e *AndExpr) Match(r Record) bool { return e.Left.Match(r) && e.Right.Match(r) }
func (e *AndExpr) String() string      { return e.Left.String() + " and " + e.Right.String() }

// OrExpr is a disjunction.
type OrExpr struct{ Left, Right Expr }

func (e *OrExpr) Match(r Record) bool { return e.Left.Match(r) || e.Right.Match(r) }
func (e *OrExpr) String() string      { return e.Left.String() + " or " + e.Right.String() }

// NotExpr is a negation.
type NotExpr struct{ Inner Expr }

func (e *NotExpr) Match(r Record) bool { return !e.Inner.Match(r) }
func (e *NotExpr) String() string      { return "!" + e.Inner.String() }

// GroupExpr is a parenthesised expression, kept so String round-trips.
type GroupExpr struct{ Inner Expr }

func (e *GroupExpr) Match(r Record) bool { return e.Inner.Match(r) }
func (e *GroupExpr) String() string      { return "(" + e.Inner.String() + ")" }

// Comparison tests one variable against a literal.
type Comparison struct {
	Var Variable
	Op  Operator

	// Exactly one of these is set, according to how the literal was written.
	Str    string
	Num    float64
	IsNum  bool
	Time   time.Time
	IsTime bool
}

func (c *Comparison) String() string {
	value := c.Str
	if c.IsNum {
		value = formatNumber(c.Num)
	} else {
		value = `"` + value + `"`
	}
	return c.Var.String() + " " + c.Op.String() + " " + value
}

// BetweenExpr tests that a timestamp falls inside a range.
type BetweenExpr struct {
	Var  Variable
	Low  time.Time
	High time.Time
}

func (e *BetweenExpr) String() string {
	return e.Var.String() + " between " +
		`"` + e.Low.Format(time.RFC3339) + `" and "` + e.High.Format(time.RFC3339) + `"`
}

// FromKind is where consumption starts.
type FromKind int

// Starting positions, mirroring kafka.StartKind.
const (
	FromEnd FromKind = iota
	FromBeginning
	FromLastN
	FromOffset
	FromTime
)

// From is the `from` clause.
type From struct {
	Kind   FromKind
	N      int64
	Offset int64
	Time   time.Time
}

// String renders the clause.
func (f From) String() string {
	switch f.Kind {
	case FromBeginning:
		return "from beginning"
	case FromLastN:
		return "from end - " + formatNumber(float64(f.N))
	case FromOffset:
		return "from " + formatNumber(float64(f.Offset))
	case FromTime:
		return `from "` + f.Time.Format(time.RFC3339) + `"`
	default:
		return "from end"
	}
}

// Order is the `order by` clause.
type Order struct {
	Var        Variable
	Descending bool
}

// String renders the clause.
func (o Order) String() string {
	direction := "asc"
	if o.Descending {
		direction = "desc"
	}
	return "order by " + o.Var.String() + " " + direction
}

// Query is a parsed search.
type Query struct {
	// Expr is nil when the query has no filter, which means everything matches.
	Expr Expr

	From  From
	Limit int
	Order *Order

	// Source is the text the query was parsed from.
	Source string
}

// Match reports whether a record satisfies the filter.
func (q *Query) Match(r Record) bool {
	if q == nil || q.Expr == nil {
		return true
	}
	return q.Expr.Match(r)
}

// HasFilter reports whether the query narrows anything.
func (q *Query) HasFilter() bool { return q != nil && q.Expr != nil }

// String renders the query in canonical form.
func (q *Query) String() string {
	if q == nil {
		return ""
	}

	var parts []string
	if q.Expr != nil {
		parts = append(parts, q.Expr.String())
	}
	parts = append(parts, q.From.String())
	if q.Limit > 0 {
		parts = append(parts, "limit "+formatNumber(float64(q.Limit)))
	}
	if q.Order != nil {
		parts = append(parts, q.Order.String())
	}
	return strings.Join(parts, " ")
}

func formatNumber(f float64) string {
	if f == float64(int64(f)) {
		return itoa(int64(f))
	}
	return trimFloat(f)
}
