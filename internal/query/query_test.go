package query

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// testRecord is a Record for tests.
type testRecord struct {
	topic     string
	partition int32
	offset    int64
	key       string
	value     string
	timestamp time.Time
	headers   map[string]string
}

func (r testRecord) Topic() string        { return r.topic }
func (r testRecord) Partition() int32     { return r.partition }
func (r testRecord) Offset() int64        { return r.offset }
func (r testRecord) Key() []byte          { return []byte(r.key) }
func (r testRecord) Value() []byte        { return []byte(r.value) }
func (r testRecord) Timestamp() time.Time { return r.timestamp }
func (r testRecord) Size() int            { return len(r.key) + len(r.value) }

func (r testRecord) Header(name string) (string, bool) {
	value, ok := r.headers[name]
	return value, ok
}

func sample() testRecord {
	return testRecord{
		topic:     "orders.events",
		partition: 3,
		offset:    418204,
		key:       "ord-99f",
		value:     `{"id":"ord-99f","status":"FAILED","amount":1499.5,"items":[{"sku":"A-1002","qty":2}]}`,
		timestamp: time.Date(2026, 8, 4, 12, 6, 1, 0, time.UTC),
		headers:   map[string]string{"trace-id": "4b1ec90", "source": "checkout-api"},
	}
}

// ---------------------------------------------------------------- parsing ---

func TestParseEmptyMatchesEverything(t *testing.T) {
	q, err := Parse("")
	if err != nil {
		t.Fatalf("an empty query should be valid: %v", err)
	}
	if q.HasFilter() {
		t.Error("an empty query should have no filter")
	}
	if !q.Match(sample()) {
		t.Error("an empty query should match every record")
	}
}

// Clause order must not matter: people type these interactively.
func TestClausesInAnyOrder(t *testing.T) {
	inputs := []string{
		`from beginning limit 10 key == "ord-99f"`,
		`key == "ord-99f" limit 10 from beginning`,
		`limit 10 key == "ord-99f" from beginning`,
		`from beginning key == "ord-99f" limit 10`,
	}

	var first string
	for i, input := range inputs {
		q, err := Parse(input)
		if err != nil {
			t.Fatalf("Parse(%q): %v", input, err)
		}
		if q.Limit != 10 {
			t.Errorf("Parse(%q): limit = %d, want 10", input, q.Limit)
		}
		if q.From.Kind != FromBeginning {
			t.Errorf("Parse(%q): from = %v, want beginning", input, q.From.Kind)
		}
		if !q.Match(sample()) {
			t.Errorf("Parse(%q) should match the sample record", input)
		}

		if i == 0 {
			first = q.Expr.String()
			continue
		}
		if got := q.Expr.String(); got != first {
			t.Errorf("Parse(%q) produced %q, want the same filter as the first form %q",
				input, got, first)
		}
	}
}

func TestParseVariablesAndAliases(t *testing.T) {
	pairs := [][2]string{
		{"topic", "t"},
		{"partition", "p"},
		{"offset", "o"},
		{"key", "k"},
		{"value", "v"},
		{"size", "si"},
	}

	for _, pair := range pairs {
		full, alias := pair[0], pair[1]
		t.Run(full, func(t *testing.T) {
			a, err := Parse(full + ` == "x"`)
			if err != nil {
				t.Fatalf("Parse(%s): %v", full, err)
			}
			b, err := Parse(alias + ` == "x"`)
			if err != nil {
				t.Fatalf("Parse(%s): %v", alias, err)
			}
			if a.Expr.String() != b.Expr.String() {
				t.Errorf("%q and %q should mean the same, got %q and %q",
					full, alias, a.Expr.String(), b.Expr.String())
			}
		})
	}
}

func TestParseOperators(t *testing.T) {
	cases := map[string]Operator{
		`key == "a"`:          OpEqual,
		`key = "a"`:           OpEqual,
		`key != "a"`:          OpNotEqual,
		`offset > 1`:          OpGreater,
		`offset >= 1`:         OpGreaterEqual,
		`offset < 1`:          OpLess,
		`offset <= 1`:         OpLessEqual,
		`key contains "a"`:    OpContains,
		`key ~= "a"`:          OpContains,
		`key =~ "a"`:          OpContains,
		`key includes "a"`:    OpContains,
		`key starts with "a"`: OpStartsWith,
	}

	for input, want := range cases {
		t.Run(input, func(t *testing.T) {
			q, err := Parse(input)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			comparison, ok := q.Expr.(*Comparison)
			if !ok {
				t.Fatalf("expression is %T, want a comparison", q.Expr)
			}
			if comparison.Op != want {
				t.Errorf("operator = %v, want %v", comparison.Op, want)
			}
		})
	}
}

func TestParseFromClause(t *testing.T) {
	cases := []struct {
		input string
		want  FromKind
		n     int64
	}{
		{"from beginning", FromBeginning, 0},
		{"from begin", FromBeginning, 0},
		{"from end", FromEnd, 0},
		{"from now", FromEnd, 0},
		{"from end - 500", FromLastN, 500},
		{"from end - 5_000", FromLastN, 5000},
		{"from 34895", FromOffset, 0},
	}

	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			q, err := Parse(tc.input)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if q.From.Kind != tc.want {
				t.Errorf("kind = %v, want %v", q.From.Kind, tc.want)
			}
			if tc.n > 0 && q.From.N != tc.n {
				t.Errorf("n = %d, want %d", q.From.N, tc.n)
			}
		})
	}
}

func TestParseOrderBy(t *testing.T) {
	q, err := Parse("order by offset desc")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if q.Order == nil {
		t.Fatal("order clause missing")
	}
	if q.Order.Var.Name != VarOffset || !q.Order.Descending {
		t.Errorf("order = %+v, want offset descending", q.Order)
	}

	// "sort by" is accepted as a synonym, and ascending is the default.
	q, err = Parse("sort by key")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if q.Order == nil || q.Order.Descending {
		t.Errorf("order = %+v, want key ascending", q.Order)
	}
}

func TestParseNoiseWordWhere(t *testing.T) {
	with, err := Parse(`where key == "a"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	without, err := Parse(`key == "a"`)
	if err != nil {
		t.Fatal(err)
	}
	if with.Expr.String() != without.Expr.String() {
		t.Error("the where keyword should be optional noise")
	}
}

func TestParseDigitSeparators(t *testing.T) {
	q, err := Parse("offset > 3_460_112")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	comparison := q.Expr.(*Comparison)
	if comparison.Num != 3460112 {
		t.Errorf("number = %v, want 3460112", comparison.Num)
	}
}

func TestParseAdjacentFiltersAreConjunction(t *testing.T) {
	q, err := Parse(`key == "a" partition == 1`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if _, ok := q.Expr.(*AndExpr); !ok {
		t.Errorf("two filters side by side should read as a conjunction, got %T", q.Expr)
	}
}

// ------------------------------------------------------------------ errors ---

func TestParseErrorsCarryPositionAndTail(t *testing.T) {
	cases := []struct {
		input    string
		wantTail string
		wantMsg  string
	}{
		{`key == "a" and`, "", "expected a field name"},
		{`nonsense == 1`, "nonsense == 1", "unknown field"},
		{`key ==`, "", "expected a value"},
		{`key "a"`, `"a"`, "expected a comparison operator"},
		{`(key == "a"`, "", "expected a closing parenthesis"},
		{`key == "unterminated`, `"unterminated`, "unterminated string"},
		{`limit 0`, "", "at least 1"},
		{`from nonsense`, "nonsense", "expected beginning, end"},
		{`timestamp >= "not a date"`, `"not a date"`, "cannot read"},
	}

	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			_, err := Parse(tc.input)
			if err == nil {
				t.Fatal("expected an error")
			}

			var parseErr *Error
			if !asParseError(err, &parseErr) {
				t.Fatalf("error is %T, want *query.Error", err)
			}
			if !strings.Contains(parseErr.Message, tc.wantMsg) {
				t.Errorf("message = %q, want it to mention %q", parseErr.Message, tc.wantMsg)
			}
			// The tail is what the input line underlines, so it has to point at
			// the offending text rather than the whole query.
			if tc.wantTail != "" && parseErr.Tail != tc.wantTail {
				t.Errorf("tail = %q, want %q", parseErr.Tail, tc.wantTail)
			}
			if parseErr.Position < 0 || parseErr.Position > len(tc.input) {
				t.Errorf("position %d is outside the input", parseErr.Position)
			}
		})
	}
}

func asParseError(err error, target **Error) bool {
	return errors.As(err, target)
}

func TestParseDoesNotLoopOnGarbage(t *testing.T) {
	// A parser that fails to consume must report rather than spin.
	for _, input := range []string{")", ")))", "== 5", "and and", "- -"} {
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = Parse(input)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("Parse(%q) did not terminate", input)
		}
	}
}

// -------------------------------------------------------------- evaluation ---

func TestMatchStringFields(t *testing.T) {
	record := sample()
	cases := map[string]bool{
		`topic == "orders.events"`:   true,
		`topic == "other"`:           false,
		`topic contains "orders"`:    true,
		`topic starts with "orders"`: true,
		`topic starts with "events"`: false,
		`key == "ord-99f"`:           true,
		`key != "ord-99f"`:           false,
		`key contains "99"`:          true,
		// Matching is case-insensitive for contains and starts with: a search
		// that misses because of capitalisation is a search that wasted time.
		`key contains "ORD"`: true,
	}

	for input, want := range cases {
		t.Run(input, func(t *testing.T) {
			q, err := Parse(input)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if got := q.Match(record); got != want {
				t.Errorf("Match = %v, want %v", got, want)
			}
		})
	}
}

func TestMatchNumericFields(t *testing.T) {
	record := sample()
	cases := map[string]bool{
		"partition == 3":    true,
		"partition == 4":    false,
		"partition > 2":     true,
		"partition <= 3":    true,
		"offset > 400000":   true,
		"offset < 400000":   false,
		"offset >= 418204":  true,
		"size > 10":         true,
		"partition != 3":    false,
		"offset == 418_204": true,
	}

	for input, want := range cases {
		t.Run(input, func(t *testing.T) {
			q, err := Parse(input)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if got := q.Match(record); got != want {
				t.Errorf("Match = %v, want %v", got, want)
			}
		})
	}
}

func TestMatchJSONPath(t *testing.T) {
	record := sample()
	cases := map[string]bool{
		`value.status == "FAILED"`:       true,
		`value.status == "OK"`:           false,
		`value.id == "ord-99f"`:          true,
		`value.amount > 1000`:            true,
		`value.amount < 1000`:            false,
		`value.items[0].sku == "A-1002"`: true,
		`value.items[0].qty == 2`:        true,
		`value.items[1].sku == "x"`:      false, // out of range
		`value.missing == "anything"`:    false,
		// The whole value is still searchable as text.
		`value contains "FAILED"`: true,
	}

	for input, want := range cases {
		t.Run(input, func(t *testing.T) {
			q, err := Parse(input)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if got := q.Match(record); got != want {
				t.Errorf("Match = %v, want %v", got, want)
			}
		})
	}
}

func TestMatchHeaders(t *testing.T) {
	record := sample()
	cases := map[string]bool{
		`headers.trace-id == "4b1ec90"`: true,
		`h.trace-id == "4b1ec90"`:       true,
		`headers.source contains "api"`: true,
		`headers.missing == "x"`:        false,
	}

	for input, want := range cases {
		t.Run(input, func(t *testing.T) {
			q, err := Parse(input)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if got := q.Match(record); got != want {
				t.Errorf("Match = %v, want %v", got, want)
			}
		})
	}
}

func TestMatchBooleanLogic(t *testing.T) {
	record := sample()
	cases := map[string]bool{
		`partition == 3 and key == "ord-99f"`:                       true,
		`partition == 3 and key == "other"`:                         false,
		`partition == 9 or key == "ord-99f"`:                        true,
		`partition == 9 or key == "other"`:                          false,
		`partition == 3 && offset > 1`:                              true,
		`partition == 9 || partition == 3`:                          true,
		`!(partition == 9)`:                                         true,
		`!(partition == 3)`:                                         false,
		`(partition == 3 or partition == 4) and key contains "ord"`: true,
		// Precedence: and binds tighter than or.
		`partition == 9 and key == "x" or topic == "orders.events"`: true,
	}

	for input, want := range cases {
		t.Run(input, func(t *testing.T) {
			q, err := Parse(input)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if got := q.Match(record); got != want {
				t.Errorf("Match = %v, want %v", got, want)
			}
		})
	}
}

func TestMatchTimestamps(t *testing.T) {
	record := sample() // 2026-08-04T12:06:01Z

	cases := map[string]bool{
		`timestamp >= "2026-08-04T00:00:00Z"`:                                 true,
		`timestamp < "2026-08-04T00:00:00Z"`:                                  false,
		`timestamp > "2026-08-05T00:00:00Z"`:                                  false,
		`timestamp between "2026-08-04T00:00:00Z" and "2026-08-05T00:00:00Z"`: true,
		`timestamp between "2026-08-01T00:00:00Z" and "2026-08-02T00:00:00Z"`: false,
	}

	for input, want := range cases {
		t.Run(input, func(t *testing.T) {
			q, err := Parse(input)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if got := q.Match(record); got != want {
				t.Errorf("Match = %v, want %v", got, want)
			}
		})
	}
}

func TestRelativeTimes(t *testing.T) {
	// Pin the clock so the relative forms are testable.
	fixed := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	original := now
	now = func() time.Time { return fixed }
	t.Cleanup(func() { now = original })

	cases := map[string]time.Time{
		"2 hours ago":    fixed.Add(-2 * time.Hour),
		"30 minutes ago": fixed.Add(-30 * time.Minute),
		"30m ago":        fixed.Add(-30 * time.Minute),
		"1 day ago":      fixed.Add(-24 * time.Hour),
		"1d":             fixed.Add(-24 * time.Hour),
		"45s ago":        fixed.Add(-45 * time.Second),
		"1 week ago":     fixed.Add(-7 * 24 * time.Hour),
		"now":            fixed,
	}

	for input, want := range cases {
		t.Run(input, func(t *testing.T) {
			got, err := parseTime(input)
			if err != nil {
				t.Fatalf("parseTime(%q): %v", input, err)
			}
			if !got.Equal(want) {
				t.Errorf("parseTime(%q) = %v, want %v", input, got, want)
			}
		})
	}
}

func TestRelativeTimeInAQuery(t *testing.T) {
	fixed := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	original := now
	now = func() time.Time { return fixed }
	t.Cleanup(func() { now = original })

	q, err := Parse(`timestamp >= "1 hour ago"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	recent := testRecord{timestamp: fixed.Add(-30 * time.Minute)}
	old := testRecord{timestamp: fixed.Add(-2 * time.Hour)}

	if !q.Match(recent) {
		t.Error("a record from 30 minutes ago should match \"1 hour ago\"")
	}
	if q.Match(old) {
		t.Error("a record from 2 hours ago should not match \"1 hour ago\"")
	}
}

func TestAbsoluteTimeFormats(t *testing.T) {
	for _, input := range []string{
		"2026-08-04T12:00:00Z",
		"2026-08-04T12:00:00",
		"2026-08-04 12:00:00",
		"2026-08-04 12:00",
		"2026-08-04",
	} {
		if _, err := parseTime(input); err != nil {
			t.Errorf("parseTime(%q): %v", input, err)
		}
	}
}

// ------------------------------------------------------- documented examples ---

// The examples printed on the help screen must actually parse, or the
// documentation is a lie.
func TestDocumentedExamplesParse(t *testing.T) {
	examples := []string{
		`from end - 500 key contains "order-" limit 100`,
		`partition == 2 and value.status == "FAILED"`,
		`timestamp >= "2 hours ago" order by offset desc`,
		`(key == "ABC") || (key contains "XYZ") from beginning`,
		`size > 1_000_000 limit 20`,
		`from begin partition == 2 and key contains "foo" limit 20`,
		`value.hello == "world" order by key desc`,
		`headers.trace-id == "abc123"`,
		`v contains "rust" and p == 2 from beginning limit 1000`,
	}

	for _, input := range examples {
		t.Run(input, func(t *testing.T) {
			if _, err := Parse(input); err != nil {
				t.Errorf("a documented example failed to parse: %v", err)
			}
		})
	}
}

func TestStringRoundTrip(t *testing.T) {
	// A query rendered back out must parse to the same thing, which is what
	// makes the history safe to replay.
	inputs := []string{
		`key == "a" and partition == 1`,
		`from end - 500 limit 10`,
		`value.status == "FAILED" order by offset desc`,
	}

	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			first, err := Parse(input)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			second, err := Parse(first.String())
			if err != nil {
				t.Fatalf("re-parsing %q: %v", first.String(), err)
			}
			if first.String() != second.String() {
				t.Errorf("round trip changed the query:\n%q\n%q", first.String(), second.String())
			}
		})
	}
}

// The help screen's examples are the language's only documentation, so they
// must parse. This is the test that keeps the two honest.
func TestHelpScreenExamplesParse(t *testing.T) {
	examples := []string{
		`key contains "order-" from end - 500 limit 100`,
		`partition == 2 and value.status == "FAILED"`,
		`timestamp >= "2 hours ago" order by offset desc`,
		`(key == "ABC") || (key contains "XYZ") from beginning`,
		`size > 1_000_000 limit 20`,
	}
	for _, input := range examples {
		if _, err := Parse(input); err != nil {
			t.Errorf("help example %q does not parse: %v", input, err)
		}
	}
}
