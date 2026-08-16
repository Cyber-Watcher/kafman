package query

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Record is what a query is evaluated against. It is an interface so the query
// package does not depend on the Kafka client, and so tests can supply a record
// without one.
type Record interface {
	Topic() string
	Partition() int32
	Offset() int64
	Key() []byte
	Value() []byte
	Timestamp() time.Time
	Size() int

	// Header returns a header value by name.
	Header(name string) (string, bool)
}

// Match evaluates a comparison against a record.
func (c *Comparison) Match(r Record) bool {
	// Timestamps are their own thing: comparing them as strings would order
	// "2026-01-02" before "2026-1-3", which is not what anyone means.
	if c.Var.Name == VarTimestamp {
		return c.matchTime(r.Timestamp())
	}

	if number, ok := c.numericValue(r); ok {
		return c.matchNumber(number)
	}
	return c.matchString(c.stringValue(r))
}

func (c *Comparison) matchTime(at time.Time) bool {
	if !c.IsTime {
		// The literal was not a time, so nothing sensible can be compared.
		return false
	}
	switch c.Op {
	case OpEqual:
		return at.Equal(c.Time)
	case OpNotEqual:
		return !at.Equal(c.Time)
	case OpGreater:
		return at.After(c.Time)
	case OpGreaterEqual:
		return !at.Before(c.Time)
	case OpLess:
		return at.Before(c.Time)
	case OpLessEqual:
		return !at.After(c.Time)
	}
	return false
}

// numericValue resolves the variable to a number when it is one, either because
// the field is numeric or because a JSON path landed on a number.
func (c *Comparison) numericValue(r Record) (float64, bool) {
	if !c.IsNum {
		return 0, false
	}

	switch c.Var.Name {
	case VarPartition:
		return float64(r.Partition()), true
	case VarOffset:
		return float64(r.Offset()), true
	case VarSize:
		return float64(r.Size()), true
	}

	if c.Var.Name == VarValue && len(c.Var.Path) > 0 {
		if value, ok := lookupJSON(r.Value(), c.Var.Path); ok {
			if number, ok := toNumber(value); ok {
				return number, true
			}
		}
	}
	return 0, false
}

func (c *Comparison) matchNumber(value float64) bool {
	switch c.Op {
	case OpEqual:
		return value == c.Num
	case OpNotEqual:
		return value != c.Num
	case OpGreater:
		return value > c.Num
	case OpGreaterEqual:
		return value >= c.Num
	case OpLess:
		return value < c.Num
	case OpLessEqual:
		return value <= c.Num
	case OpContains, OpStartsWith:
		// Falls back to comparing the rendered number, which is what someone
		// writing `offset contains "42"` means.
		return c.matchString(formatNumber(value))
	}
	return false
}

// stringValue resolves the variable to text.
func (c *Comparison) stringValue(r Record) string {
	switch c.Var.Name {
	case VarTopic:
		return r.Topic()
	case VarKey:
		return string(r.Key())
	case VarPartition:
		return strconv.FormatInt(int64(r.Partition()), 10)
	case VarOffset:
		return strconv.FormatInt(r.Offset(), 10)
	case VarSize:
		return strconv.Itoa(r.Size())
	case VarHeader:
		if len(c.Var.Path) == 0 {
			return ""
		}
		value, _ := r.Header(c.Var.Path[0])
		return value

	case VarValue:
		if len(c.Var.Path) == 0 {
			return string(r.Value())
		}
		if value, ok := lookupJSON(r.Value(), c.Var.Path); ok {
			return renderJSON(value)
		}
		return ""
	}
	return ""
}

func (c *Comparison) matchString(value string) bool {
	switch c.Op {
	case OpEqual:
		return value == c.Str
	case OpNotEqual:
		return value != c.Str
	case OpContains:
		return strings.Contains(strings.ToLower(value), strings.ToLower(c.Str))
	case OpStartsWith:
		return strings.HasPrefix(strings.ToLower(value), strings.ToLower(c.Str))
	case OpGreater:
		return value > c.Str
	case OpGreaterEqual:
		return value >= c.Str
	case OpLess:
		return value < c.Str
	case OpLessEqual:
		return value <= c.Str
	}
	return false
}

// Match evaluates a between clause.
func (e *BetweenExpr) Match(r Record) bool {
	if e.Var.Name != VarTimestamp {
		return false
	}
	at := r.Timestamp()
	return !at.Before(e.Low) && !at.After(e.High)
}

// ------------------------------------------------------------- JSON paths ---

// lookupJSON walks a path into a JSON payload. It parses on every call, which
// is acceptable because a filter only runs on records that reached the client
// and the payloads involved are small.
func lookupJSON(data []byte, path []string) (any, bool) {
	if len(data) == 0 {
		return nil, false
	}

	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, false
	}

	for _, segment := range path {
		switch container := value.(type) {
		case map[string]any:
			next, ok := container[segment]
			if !ok {
				return nil, false
			}
			value = next

		case []any:
			index, err := strconv.Atoi(segment)
			if err != nil || index < 0 || index >= len(container) {
				return nil, false
			}
			value = container[index]

		default:
			return nil, false
		}
	}
	return value, true
}

func toNumber(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case string:
		number, err := strconv.ParseFloat(typed, 64)
		return number, err == nil
	case bool:
		if typed {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

// renderJSON turns a resolved path value back into text for string comparison.
// Strings are used as-is rather than re-quoted: `value.status == "FAILED"`
// should match the string FAILED, not "\"FAILED\"".
func renderJSON(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case float64:
		return formatNumber(typed)
	case bool:
		return strconv.FormatBool(typed)
	case nil:
		return "null"
	}

	encoded, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(encoded)
}

// ------------------------------------------------------------------- time ---

// timeLayouts accepted for an absolute timestamp, most precise first.
var timeLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
	"2006-01-02 15:04",
	"2006-01-02",
	"15:04:05",
	"15:04",
}

// relativePattern matches "2 hours ago", "30m ago", "1 day ago".
var relativePattern = regexp.MustCompile(
	`^(\d+)\s*(s|sec|secs|second|seconds|m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days|w|week|weeks)\s*(ago)?$`)

// now is overridable so tests can pin relative times.
var now = time.Now

// ParseTime exposes the language's time syntax to screens that ask for a
// timestamp outside a query — the offset-reset form, for one. Someone who has
// learned "2 hours ago" in the query line should not meet a different syntax two
// screens away.
func ParseTime(text string) (time.Time, error) { return parseTime(text) }

// parseTime accepts an absolute timestamp or a relative one such as
// "2 hours ago". The relative form is what makes the language usable
// interactively: nobody wants to work out an RFC3339 string in their head.
func parseTime(text string) (time.Time, error) {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return time.Time{}, fmt.Errorf("empty time")
	}

	lower := strings.ToLower(trimmed)
	switch lower {
	case "now":
		return now(), nil
	case "today":
		current := now()
		return time.Date(current.Year(), current.Month(), current.Day(), 0, 0, 0, 0, current.Location()), nil
	case "yesterday":
		current := now().AddDate(0, 0, -1)
		return time.Date(current.Year(), current.Month(), current.Day(), 0, 0, 0, 0, current.Location()), nil
	}

	if match := relativePattern.FindStringSubmatch(lower); match != nil {
		amount, err := strconv.ParseInt(match[1], 10, 64)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid amount in %q", text)
		}
		return now().Add(-time.Duration(amount) * unitDuration(match[2])), nil
	}

	for _, layout := range timeLayouts {
		if at, err := time.ParseInLocation(layout, trimmed, time.Local); err == nil {
			// A bare clock time means today.
			if layout == "15:04:05" || layout == "15:04" {
				current := now()
				return time.Date(current.Year(), current.Month(), current.Day(),
					at.Hour(), at.Minute(), at.Second(), 0, time.Local), nil
			}
			return at, nil
		}
	}

	return time.Time{}, fmt.Errorf(
		"cannot read %q as a time; try 2026-08-04T12:00:00, 2026-08-04, 12:30 or \"2 hours ago\"", text)
}

func unitDuration(unit string) time.Duration {
	switch {
	case strings.HasPrefix(unit, "s"):
		return time.Second
	case strings.HasPrefix(unit, "m") && unit != "mo":
		return time.Minute
	case strings.HasPrefix(unit, "h"):
		return time.Hour
	case strings.HasPrefix(unit, "d"):
		return 24 * time.Hour
	case strings.HasPrefix(unit, "w"):
		return 7 * 24 * time.Hour
	}
	return time.Second
}
