package config

import (
	"fmt"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration that round-trips through YAML as a human-readable
// string such as "10s" or "1m30s".
//
// yaml.v3 has no built-in support for time.Duration: it would marshal it as a
// nanosecond integer, which is unreadable in a hand-edited config file.
type Duration time.Duration

// D returns the value as a time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// String implements fmt.Stringer.
func (d Duration) String() string { return time.Duration(d).String() }

// MarshalYAML writes the duration as a string.
func (d Duration) MarshalYAML() (any, error) {
	return time.Duration(d).String(), nil
}

// UnmarshalYAML accepts either a duration string ("10s") or a bare number,
// which is read as seconds — a common mistake worth tolerating.
//
// Note that yaml.v3 happily decodes the scalar 15 into a string, so the numeric
// case has to be handled after ParseDuration fails, not before.
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var s string
	if err := node.Decode(&s); err != nil {
		return fmt.Errorf("invalid duration %q: use a form like 10s, 500ms or 1m30s", node.Value)
	}

	if parsed, err := time.ParseDuration(s); err == nil {
		*d = Duration(parsed)
		return nil
	}

	if seconds, err := strconv.ParseFloat(s, 64); err == nil {
		*d = Duration(time.Duration(seconds * float64(time.Second)))
		return nil
	}

	return fmt.Errorf("invalid duration %q: use a form like 10s, 500ms or 1m30s", s)
}
