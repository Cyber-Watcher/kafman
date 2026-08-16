package panel

import (
	"strings"
	"testing"

	"github.com/charmbracelet/lipgloss"
)

func TestTruncate(t *testing.T) {
	tests := []struct {
		name  string
		value string
		width int
		want  string
	}{
		{name: "a value that fits is left alone", value: "orders", width: 10, want: "orders"},
		{name: "exactly the width is left alone", value: "orders", width: 6, want: "orders"},
		{name: "a longer value is cut with an ellipsis", value: "orders.events", width: 8, want: "orders.…"},
		{name: "width one leaves only the mark", value: "orders", width: 1, want: "…"},
		{name: "zero width is empty", value: "orders", width: 0, want: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := Truncate(test.value, test.width)
			if got != test.want {
				t.Errorf("Truncate(%q, %d) = %q, want %q", test.value, test.width, got, test.want)
			}
			if lipgloss.Width(got) > test.width {
				t.Errorf("the result is %d cells wide, more than %d", lipgloss.Width(got), test.width)
			}
		})
	}
}

// A plain string must come back plain. Truncation used to append a reset
// unconditionally, which ended the style of the row the value was embedded in
// and left the escape visible in the middle of the line.
func TestTruncateDoesNotInventEscapes(t *testing.T) {
	got := Truncate("kafman-ui-group-125447", 13)
	if strings.Contains(got, "\x1b") {
		t.Errorf("a plain value should stay plain, got %q", got)
	}
}

// A style opened inside the part that was cut is closed, so it cannot bleed into
// what follows.
func TestTruncateClosesAStyleItCut(t *testing.T) {
	// Written out rather than built with lipgloss: colour profiles are detected
	// from the terminal, and under `go test` there is none, so Render would hand
	// back a plain string.
	styled := "\x1b[31morders.events\x1b[0m"

	got := Truncate(styled, 8)
	if !strings.HasSuffix(got, "\x1b[0m…") {
		t.Errorf("a cut style should be closed, got %q", got)
	}
	if lipgloss.Width(got) > 8 {
		t.Errorf("the result is %d cells wide, more than 8", lipgloss.Width(got))
	}
}

func TestTruncateLeft(t *testing.T) {
	// Topic names are distinguished by their tail, so the head is what goes.
	got := TruncateLeft("prod.orders.events", 10)
	if got != "…ers.events" && lipgloss.Width(got) > 10 {
		t.Errorf("TruncateLeft = %q, want the tail kept within 10 cells", got)
	}
	if !strings.HasPrefix(got, "…") {
		t.Errorf("TruncateLeft = %q, want it to mark the cut at the front", got)
	}
	if !strings.HasSuffix(got, "events") {
		t.Errorf("TruncateLeft = %q, want the informative tail kept", got)
	}
}
