package messages

import (
	"strings"
	"testing"

	"github.com/charmbracelet/x/ansi"

	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

func openDetails(t *testing.T, m *Model) {
	t.Helper()
	m.Update(key("enter"))
	if m.details == nil {
		t.Fatal("details overlay did not open")
	}
}

// Tab hands the arrow keys to the value: they scroll the record instead of
// stepping through the list.
func TestDetailsTabFocusesTheValue(t *testing.T) {
	m, _ := withRecords(t)
	openDetails(t, m)

	before := m.records.Selected().record.Offset
	m.Update(key("tab"))
	if !m.details.Focused() {
		t.Fatal("tab should focus the value")
	}

	m.Update(key("down"))
	m.Update(key("down"))
	if m.details.offset != 2 {
		t.Errorf("the value should have scrolled 2 lines, got %d", m.details.offset)
	}
	// In list mode up would step to the previous record; focused, it scrolls.
	m.Update(key("up"))
	if m.details.offset != 1 {
		t.Errorf("up should scroll back, got offset %d", m.details.offset)
	}
	if got := m.records.Selected().record.Offset; got != before {
		t.Errorf("the selection moved while the value was focused: %d -> %d", before, got)
	}
}

// Esc undoes the innermost thing: first the value focus, then the overlay.
func TestDetailsEscBlursBeforeClosing(t *testing.T) {
	m, _ := withRecords(t)
	openDetails(t, m)

	m.Update(key("tab"))
	m.Update(key("esc"))
	if m.details == nil {
		t.Fatal("the first esc should only blur the value, not close the overlay")
	}
	if m.details.Focused() {
		t.Error("the value should be blurred")
	}

	m.Update(key("esc"))
	if m.details != nil {
		t.Error("the second esc should close the overlay")
	}
}

// Without the value focus the arrows keep stepping through records, exactly as
// before.
func TestDetailsArrowsStillPageRecords(t *testing.T) {
	m, _ := withRecords(t)
	openDetails(t, m)

	// The list starts in follow mode, on the newest record: up is the
	// direction that has somewhere to go.
	before := m.records.Selected().record.Offset
	m.Update(key("up"))
	if got := m.records.Selected().record.Offset; got == before {
		t.Error("up should move to the previous record in list mode")
	}
	if m.details.offset != 0 {
		t.Errorf("stepping records must not scroll the value, got offset %d", m.details.offset)
	}
}

// The JSON colouring must never change the text itself, only wrap it in
// styles, and a key must be styled differently from a string value.
func TestColourJSONLineKeepsTheText(t *testing.T) {
	th := theme.New(theme.Dark, false)

	for _, line := range []string{
		`  "status": "FAILED",`,
		`  "count": 42.5e-1,`,
		`  "flags": [true, false, null],`,
		`{`,
		`tail of a wrapped string without quotes`,
	} {
		coloured := colourJSONLine(line, th)
		if got := ansi.Strip(coloured); got != line {
			t.Errorf("the text changed:\n want %q\n got  %q", line, got)
		}
	}

	// The key and the value are both strings, but must not look the same.
	keyed := colourJSONLine(`"id": "abc"`, th)
	if !strings.Contains(keyed, th.Label.Render(`"id"`)) {
		t.Error("a key should use the label style")
	}
	if !strings.Contains(keyed, th.Success.Render(`"abc"`)) {
		t.Error("a string value should use the success style")
	}
}
