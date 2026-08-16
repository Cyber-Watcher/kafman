package components_test

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/ansi"

	"github.com/Cyber-Watcher/kafman/internal/ui/components/chrome"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/modal"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/panel"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/table"
	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

func testTheme() *theme.Theme { return theme.New(theme.Dark, false) }

func plain(s string) string { return ansi.Strip(s) }

// ------------------------------------------------------------------ panel ---

func TestPanelDimensions(t *testing.T) {
	th := testTheme()

	for _, size := range []struct{ w, h int }{{40, 6}, {100, 20}, {12, 3}} {
		p := panel.Panel{Title: "Topics", Width: size.w, Height: size.h}
		lines := strings.Split(strings.TrimRight(p.Render(th, "content"), "\n"), "\n")

		if len(lines) != size.h {
			t.Errorf("panel %dx%d rendered %d lines, want %d", size.w, size.h, len(lines), size.h)
		}
		for i, line := range lines {
			if w := ansi.StringWidth(line); w != size.w {
				t.Errorf("panel %dx%d line %d is %d cells, want %d: %q",
					size.w, size.h, i, w, size.w, plain(line))
			}
		}
	}
}

func TestPanelBorderText(t *testing.T) {
	th := testTheme()
	p := panel.Panel{Title: "Records", TopRight: "[ 132 / 8_412 ]", Width: 60, Height: 5}

	top := strings.Split(plain(p.Render(th, "")), "\n")[0]
	if !strings.Contains(top, "Records") {
		t.Errorf("the title should sit in the top border, got %q", top)
	}
	if !strings.Contains(top, "132 / 8_412") {
		t.Errorf("the counter should sit in the top border, got %q", top)
	}
}

func TestPanelDropsCounterBeforeTitle(t *testing.T) {
	th := testTheme()
	// Too narrow for both: the title is the more useful of the two.
	p := panel.Panel{Title: "Consumer groups", TopRight: "[ total 41_882 ]", Width: 24, Height: 4}

	top := strings.Split(plain(p.Render(th, "")), "\n")[0]
	if !strings.Contains(top, "Consumer groups") {
		t.Errorf("the title should survive a narrow panel, got %q", top)
	}
	if strings.Contains(top, "41_882") {
		t.Errorf("the counter should be dropped when it does not fit, got %q", top)
	}
	if w := ansi.StringWidth(top); w != 24 {
		t.Errorf("border is %d cells wide, want 24: %q", w, top)
	}
}

func TestPanelFocusChangesBorder(t *testing.T) {
	th := testTheme()

	unfocused := plain(panel.Panel{Title: "T", Width: 20, Height: 3}.Render(th, ""))
	focused := plain(panel.Panel{Title: "T", Width: 20, Height: 3, Focused: true}.Render(th, ""))

	if !strings.Contains(unfocused, "╭") {
		t.Error("an unfocused panel should use the rounded border")
	}
	if !strings.Contains(focused, "┏") {
		t.Error("a focused panel should use the thick border")
	}
}

func TestTruncate(t *testing.T) {
	cases := []struct {
		in    string
		width int
		want  string
	}{
		{"short", 10, "short"},
		{"exactly-10", 10, "exactly-10"},
		{"this is far too long", 10, "this is a…"},
	}
	for _, tc := range cases {
		got := plain(panel.Truncate(tc.in, tc.width))
		if ansi.StringWidth(got) > tc.width {
			t.Errorf("Truncate(%q, %d) = %q, wider than %d", tc.in, tc.width, got, tc.width)
		}
	}
}

func TestTruncateLeftKeepsTheTail(t *testing.T) {
	// Kafka topic names differ at the end, so the head is what to cut.
	got := plain(panel.TruncateLeft("prod.orders.events.v2", 12))

	if ansi.StringWidth(got) > 12 {
		t.Errorf("TruncateLeft returned %d cells, want at most 12: %q", ansi.StringWidth(got), got)
	}
	if !strings.HasSuffix(got, "events.v2") {
		t.Errorf("the informative tail should survive, got %q", got)
	}
	if !strings.HasPrefix(got, "…") {
		t.Errorf("truncation should be marked with an ellipsis, got %q", got)
	}
}

// ------------------------------------------------------------------ table ---

func buildTable() *table.Model {
	tbl := table.New(
		table.Column{Title: "NAME", Width: 20},
		table.Column{Title: "PARTS", Width: 6, Align: table.Right},
		table.Column{Title: "SIZE", Width: 0},
	)
	tbl.SetRows([]table.Row{
		{Cells: []string{"orders.events", "12", "38.1 GB"}},
		{Cells: []string{"payments.tx", "6", "12.4 GB"}},
		{Cells: []string{"audit.log", "24", "92.8 GB"}},
		{Cells: []string{"user.clicks", "9", "8.9 GB"}},
	})
	return tbl
}

func TestTableRendersWithinBounds(t *testing.T) {
	th := testTheme()
	tbl := buildTable()

	out := tbl.View(th, 60, 6)
	for i, line := range strings.Split(out, "\n") {
		if w := ansi.StringWidth(line); w > 60 {
			t.Errorf("line %d is %d cells wide, want at most 60: %q", i, w, plain(line))
		}
	}
}

func TestTableNavigation(t *testing.T) {
	th := testTheme()
	tbl := buildTable()
	tbl.Focus()
	tbl.View(th, 60, 6) // establishes the page size

	if got := tbl.SelectedRow().Cells[0]; got != "orders.events" {
		t.Errorf("initial selection = %q, want the first row", got)
	}

	tbl.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("j")})
	if got := tbl.SelectedRow().Cells[0]; got != "payments.tx" {
		t.Errorf("after j, selection = %q", got)
	}

	tbl.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("G")})
	if got := tbl.SelectedRow().Cells[0]; got != "user.clicks" {
		t.Errorf("after G, selection = %q, want the last row", got)
	}

	// Moving past the end must clamp, not wrap or panic.
	tbl.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("j")})
	if got := tbl.SelectedRow().Cells[0]; got != "user.clicks" {
		t.Errorf("selection should stay on the last row, got %q", got)
	}
}

func TestTableIgnoresKeysWhenBlurred(t *testing.T) {
	tbl := buildTable()
	tbl.Blur()

	tbl.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("j")})
	if tbl.Cursor() != 0 {
		t.Error("an unfocused table must not react to keys")
	}
}

func TestTableFilter(t *testing.T) {
	tbl := buildTable()

	tbl.SetFilter("user")
	if tbl.Len() != 1 {
		t.Errorf("filter matched %d rows, want 1", tbl.Len())
	}
	if got := tbl.SelectedRow().Cells[0]; got != "user.clicks" {
		t.Errorf("filtered selection = %q", got)
	}
	if tbl.TotalLen() != 4 {
		t.Errorf("TotalLen = %d, want the unfiltered count of 4", tbl.TotalLen())
	}

	tbl.SetFilter("")
	if tbl.Len() != 4 {
		t.Errorf("clearing the filter left %d rows", tbl.Len())
	}
}

func TestTableSortIsNumericWhereItShouldBe(t *testing.T) {
	th := testTheme()
	tbl := buildTable()
	tbl.Focus()
	tbl.View(th, 60, 6)

	// F3 once sorts by the first column ascending; three more presses reach the
	// numeric PARTS column.
	tbl.Update(tea.KeyMsg{Type: tea.KeyF3})
	if got := tbl.Rows()[0].Cells[0]; got != "audit.log" {
		t.Errorf("after sorting by name, first row = %q, want audit.log", got)
	}

	tbl.Update(tea.KeyMsg{Type: tea.KeyF3}) // name descending
	tbl.Update(tea.KeyMsg{Type: tea.KeyF3}) // parts ascending
	first := tbl.Rows()[0].Cells[1]
	if first != "6" {
		t.Errorf("numeric sort put %q first; string sort would pick 12", first)
	}
}

func TestTableSelectionSurvivesRefresh(t *testing.T) {
	th := testTheme()
	tbl := buildTable()
	tbl.Focus()
	tbl.View(th, 60, 6)
	tbl.MoveCursor(2)
	selected := tbl.SelectedRow().Cells[0]

	// A periodic refresh must not move the selection under the user's hands.
	tbl.SetRows([]table.Row{
		{Cells: []string{"orders.events", "12", "40.0 GB"}},
		{Cells: []string{"payments.tx", "6", "12.4 GB"}},
		{Cells: []string{"audit.log", "24", "93.0 GB"}},
		{Cells: []string{"user.clicks", "9", "8.9 GB"}},
	})

	if got := tbl.SelectedRow().Cells[0]; got != selected {
		t.Errorf("selection moved from %q to %q after a refresh", selected, got)
	}
}

func TestTableVisibleRange(t *testing.T) {
	th := testTheme()
	tbl := table.New(table.Column{Title: "N", Width: 10})

	rows := make([]table.Row, 100)
	for i := range rows {
		rows[i] = table.Row{Cells: []string{string(rune('a' + i%26))}}
	}
	tbl.SetRows(rows)
	tbl.Focus()
	tbl.View(th, 20, 11) // header plus ten rows

	start, end := tbl.VisibleRange()
	if start != 0 || end != 10 {
		t.Errorf("VisibleRange() = (%d, %d), want (0, 10)", start, end)
	}

	tbl.MoveCursor(50)
	start, end = tbl.VisibleRange()
	if end-start != 10 {
		t.Errorf("the window should stay ten rows, got %d", end-start)
	}
	if tbl.Cursor() < start || tbl.Cursor() >= end {
		t.Errorf("cursor %d is outside the visible window [%d, %d)", tbl.Cursor(), start, end)
	}
}

func TestEmptyTableIsSafe(t *testing.T) {
	th := testTheme()
	tbl := table.New(table.Column{Title: "N", Width: 10})
	tbl.Focus()

	if tbl.SelectedRow() != nil {
		t.Error("an empty table has no selected row")
	}
	tbl.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("j")})
	tbl.View(th, 20, 5) // must not panic
}

// ----------------------------------------------------------------- chrome ---

func TestStatusBarDropsOverflow(t *testing.T) {
	th := testTheme()
	shortcuts := []chrome.Shortcut{
		{Key: "Enter", Label: "Open"},
		{Key: "F2", Label: "Delete"},
		{Key: "Ctrl+N", Label: "Create"},
		{Key: "F5", Label: "Refresh"},
		{Key: "q", Label: "Quit"},
	}

	narrow := plain(chrome.StatusBar(th, shortcuts, 30))
	if ansi.StringWidth(narrow) > 30 {
		t.Errorf("status bar is %d cells wide, want at most 30: %q", ansi.StringWidth(narrow), narrow)
	}
	// The first entries are the most relevant, so they are the ones kept.
	if !strings.Contains(narrow, "Open") {
		t.Errorf("the first shortcut should survive, got %q", narrow)
	}
}

func TestClusterChip(t *testing.T) {
	th := testTheme()

	// Colour cannot be asserted here: lipgloss emits no escape codes without a
	// TTY. The contrast rule is tested directly in theme/theme_test.go.
	for _, colour := range []string{"#FFFF00", "#000080", ""} {
		if got := plain(th.ClusterChip("prod", colour)); got != " prod " {
			t.Errorf("ClusterChip(prod, %q) = %q, want the padded name", colour, got)
		}
	}
}

// ------------------------------------------------------------------ modal ---

func TestOverlayCompositesInPlace(t *testing.T) {
	background := strings.Join([]string{
		strings.Repeat("a", 40),
		strings.Repeat("b", 40),
		strings.Repeat("c", 40),
		strings.Repeat("d", 40),
		strings.Repeat("e", 40),
	}, "\n")
	foreground := "XXXX\nYYYY"

	out := strings.Split(modal.Overlay(background, foreground), "\n")

	if len(out) != 5 {
		t.Fatalf("overlay changed the line count to %d, want 5", len(out))
	}
	for i, line := range out {
		if w := ansi.StringWidth(line); w != 40 {
			t.Errorf("line %d is %d cells wide, want 40: %q", i, w, line)
		}
	}
	// Centred vertically: rows 1 and 2 of five, with two lines of foreground.
	if !strings.Contains(out[1], "XXXX") || !strings.Contains(out[2], "YYYY") {
		t.Errorf("the overlay is not centred:\n%s", strings.Join(out, "\n"))
	}
	// The background survives on either side.
	if !strings.HasPrefix(out[1], "bbbb") || !strings.HasSuffix(out[1], "bbbb") {
		t.Errorf("background lost around the overlay: %q", out[1])
	}
}

func TestConfirmRequiresThePhrase(t *testing.T) {
	c := modal.NewDangerConfirm("Delete topic", "orders.events", "12 partitions", "orders.events")

	if c.Ready() {
		t.Error("confirmation must not be available before the phrase is typed")
	}

	c.Update(tea.KeyMsg{Type: tea.KeyEnter})
	if c.Confirmed() {
		t.Error("Enter must do nothing until the phrase matches")
	}

	for _, r := range "orders.events" {
		c.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}
	if !c.Ready() {
		t.Fatal("the typed phrase matches, confirmation should be available")
	}

	c.Update(tea.KeyMsg{Type: tea.KeyEnter})
	if !c.Confirmed() {
		t.Error("Enter should confirm once the phrase matches")
	}
}

func TestConfirmCancels(t *testing.T) {
	c := modal.NewConfirm("Delete", "Really?")

	c.Update(tea.KeyMsg{Type: tea.KeyEsc})
	if !c.Cancelled() {
		t.Error("Esc should cancel")
	}
}

func TestSimpleConfirmAcceptsY(t *testing.T) {
	c := modal.NewConfirm("Delete", "Really?")

	c.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("y")})
	if !c.Confirmed() {
		t.Error("y should confirm a dialog with no phrase to type")
	}
}
