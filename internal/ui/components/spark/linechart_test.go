package spark

import (
	"strings"
	"testing"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"
)

func TestLineChartMultiShape(t *testing.T) {
	style := lipgloss.NewStyle()
	// Flat at zero, a jump to the top, flat at the top.
	rows := LineChartMulti([][]float64{{0, 0, 10, 10}}, []lipgloss.Style{style}, 4, 3)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	for i, row := range rows {
		if got := len([]rune(ansi.Strip(row))); got != 4 {
			t.Errorf("row %d: %d runes, want 4", i, got)
		}
	}

	top, mid, bottom := ansi.Strip(rows[0]), ansi.Strip(rows[1]), ansi.Strip(rows[2])
	if !strings.Contains(bottom, "─") || !strings.Contains(bottom, "╯") {
		t.Errorf("the floor should run flat and turn up, got %q", bottom)
	}
	if !strings.Contains(top, "╭") || !strings.Contains(top, "─") {
		t.Errorf("the top should arrive with a corner and run flat, got %q", top)
	}
	if strings.TrimSpace(mid) != "│" {
		t.Errorf("the jump should be a vertical run, got %q", mid)
	}
}

// The chart draws with the border glyphs only — that is its whole point.
func TestLineChartMultiUsesBoxDrawing(t *testing.T) {
	rows := LineChartMulti([][]float64{{3, 1, 4, 1, 5, 9, 2, 6}}, []lipgloss.Style{lipgloss.NewStyle()}, 10, 5)
	for _, row := range rows {
		for _, r := range ansi.Strip(row) {
			if !strings.ContainsRune(" ─│╭╮╰╯", r) {
				t.Fatalf("unexpected rune %q in %q", r, row)
			}
		}
	}
}

// Two series share one scale, and the first stays on top of a collision.
func TestLineChartMultiSharesTheScale(t *testing.T) {
	styles := []lipgloss.Style{lipgloss.NewStyle(), lipgloss.NewStyle()}
	rows := LineChartMulti([][]float64{{10, 10}, {5, 5}}, styles, 4, 5)

	if strings.TrimSpace(ansi.Strip(rows[0])) == "" {
		t.Error("the taller series should reach the top row")
	}
	if strings.TrimSpace(ansi.Strip(rows[2])) == "" {
		t.Error("the shorter series should sit around the middle")
	}
}
