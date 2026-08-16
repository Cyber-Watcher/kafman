package spark

import (
	"strings"
	"testing"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"
	"github.com/muesli/termenv"
)

func TestBrailleLineWidthIsExact(t *testing.T) {
	for _, width := range []int{1, 7, 20} {
		line := BrailleLine([]float64{1, 5, 2, 8, 3}, width)
		if got := len([]rune(line)); got != width {
			t.Errorf("width %d: got %d runes", width, got)
		}
	}
}

func TestBrailleLineUsesBrailleRunes(t *testing.T) {
	line := BrailleLine([]float64{0, 2, 4, 6, 8}, 10)
	for _, r := range line {
		if r != ' ' && (r < 0x2800 || r > 0x28FF) {
			t.Fatalf("non-braille rune %q in %q", r, line)
		}
	}
}

// A peak fills the whole cell, a zero keeps a floor dot: same contract as the
// block sparkline.
func TestBrailleLineScale(t *testing.T) {
	line := []rune(BrailleLine([]float64{0, 0, 8, 8}, 2))

	if line[0] != rune(0x2800|0x40|0x80) {
		t.Errorf("a zero column should show floor dots, got %q", line[0])
	}
	if line[1] != rune(0x2800|0xFF) {
		t.Errorf("a peak column should be fully lit, got %q", line[1])
	}
}

func TestBrailleChartShape(t *testing.T) {
	rows := BrailleChart([]float64{0, 0, 0, 10}, 2, 2)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	for i, row := range rows {
		if got := len([]rune(row)); got != 2 {
			t.Errorf("row %d: got %d runes", i, got)
		}
	}

	// The peak lives in the right column: its top-row cell must have dots
	// there, while the left column's top row has none.
	top := []rune(rows[0])
	if top[0] != rune(brailleBase) {
		t.Errorf("the idle side should be empty at the top, got %q", top[0])
	}
	if top[1] == rune(brailleBase) {
		t.Error("the peak should reach the top row")
	}
	// The bottom row keeps a floor for the zero columns.
	bottom := []rune(rows[1])
	if bottom[0] == rune(brailleBase) {
		t.Error("a zero series still draws its floor")
	}
}

func TestBrailleMultiLineSharesTheScale(t *testing.T) {
	styles := []lipgloss.Style{
		lipgloss.NewStyle().Foreground(lipgloss.Color("#FF0000")),
		lipgloss.NewStyle().Foreground(lipgloss.Color("#00FF00")),
	}
	// Series 0 peaks at 10, series 1 at 5: on a shared scale the second line
	// must sit around the middle, not at the top.
	rows := BrailleMultiLine([][]float64{{10, 10}, {5, 5}}, styles, 4, 4)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}

	plain := make([]string, len(rows))
	for i, row := range rows {
		plain[i] = ansi.Strip(row)
		if got := len([]rune(plain[i])); got != 4 {
			t.Errorf("row %d: %d runes after stripping styles", i, got)
		}
	}

	if strings.TrimSpace(plain[0]) == "" {
		t.Error("the taller series should reach the top row")
	}
	if strings.TrimSpace(plain[1]) != "" && strings.TrimSpace(plain[2]) == "" {
		t.Error("the shorter series should sit near the middle")
	}
}

// Where two lines land in one cell, the earlier series' colour wins: the
// caller puts the most important one first.
func TestBrailleMultiLineEarlierSeriesWins(t *testing.T) {
	// Tests run without a TTY, where lipgloss strips colours; force them on so
	// the ownership of a cell is observable.
	profile := lipgloss.ColorProfile()
	lipgloss.SetColorProfile(termenv.TrueColor)
	t.Cleanup(func() { lipgloss.SetColorProfile(profile) })

	red := lipgloss.NewStyle().Foreground(lipgloss.Color("#FF0000"))
	green := lipgloss.NewStyle().Foreground(lipgloss.Color("#00FF00"))

	// Both series peak in the same cells, so every lit cell is a collision.
	rows := BrailleMultiLine([][]float64{{8, 8}, {8, 8}}, []lipgloss.Style{red, green}, 2, 2)
	joined := strings.Join(rows, "\n")
	if !strings.Contains(joined, "38;2;255;0;0") {
		t.Errorf("the colliding cells should carry the first series' colour:\n%q", joined)
	}
	if strings.Contains(joined, "38;2;0;255;0") {
		t.Errorf("the hidden series should not surface anywhere:\n%q", joined)
	}
}

func TestASCIIMultiLineIsPlain(t *testing.T) {
	styles := []lipgloss.Style{lipgloss.NewStyle()}
	rows := ASCIIMultiLine([][]float64{{1, 5, 9}}, styles, 6, 3)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	for _, row := range rows {
		for _, r := range ansi.Strip(row) {
			if r != ' ' && r != '*' {
				t.Fatalf("plain chart may only use spaces and asterisks, got %q", r)
			}
		}
	}
}

func TestBrailleGauge(t *testing.T) {
	filled, track := BrailleGauge(0.5, 8)
	if got := len([]rune(filled)) + len([]rune(track)); got != 8 {
		t.Errorf("filled+track should cover the width, got %d", got)
	}
	if len([]rune(filled)) != 4 {
		t.Errorf("half of 8 cells should be filled, got %q", filled)
	}

	// The half-cell resolution: 5/16 lands on two full cells and a half column.
	filled, _ = BrailleGauge(5.0/16.0, 8)
	if filled != "⣿⣿⡇" {
		t.Errorf("expected two full cells and a half column, got %q", filled)
	}

	if filled, track := BrailleGauge(0, 4); filled != "" || track != "⠒⠒⠒⠒" {
		t.Errorf("an empty gauge is all track, got %q + %q", filled, track)
	}
	if filled, track := BrailleGauge(1, 4); filled != "⣿⣿⣿⣿" || track != "" {
		t.Errorf("a full gauge has no track, got %q + %q", filled, track)
	}
}

// A steep series must render as a continuous line, not scattered dots: every
// pair of neighbouring dot columns has to touch or overlap vertically.
func TestBrailleMultiLineIsContinuous(t *testing.T) {
	style := lipgloss.NewStyle()
	rows := BrailleMultiLine([][]float64{{0, 100, 0, 100, 50, 0}}, []lipgloss.Style{style}, 12, 6)

	// Reconstruct the lit dot rows per dot column from the rendered runes.
	height := len(rows)
	width := len([]rune(ansi.Strip(rows[0])))
	lit := make([][]bool, width*2)
	for x := range lit {
		lit[x] = make([]bool, height*4)
	}
	for rowIdx, row := range rows {
		for cell, r := range []rune(ansi.Strip(row)) {
			if r < 0x2800 || r > 0x28FF {
				continue
			}
			pattern := int(r - 0x2800)
			for dot := range 4 {
				for half := range 2 {
					if pattern&int(brailleDots[dot][half]) != 0 {
						lit[cell*2+half][rowIdx*4+dot] = true
					}
				}
			}
		}
	}

	span := func(x int) (top, bottom int, any bool) {
		top, bottom = -1, -1
		for y, on := range lit[x] {
			if !on {
				continue
			}
			if top < 0 {
				top = y
			}
			bottom = y
		}
		return top, bottom, top >= 0
	}

	for x := 1; x < width*2; x++ {
		pt, pb, pok := span(x - 1)
		ct, cb, cok := span(x)
		if !pok || !cok {
			t.Fatalf("dot column %d or %d is empty — the line has a hole", x-1, x)
		}
		// Adjacent columns must touch: a gap between their vertical spans is
		// exactly the "starry sky" this joins were added to prevent.
		if ct > pb+1 || cb < pt-1 {
			t.Errorf("columns %d and %d do not touch: [%d..%d] then [%d..%d]",
				x-1, x, pt, pb, ct, cb)
		}
	}
}

// A fixed scale keeps a small percentage small: 25 out of 100 must not fill
// the cell just because it is the series' own maximum.
func TestBrailleLineOnFixedScale(t *testing.T) {
	line := []rune(BrailleLineOn([]float64{25, 25}, 1, 100))
	if line[0] != rune(0x2800|0x40|0x80) {
		t.Errorf("25%% on a 0-100 scale is one dot of four, got %q", string(line[0]))
	}

	full := []rune(BrailleLineOn([]float64{100, 100}, 1, 100))
	if full[0] != rune(0x2800|0xFF) {
		t.Errorf("100%% should fill the cell, got %q", string(full[0]))
	}

	// A peak below the data falls back to the measured maximum.
	over := []rune(BrailleLineOn([]float64{200, 200}, 1, 100))
	if over[0] != rune(0x2800|0xFF) {
		t.Errorf("data above the scale should still fit, got %q", string(over[0]))
	}
}
