package spark

import (
	"strings"
	"testing"

	"github.com/charmbracelet/x/ansi"
)

func TestLineWidthIsExact(t *testing.T) {
	for _, width := range []int{1, 5, 20, 80} {
		for _, values := range [][]float64{nil, {1}, {1, 2, 3}, make([]float64, 500)} {
			got := Line(values, width, false)
			if ansi.StringWidth(got) != width {
				t.Errorf("Line(%d values, width %d) is %d cells wide",
					len(values), width, ansi.StringWidth(got))
			}
		}
	}
}

// A flat line at zero is information; an empty row would read as missing data.
func TestZeroSeriesIsDrawnNotBlank(t *testing.T) {
	got := Line([]float64{0, 0, 0}, 3, false)
	if strings.TrimSpace(got) == "" {
		t.Errorf("a zero series should still be drawn, got %q", got)
	}
}

// The scale starts at zero, so a series wobbling between 900 and 1000 looks
// flat rather than full-scale.
func TestScaleStartsAtZero(t *testing.T) {
	flat := Line([]float64{900, 950, 1000}, 3, false)
	spiky := Line([]float64{0, 500, 1000}, 3, false)

	if flat == spiky {
		t.Errorf("a near-flat series and a spike should not draw the same: %q", flat)
	}
	if strings.Count(flat, "█") == 0 {
		t.Errorf("the peak should reach the top block, got %q", flat)
	}
	// The lowest of the near-flat values is nine tenths of the peak, so it must
	// be drawn high, not near the floor.
	if strings.HasPrefix(flat, "▁") {
		t.Errorf("900 out of 1000 should not be the lowest block: %q", flat)
	}
}

func TestLineFallsBackToASCII(t *testing.T) {
	got := Line([]float64{1, 5, 10}, 3, true)
	for _, r := range got {
		if r > 127 {
			t.Errorf("plain mode should stay ASCII, got %q", got)
			break
		}
	}
}

func TestGaugeWidthAndFill(t *testing.T) {
	tests := []struct {
		name     string
		fraction float64
		width    int
		full     int
	}{
		{name: "empty", fraction: 0, width: 10, full: 0},
		{name: "half", fraction: 0.5, width: 10, full: 5},
		{name: "full", fraction: 1, width: 10, full: 10},
		{name: "over one is clamped", fraction: 4, width: 10, full: 10},
		{name: "below zero is clamped", fraction: -1, width: 10, full: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := Gauge(test.fraction, test.width, false)
			if ansi.StringWidth(got) != test.width {
				t.Errorf("Gauge is %d cells wide, want %d: %q",
					ansi.StringWidth(got), test.width, got)
			}
			if full := strings.Count(got, "█"); full != test.full {
				t.Errorf("Gauge(%v) has %d full blocks, want %d: %q",
					test.fraction, full, test.full, got)
			}
		})
	}
}

// The eighth-blocks are what make a narrow gauge readable: without them a
// 5-cell bar could only show five values.
func TestGaugeHasSubCellPrecision(t *testing.T) {
	a := Gauge(0.42, 5, false)
	b := Gauge(0.50, 5, false)

	if a == b {
		t.Errorf("42%% and 50%% should differ at width 5, both are %q", a)
	}
}

func TestChartShape(t *testing.T) {
	rows := Chart([]float64{1, 2, 3, 4}, 4, 3, false)

	if len(rows) != 3 {
		t.Fatalf("chart has %d rows, want 3", len(rows))
	}
	for i, row := range rows {
		if ansi.StringWidth(row) != 4 {
			t.Errorf("row %d is %d cells wide, want 4: %q", i, ansi.StringWidth(row), row)
		}
	}

	// The tallest column reaches the top row; the shortest does not.
	if !strings.HasSuffix(rows[0], "█") {
		t.Errorf("the peak should reach the top row: %q", rows[0])
	}
	if strings.HasPrefix(rows[0], "█") {
		t.Errorf("the smallest value should not reach the top row: %q", rows[0])
	}
	// The bottom row is filled wherever there is any value at all.
	if strings.Contains(rows[2], " ") {
		t.Errorf("the bottom row should be filled: %q", rows[2])
	}
}

func TestChartHandlesEmptyAndFlat(t *testing.T) {
	empty := Chart(nil, 5, 2, false)
	if len(empty) != 2 {
		t.Fatalf("an empty chart should still have its rows, got %d", len(empty))
	}
	for _, row := range empty {
		if ansi.StringWidth(row) != 5 {
			t.Errorf("empty row is %d cells wide, want 5", ansi.StringWidth(row))
		}
	}

	flat := Chart([]float64{0, 0, 0}, 3, 2, false)
	if strings.TrimSpace(flat[len(flat)-1]) == "" {
		t.Errorf("a flat zero series should still show its floor: %q", flat)
	}
}

// More samples than columns are averaged rather than sampled, so a spike
// between two sampled points cannot vanish.
func TestResampleAveragesRatherThanSkips(t *testing.T) {
	values := make([]float64, 100)
	values[42] = 1000

	got := resample(values, 10)
	if len(got) != 10 {
		t.Fatalf("resample returned %d values, want 10", len(got))
	}

	var sum float64
	for _, value := range got {
		sum += value
	}
	if sum == 0 {
		t.Error("the spike disappeared: buckets should average, not sample")
	}
}

// Fewer samples than columns grow in from the right, the way a live graph does.
func TestResamplePadsOnTheLeft(t *testing.T) {
	got := resample([]float64{5, 6}, 5)

	if len(got) != 5 {
		t.Fatalf("resample returned %d values, want 5", len(got))
	}
	if got[3] != 5 || got[4] != 6 {
		t.Errorf("the newest values should sit at the right: %v", got)
	}
	if got[0] != 0 {
		t.Errorf("the left should be padded: %v", got)
	}
}
