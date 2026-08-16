package spark

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// Braille rendering, btop-style: every cell is a 2×4 grid of dots, which packs
// four times the vertical and twice the horizontal resolution of a block
// glyph. The Braille Patterns block is plain Unicode — not a Nerd Font — but
// plain-fonts mode still avoids it, so every caller here pairs a braille chart
// with a block fallback.

// brailleBase is the empty pattern; dots are OR-ed onto it.
const brailleBase = 0x2800

// brailleDots maps [row][column] of the 4×2 dot grid to its bit, rows top
// down. The layout is historical (dots 1-8), hence the irregular values.
var brailleDots = [4][2]rune{
	{0x01, 0x08},
	{0x02, 0x10},
	{0x04, 0x20},
	{0x40, 0x80},
}

// BrailleLine renders values as a one-row sparkline of braille dots: four
// vertical levels, two samples per cell.
//
// The scale starts at zero, like Line: a bar's height means "this much of the
// peak". A non-zero value always shows at least one dot, and a zero shows the
// floor dot for the same reason Line draws the lowest block — an empty cell
// reads as missing data, not as an idle series.
func BrailleLine(values []float64, width int) string {
	return BrailleLineOn(values, width, 0)
}

// BrailleLineOn is BrailleLine on a fixed scale: a bar's height means "this
// much of peak" rather than of the series' own maximum. Made for percentages,
// where 12% of CPU must not fill the row just because it is the largest sample
// seen. A peak of zero (or below the data) falls back to the measured maximum.
func BrailleLineOn(values []float64, width int, peak float64) string {
	if width <= 0 {
		return ""
	}
	if len(values) == 0 {
		return strings.Repeat(" ", width)
	}

	scaled := resample(values, width*2)
	peak = max(peak, maxOf(scaled))

	var b strings.Builder
	for cell := range width {
		pattern := brailleBase
		for half := range 2 {
			level := dotLevel(scaled[cell*2+half], peak, 4)
			for dot := range level {
				pattern |= int(brailleDots[3-dot][half])
			}
		}
		b.WriteRune(rune(pattern))
	}
	return b.String()
}

// BrailleChart renders values as a filled area chart of braille dots, height
// rows tall — the btop look. Rows come back top first.
func BrailleChart(values []float64, width, height int) []string {
	if width <= 0 || height <= 0 {
		return nil
	}

	rows := make([]string, height)
	if len(values) == 0 {
		for i := range rows {
			rows[i] = strings.Repeat(" ", width)
		}
		return rows
	}

	scaled := resample(values, width*2)
	peak := maxOf(scaled)
	total := height * 4

	// levels[x] is how many dots of the column are lit, bottom up.
	levels := make([]int, len(scaled))
	for x, value := range scaled {
		levels[x] = dotLevel(value, peak, total)
	}

	for row := range height {
		var b strings.Builder
		for cell := range width {
			pattern := brailleBase
			for half := range 2 {
				lit := levels[cell*2+half]
				for dot := range 4 {
					// Global dot row, top first; lit when it is within the
					// column's height measured from the bottom.
					if total-(row*4+dot) <= lit {
						pattern |= int(brailleDots[dot][half])
					}
				}
			}
			b.WriteRune(rune(pattern))
		}
		rows[row] = b.String()
	}
	return rows
}

// BrailleMultiLine draws several series as continuous lines in one chart, each
// in its own style, all on a shared zero-based scale so their heights compare.
// Neighbouring samples are joined with vertical segments — one dot per column
// would scatter a steep or sparse series into unreadable "stars". Earlier
// series win cells that collide, so the first — the caller's most important —
// is never hidden by the rest. Rows come back top first, styles applied.
func BrailleMultiLine(series [][]float64, styles []lipgloss.Style, width, height int) []string {
	if width <= 0 || height <= 0 || len(series) == 0 {
		return nil
	}

	// One shared peak: separate scales would make every line touch the top and
	// the comparison meaningless.
	peak := 0.0
	scaled := make([][]float64, len(series))
	for i, values := range series {
		scaled[i] = resample(values, width*2)
		peak = max(peak, maxOf(scaled[i]))
	}

	total := height * 4
	// owner[y][x] is the series whose dot occupies the point, -1 for none.
	owner := make([][]int, total)
	for y := range owner {
		owner[y] = make([]int, width*2)
		for x := range owner[y] {
			owner[y][x] = -1
		}
	}

	// Later series first, so earlier ones overwrite them where lines cross. An
	// idle series stays visible as a line along the floor, like the block
	// charts' zero row.
	for s := len(scaled) - 1; s >= 0; s-- {
		plotLine(owner, scaled[s], peak, total, s)
	}

	rows := make([]string, height)
	for row := range height {
		var b strings.Builder

		// Adjacent cells of one series join into a single styled run, keeping
		// the escape codes down.
		runOwner := -2
		var run strings.Builder
		flush := func() {
			if run.Len() == 0 {
				return
			}
			if runOwner >= 0 && runOwner < len(styles) {
				b.WriteString(styles[runOwner].Render(run.String()))
			} else {
				b.WriteString(run.String())
			}
			run.Reset()
		}

		for cell := range width {
			pattern := brailleBase
			cellOwner := -1
			for half := range 2 {
				for dot := range 4 {
					s := owner[row*4+dot][cell*2+half]
					if s < 0 {
						continue
					}
					pattern |= int(brailleDots[dot][half])
					if cellOwner < 0 || s < cellOwner {
						// A cell shared by two lines takes the colour of the
						// more important one.
						cellOwner = s
					}
				}
			}
			if cellOwner != runOwner {
				flush()
				runOwner = cellOwner
			}
			if pattern == brailleBase {
				run.WriteRune(' ')
			} else {
				run.WriteRune(rune(pattern))
			}
		}
		flush()
		rows[row] = b.String()
	}
	return rows
}

// BrailleGauge renders a fraction as a horizontal dot bar at two positions per
// cell. The filled part and the remaining track come back separately so the
// caller can style them apart — a bright bar on a dim track, the btop look.
func BrailleGauge(fraction float64, width int) (filled, track string) {
	if width <= 0 {
		return "", ""
	}
	if fraction < 0 {
		fraction = 0
	}
	if fraction > 1 {
		fraction = 1
	}

	columns := int(fraction*float64(width*2) + 0.5)
	full, half := columns/2, columns%2

	var b strings.Builder
	b.WriteString(strings.Repeat("⣿", full))
	if half > 0 {
		b.WriteRune('⡇') // the left dot column only
	}
	return b.String(), strings.Repeat("⠒", width-full-half)
}

// ASCIIMultiLine is BrailleMultiLine for plain-fonts mode: one asterisk per
// cell instead of dots, at cell resolution. Coarser, but every terminal has
// an asterisk.
func ASCIIMultiLine(series [][]float64, styles []lipgloss.Style, width, height int) []string {
	if width <= 0 || height <= 0 || len(series) == 0 {
		return nil
	}

	peak := 0.0
	scaled := make([][]float64, len(series))
	for i, values := range series {
		scaled[i] = resample(values, width)
		peak = max(peak, maxOf(scaled[i]))
	}

	owner := make([][]int, height)
	for y := range owner {
		owner[y] = make([]int, width)
		for x := range owner[y] {
			owner[y][x] = -1
		}
	}
	for s := len(scaled) - 1; s >= 0; s-- {
		plotLine(owner, scaled[s], peak, height, s)
	}

	rows := make([]string, height)
	for y := range height {
		var b strings.Builder
		for x := range width {
			s := owner[y][x]
			if s < 0 {
				b.WriteRune(' ')
				continue
			}
			if s < len(styles) {
				b.WriteString(styles[s].Render("*"))
			} else {
				b.WriteRune('*')
			}
		}
		rows[y] = b.String()
	}
	return rows
}

// plotLine marks one series' dots in the owner grid, joining neighbouring
// columns with vertical segments the way a line chart does: half of each jump
// in the leaving column, half in the arriving one.
func plotLine(owner [][]int, values []float64, peak float64, total, s int) {
	prev := -1
	for x, value := range values {
		y := total - dotLevel(value, peak, total)
		if prev >= 0 && absInt(prev-y) > 1 {
			mid := (prev + y) / 2
			fillColumn(owner, x-1, prev, mid, s)
			fillColumn(owner, x, mid, y, s)
		}
		owner[y][x] = s
		prev = y
	}
}

// fillColumn marks the vertical run between two dot rows, inclusive.
func fillColumn(owner [][]int, x, from, to, s int) {
	if from > to {
		from, to = to, from
	}
	for y := from; y <= to; y++ {
		owner[y][x] = s
	}
}

func absInt(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// dotLevel converts a value to a dot count out of total, on the same
// zero-based scale the block charts use: zero stays one floor dot, anything
// positive lights at least one.
func dotLevel(value, peak float64, total int) int {
	if peak <= 0 || value <= 0 {
		return 1
	}
	level := int(value/peak*float64(total) + 0.5)
	return min(max(level, 1), total)
}
