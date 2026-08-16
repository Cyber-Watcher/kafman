package spark

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// Box-drawing line charts: series drawn with the same glyphs panel borders
// use (─ ╭ ╮ ╰ ╯ │), so a line reads as a line rather than as a run of dots.
// Cell resolution — coarser vertically than braille, and far easier to
// follow; braille stays for the filled area chart and the one-row sparklines.

// LineChartMulti draws several series as continuous lines, each in its own
// style, all on a shared zero-based scale so their heights compare. Later
// series are drawn first, so the earlier — the caller's most important — stay
// on top where lines cross. Rows come back top first, styles applied.
func LineChartMulti(series [][]float64, styles []lipgloss.Style, width, height int) []string {
	if width <= 0 || height <= 0 || len(series) == 0 {
		return nil
	}

	// One shared peak: separate scales would make every line touch the top
	// and the comparison meaningless.
	peak := 0.0
	scaled := make([][]float64, len(series))
	for i, values := range series {
		scaled[i] = resample(values, width)
		peak = max(peak, maxOf(scaled[i]))
	}

	glyphs := make([][]rune, height)
	owner := make([][]int, height)
	for y := range height {
		glyphs[y] = make([]rune, width)
		owner[y] = make([]int, width)
		for x := range width {
			glyphs[y][x] = ' '
			owner[y][x] = -1
		}
	}

	set := func(y, x int, r rune, s int) {
		glyphs[y][x] = r
		owner[y][x] = s
	}

	for s := len(scaled) - 1; s >= 0; s-- {
		values := scaled[s]
		levels := make([]int, len(values))
		for i, value := range values {
			levels[i] = cellLevel(value, peak, height)
		}

		// The asciichart transition scheme: a flat step is one dash, a jump
		// draws the leaving corner, the arriving corner and the vertical run
		// between them, all in the leaving column.
		for x := 0; x+1 < len(values); x++ {
			y0, y1 := levels[x], levels[x+1]
			if y0 == y1 {
				set(height-1-y0, x, '─', s)
				continue
			}
			leaving, arriving := '╮', '╰'
			if y1 > y0 {
				leaving, arriving = '╯', '╭'
			}
			set(height-1-y1, x, arriving, s)
			set(height-1-y0, x, leaving, s)
			for y := min(y0, y1) + 1; y < max(y0, y1); y++ {
				set(height-1-y, x, '│', s)
			}
		}
		if n := len(values); n > 0 {
			set(height-1-levels[n-1], width-1, '─', s)
		}
	}

	rows := make([]string, height)
	for y := range height {
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

		for x := range width {
			if owner[y][x] != runOwner {
				flush()
				runOwner = owner[y][x]
			}
			run.WriteRune(glyphs[y][x])
		}
		flush()
		rows[y] = b.String()
	}
	return rows
}

// cellLevel converts a value to a row index out of height, bottom-based: zero
// sits on the floor row, the peak on the top one.
func cellLevel(value, peak float64, height int) int {
	if peak <= 0 || value <= 0 {
		return 0
	}
	level := int(value/peak*float64(height-1) + 0.5)
	return min(max(level, 0), height-1)
}
