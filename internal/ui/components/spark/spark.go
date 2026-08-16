// Package spark draws the small graphics the Monitoring tab is made of:
// sparklines, bar gauges and single- and multi-series charts.
//
// Two families: braille dots (braille.go), the btop look, packing a 2×4 dot
// grid into every cell; and Unicode blocks with an ASCII fallback (this file)
// for plain-fonts mode, where the braille glyphs cannot be relied on. Braille
// is plain Unicode rather than a Nerd Font, but a font that renders it badly
// ruins every chart at once — so the plain-fonts switch keeps working as the
// escape hatch.
package spark

import (
	"math"
	"strings"
)

// blocks are the eighths used for vertical resolution inside one cell.
var blocks = []rune{'▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'}

// bars are the same eighths horizontally, for gauges.
var bars = []rune{'▏', '▎', '▍', '▌', '▋', '▊', '▉', '█'}

// asciiBlocks stand in when Nerd Fonts, or any decent block support, is off.
var asciiBlocks = []rune{'.', '.', ':', ':', '|', '|', '#', '#'}

// Line renders values as a one-row sparkline of the given width.
//
// The scale is taken from the values themselves and starts at zero, so the
// height of a bar means "this much of the peak", not "this much of the range" —
// a series wobbling between 900 and 1000 should look flat, not full-scale.
func Line(values []float64, width int, plain bool) string {
	return LineOn(values, width, 0, plain)
}

// LineOn is Line on a fixed scale — see BrailleLineOn. A peak of zero (or
// below the data) falls back to the measured maximum.
func LineOn(values []float64, width int, peak float64, plain bool) string {
	if width <= 0 {
		return ""
	}
	if len(values) == 0 {
		return strings.Repeat(" ", width)
	}

	scaled := resample(values, width)
	peak = math.Max(peak, maxOf(scaled))

	glyphs := blocks
	if plain {
		glyphs = asciiBlocks
	}

	var b strings.Builder
	for _, value := range scaled {
		b.WriteRune(cell(value, peak, glyphs))
	}
	return b.String()
}

// cell picks the block for one value against a peak.
func cell(value, peak float64, glyphs []rune) rune {
	if peak <= 0 || value <= 0 {
		// Zero is drawn as the lowest block rather than a space: a flat line at
		// zero is information, an empty row reads as missing data.
		return glyphs[0]
	}

	level := int(math.Round(value / peak * float64(len(glyphs)-1)))
	return glyphs[min(max(level, 0), len(glyphs)-1)]
}

// Gauge renders a fraction as a bar of the given width, with sub-cell precision.
func Gauge(fraction float64, width int, plain bool) string {
	if width <= 0 {
		return ""
	}
	fraction = math.Max(0, math.Min(1, fraction))

	full := int(fraction * float64(width))
	remainder := fraction*float64(width) - float64(full)

	if plain {
		return strings.Repeat("#", full) + strings.Repeat(".", width-full)
	}

	var b strings.Builder
	b.WriteString(strings.Repeat("█", full))

	// The eighth-blocks are what make a gauge readable at small widths: without
	// them a 5-cell bar can only show five values.
	if full < width {
		if part := int(remainder * 8); part > 0 {
			b.WriteRune(bars[min(part-1, len(bars)-1)])
			full++
		}
	}
	if pad := width - full; pad > 0 {
		b.WriteString(strings.Repeat(" ", pad))
	}
	return b.String()
}

// Chart renders values as a chart height rows tall and width columns wide.
//
// Each column is one bar drawn from the bottom up, so the shape reads as an area
// chart. The returned rows are top first, ready to join with newlines.
func Chart(values []float64, width, height int, plain bool) []string {
	if width <= 0 || height <= 0 {
		return nil
	}

	glyphs := blocks
	if plain {
		glyphs = asciiBlocks
	}

	rows := make([]string, height)
	if len(values) == 0 {
		for i := range rows {
			rows[i] = strings.Repeat(" ", width)
		}
		return rows
	}

	scaled := resample(values, width)
	peak := maxOf(scaled)
	if peak <= 0 {
		for i := range rows {
			rows[i] = strings.Repeat(" ", width)
		}
		// A flat zero series still gets a floor, for the same reason Line does.
		rows[height-1] = strings.Repeat(string(glyphs[0]), width)
		return rows
	}

	for row := range height {
		// Row 0 is the top of the chart; each row covers one height-th of the
		// range.
		top := float64(height-row) / float64(height)
		bottom := float64(height-row-1) / float64(height)

		var b strings.Builder
		for _, value := range scaled {
			fraction := value / peak
			switch {
			case fraction >= top:
				b.WriteRune('█')
			case fraction <= bottom:
				b.WriteRune(' ')
			default:
				// Partly filled: the block shows how far into this row it reaches.
				within := (fraction - bottom) * float64(height)
				b.WriteRune(cell(within, 1, glyphs))
			}
		}
		rows[row] = b.String()
	}
	return rows
}

// resample fits values into exactly width columns.
//
// Too many values are averaged into buckets rather than sampled, so a spike
// between two sampled points cannot vanish; too few are stretched, keeping the
// newest value in the rightmost column.
func resample(values []float64, width int) []float64 {
	switch {
	case len(values) == 0:
		return nil
	case len(values) == width:
		return values
	case len(values) > width:
		out := make([]float64, width)
		for i := range width {
			start := i * len(values) / width
			end := (i + 1) * len(values) / width
			if end <= start {
				end = start + 1
			}
			var sum float64
			for _, value := range values[start:min(end, len(values))] {
				sum += value
			}
			out[i] = sum / float64(min(end, len(values))-start)
		}
		return out
	default:
		// Fewer samples than columns: pad on the left so the series grows into the
		// chart from the right, the way a live graph does.
		out := make([]float64, width)
		offset := width - len(values)
		copy(out[offset:], values)
		return out
	}
}

func maxOf(values []float64) float64 {
	peak := 0.0
	for _, value := range values {
		if value > peak {
			peak = value
		}
	}
	return peak
}
