package monitoring

// Formatting and drawing helpers shared by every section.

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"github.com/Cyber-Watcher/kafman/internal/kafka"
	"github.com/Cyber-Watcher/kafman/internal/metrics"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/spark"
	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

// waitingText explains an empty chart. The first poll only establishes the
// baseline every rate is measured against, so there is nothing to draw yet.
func (m *Model) waitingText() string {
	if m.view.Err != nil {
		return "collecting: " + kafka.Explain(m.view.Err)
	}
	if m.view.Polls == 0 {
		return "waiting for the first poll…"
	}
	return fmt.Sprintf("measuring: rates need two polls, the next is in %s", m.interval)
}

// sparkline draws a table-row sparkline: braille dots in the btop style, or
// ASCII blocks when plain fonts are asked for.
func (m *Model) sparkline(values []float64, width int) string {
	if m.plain {
		return spark.Line(values, width, true)
	}
	return spark.BrailleLine(values, width)
}

// seriesCell styles the sparkline cell at index spark with the row's series
// colour, so every row's chart gets its own — the btop look. The other cells
// keep the row style.
func (m *Model) seriesCell(row, spark, cells int) []*lipgloss.Style {
	if m.theme == nil {
		return nil
	}
	styles := make([]*lipgloss.Style, cells)
	style := m.theme.SeriesStyle(row)
	styles[spark] = &style
	return styles
}

// multiLineChart draws one continuous line per series over a shared
// zero-based scale: a y-axis on the left, the plot, and the time axis
// underneath. The lines use the same box-drawing glyphs the panel borders do —
// a line has to read as a line, not as a run of dots. It returns height
// lines; a legend is the caller's business, because only the caller knows
// what the series are.
func (m *Model) multiLineChart(th *theme.Theme, series [][]float64, styles []lipgloss.Style, width, height int) []string {
	peak := 0.0
	for _, values := range series {
		for _, value := range values {
			peak = max(peak, value)
		}
	}

	plotHeight := max(height-1, 1) // one row kept for the time axis

	labels := axisLabels(peak, plotHeight)
	labelWidth := 0
	for _, label := range labels {
		labelWidth = max(labelWidth, len(label))
	}
	plotWidth := max(width-labelWidth-2, 8)

	rows := spark.LineChartMulti(series, styles, plotWidth, plotHeight)
	if m.plain {
		rows = spark.ASCIIMultiLine(series, styles, plotWidth, plotHeight)
	}

	lines := make([]string, 0, height)
	for i, row := range rows {
		lines = append(lines,
			th.Dim.Render(pad(labels[i], labelWidth)+" "+th.Glyph("┤", "|"))+row)
	}
	return append(lines,
		th.Dim.Render(strings.Repeat(" ", labelWidth+1)+timeAxis(windows[m.window], plotWidth)))
}

// notConfigured centres an explanation in an empty section, wrapped so a
// narrow terminal does not push it past the frame.
func notConfigured(th *theme.Theme, width, height int, text string) string {
	note := th.Dim.Width(max(min(width-4, lipgloss.Width(text)), 10)).
		Align(lipgloss.Center).Render(text)
	return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center, note)
}

// axisLabels are the y-axis values from the peak down to zero.
//
// A flat zero series gets one label rather than a column of zeroes: repeating
// "0" down the whole axis looks like a broken renderer, not like an idle
// cluster.
func axisLabels(peak float64, rows int) []string {
	labels := make([]string, rows)
	if rows == 0 {
		return labels
	}

	if peak <= 0 {
		labels[rows-1] = "0"
		return labels
	}

	for i := range rows {
		labels[i] = formatRate(peak * float64(rows-i) / float64(rows))
	}
	labels[rows-1] = "0"
	return labels
}

// timeAxis labels the plot with the span it covers.
func timeAxis(window time.Duration, width int) string {
	left := "-" + shortWindow(window)
	right := "now"
	if width < len(left)+len(right)+2 {
		return strings.Repeat(" ", max(width, 0))
	}
	return left + strings.Repeat(" ", width-len(left)-len(right)) + right
}

func shortWindow(d time.Duration) string {
	if d >= time.Hour {
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dm", int(d.Minutes()))
}

func trendLabel(trend metrics.Trend, plain bool) string {
	marker := map[metrics.Trend]string{
		metrics.TrendGrowing:  "▲",
		metrics.TrendDraining: "▼",
		metrics.TrendSteady:   "●",
		metrics.TrendIdle:     "○",
	}[trend]
	if plain {
		marker = map[metrics.Trend]string{
			metrics.TrendGrowing:  "^",
			metrics.TrendDraining: "v",
			metrics.TrendSteady:   "=",
			metrics.TrendIdle:     "-",
		}[trend]
	}
	return marker + " " + trend.String()
}

// scrapeError shortens a scrape failure to what fits a table row.
func scrapeError(err error) string {
	text := err.Error()
	if i := strings.LastIndex(text, ": "); i > 0 && len(text)-i < 60 {
		text = text[i+2:]
	}
	return "exporter unreachable — " + text
}

func percent(value float64) string { return fmt.Sprintf("%.0f%%", value) }

// humanPause renders an average GC pause at the precision it deserves.
func humanPause(pause time.Duration) string {
	switch {
	case pause <= 0:
		return "—"
	case pause < time.Millisecond:
		return fmt.Sprintf("%dµs", pause.Microseconds())
	case pause < time.Second:
		return fmt.Sprintf("%dms", pause.Milliseconds())
	default:
		return fmt.Sprintf("%.1fs", pause.Seconds())
	}
}

// humanAge renders how long ago something happened.
func humanAge(age time.Duration) string {
	switch {
	case age < time.Second:
		return fmt.Sprintf("%dms ago", age.Milliseconds())
	case age < time.Minute:
		return fmt.Sprintf("%.1fs ago", age.Seconds())
	default:
		return fmt.Sprintf("%dm ago", int(age.Minutes()))
	}
}

// gauge draws a horizontal bar: braille dots on a dim track, or the block
// gauge in plain-fonts mode. The bar takes the given style, the track stays
// dim.
func (m *Model) gauge(fraction float64, width int, style lipgloss.Style, th *theme.Theme) string {
	if m.plain {
		return style.Render(spark.Gauge(fraction, width, true))
	}
	filled, track := spark.BrailleGauge(fraction, width)
	return style.Render(filled) + th.Dim.Render(track)
}

// historyCell draws a table-row sparkline of a percentage's history on a fixed
// scale — 12% of CPU must not fill the row just because it is the largest
// sample seen. A history that has no samples yet falls back to the current
// value alone, so the column is never blank while the number next to it is
// real.
func (m *Model) historyCell(points []metrics.Point, current float64, width int, peak float64) string {
	values := metrics.Values(points)
	if len(values) == 0 {
		values = []float64{current}
	}
	if m.plain {
		return spark.LineOn(values, width, peak, true)
	}
	return spark.BrailleLineOn(values, width, peak)
}

// headroomStyle colours a "how close to the limit" fraction: green while
// comfortable, yellow when tight, red when nearly out.
func headroomStyle(th *theme.Theme, used float64) lipgloss.Style {
	switch {
	case used >= 0.85:
		return th.Critical
	case used >= 0.6:
		return th.Warning
	default:
		return th.Success
	}
}

func fraction(value, peak int64) float64 {
	if peak <= 0 {
		return 0
	}
	return float64(value) / float64(peak)
}

// formatRate renders a per-second rate compactly: exact when small, thousands
// and millions when not.
func formatRate(rate float64) string {
	switch {
	case rate <= 0:
		return "0"
	case rate < 10:
		return fmt.Sprintf("%.1f", rate)
	case rate < 1000:
		return fmt.Sprintf("%.0f", rate)
	case rate < 1_000_000:
		return fmt.Sprintf("%.1fk", rate/1000)
	default:
		return fmt.Sprintf("%.1fM", rate/1_000_000)
	}
}

func humanBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}

	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit && exp < 4; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTP"[exp])
}

// humanCount groups digits with underscores, as every other screen does.
func humanCount(n int64) string {
	if n < 0 {
		return "-" + humanCount(-n)
	}
	digits := fmt.Sprint(n)
	if len(digits) <= 4 {
		return digits
	}

	var b strings.Builder
	lead := len(digits) % 3
	if lead > 0 {
		b.WriteString(digits[:lead])
	}
	for i := lead; i < len(digits); i += 3 {
		if b.Len() > 0 {
			b.WriteByte('_')
		}
		b.WriteString(digits[i : i+3])
	}
	return b.String()
}

func pad(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return strings.Repeat(" ", width-len(s)) + s
}

func itoa(n int) string { return fmt.Sprint(n) }

var warningStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#FFAF00"))
