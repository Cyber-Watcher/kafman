package monitoring

// The JMX section: what the brokers' jmx_exporter endpoints report — the
// numbers the Kafka API cannot give. A full-width table on top and a braille
// chart underneath, toggled between heap and bytes-in with `t`.

import (
	"fmt"
	"time"

	"github.com/charmbracelet/lipgloss"

	"github.com/Cyber-Watcher/kafman/internal/metrics"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/panel"
	"github.com/Cyber-Watcher/kafman/internal/ui/ktx"
	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

// jmxView is the section's whole content: the table panel and, when there is
// room, the chart panel.
func (m *Model) jmxView(k *ktx.Context, width, height int) string {
	th := k.Theme

	if !m.view.JMXConfigured {
		return notConfigured(th, width, height,
			"no JMX exporter configured for this cluster — add a jmx: section to the config")
	}

	table, chart := jmxHeights(height, len(m.view.JMX))

	// The chart sits on top, the same as every other section's.
	sections := make([]string, 0, 2)
	if chart > 0 {
		sections = append(sections, m.jmxChartPanel(k, width, chart))
	}
	sections = append(sections, panel.Panel{
		Title:    "JMX",
		TopRight: m.jmxCounter(),
		Width:    width,
		Height:   table,
	}.Render(th, m.jmxBody(k, width-2)))

	return lipgloss.JoinVertical(lipgloss.Left, sections...)
}

// jmxHeights splits the section between the table and the chart. The table is
// the primary reading, so it is satisfied first; the chart takes the rest and
// drops out entirely below a height where a shape can be read.
func jmxHeights(available, brokers int) (table, chart int) {
	const minChart = 5

	// Borders, the header, a blank line and the summary around the rows.
	want := max(brokers, 1) + 5

	switch {
	case available >= want+minChart:
		return want, available - want
	case available >= want:
		return available, 0
	default:
		return max(available, 0), 0
	}
}

// jmxCounter says how fresh the scrape is, because a stale exporter reading is
// worse than none: the numbers look current.
func (m *Model) jmxCounter() string {
	for _, broker := range m.view.JMX {
		if broker.OK() {
			return "scraped " + humanAge(time.Since(broker.At))
		}
	}
	return ""
}

func (m *Model) jmxBody(k *ktx.Context, width int) string {
	th := k.Theme

	if len(m.view.JMX) == 0 {
		if !m.view.JMXConfigured {
			return th.Dim.Render("  no JMX exporter configured for this cluster")
		}
		return th.Dim.Render("  " + m.waitingText())
	}

	// Two gauges and five numbers; the gauges take what the fixed columns
	// leave. The header is the widest line here, so it sets the budget —
	// measured rather than guessed, and held by a test at several widths.
	const fixed = 2 + 4 + 1 + 6 + 1 + 6 + 1 + 8 + 1 + 9 + 1 + 9 + 1 + 7 + 2 + 2
	gaugeWidth := max(min((width-1-fixed)/2, 16), 4)

	header := fmt.Sprintf("  %-4s %-*s %-*s %8s %9s %9s %7s  ",
		"ID", gaugeWidth+6, "HEAP", gaugeWidth+6, "IDLE", "NET IDLE", "IN", "OUT", "GC")
	lines := []string{th.Dim.Render(panel.Truncate(header, width-1))}

	for _, broker := range m.view.JMX {
		if !broker.OK() {
			// The broker keeps its row: "this exporter is unreachable" is itself
			// worth seeing, and a missing row would read as a missing broker.
			lines = append(lines, th.Critical.Render(panel.Truncate(
				fmt.Sprintf("  %-3d %s", broker.BrokerID, scrapeError(broker.Err)), width-1)))
			continue
		}

		style := th.Value
		warn := false
		switch {
		case broker.UnderReplicated > 0:
			style = th.Warning
			warn = true
		case metrics.IdlePercent(broker.HandlerIdle) < 30:
			// A handler pool with little idle time is the classic saturation
			// signal, and the reason this panel exists.
			style = th.Warning
			warn = true
		}

		// These two histories measure headroom, so they are coloured by how
		// much is left: heap by how full it is, idle by how little remains.
		// Fixed scales — a heap at 12% must not fill the row just because it
		// is the largest sample seen.
		heap := broker.HeapFraction()
		idle := metrics.IdlePercent(broker.HandlerIdle) / 100

		netIdle := "—"
		if broker.Has("network_idle") {
			netIdle = percent(metrics.IdlePercent(broker.NetworkIdle))
		}

		heapStyle := headroomStyle(th, heap)
		idleStyle := headroomStyle(th, 1-idle)
		line := style.Render(fmt.Sprintf("  %-3d ", broker.BrokerID)) +
			heapStyle.Render(m.historyCell(broker.HeapSeries, heap, gaugeWidth, 1)) +
			style.Render(fmt.Sprintf(" %-5s ", percent(heap*100))) +
			idleStyle.Render(m.historyCell(broker.IdleSeries, idle*100, gaugeWidth, 100)) +
			style.Render(fmt.Sprintf(" %-5s %8s %9s %9s %7s",
				percent(idle*100), netIdle,
				humanBytes(int64(broker.BytesIn))+"/s",
				humanBytes(int64(broker.BytesOut))+"/s",
				humanPause(broker.GCPause())))
		if warn {
			line += style.Render(" " + th.Glyph("▲", "!"))
		}
		lines = append(lines, panel.Truncate(line, width-1))
	}

	if summary := m.jmxSummary(); summary != "" {
		lines = append(lines, "", th.Dim.Render(panel.Truncate(summary, width-1)))
	}
	return lipgloss.JoinVertical(lipgloss.Left, lines...)
}

// jmxSummary is the cluster-wide line under the table: the numbers that only
// mean something added up.
func (m *Model) jmxSummary() string {
	var in, out float64
	var underReplicated, shrinks float64
	var known bool

	for _, broker := range m.view.JMX {
		if !broker.OK() {
			continue
		}
		known = true
		in += broker.BytesIn
		out += broker.BytesOut
		underReplicated += broker.UnderReplicated
		shrinks += broker.IsrShrinks
	}
	if !known {
		return ""
	}

	summary := fmt.Sprintf("  cluster in %s/s · out %s/s",
		humanBytes(int64(in)), humanBytes(int64(out)))
	if underReplicated > 0 {
		summary += fmt.Sprintf(" · %d under-replicated", int(underReplicated))
	}
	if shrinks > 0 {
		summary += fmt.Sprintf(" · %d ISR shrinks", int(shrinks))
	}
	return summary
}

// jmxChartPanel draws heap or bytes-in over the window, one line per broker in
// its series colour.
func (m *Model) jmxChartPanel(k *ktx.Context, width, height int) string {
	th := k.Theme

	title := "Heap  %"
	if m.jmxChartBytes {
		title = "Bytes in  B/s"
	}

	return panel.Panel{
		Title:    title,
		TopRight: shortWindow(windows[m.window]),
		Width:    width,
		Height:   height,
	}.Render(th, m.jmxChartBody(th, width-2, height-2))
}

func (m *Model) jmxChartBody(th *theme.Theme, width, height int) string {
	series := make([][]float64, 0, len(m.view.JMX))
	styles := make([]lipgloss.Style, 0, len(m.view.JMX))
	names := make([]string, 0, len(m.view.JMX))

	for i, broker := range m.view.JMX {
		points := broker.HeapSeries
		if m.jmxChartBytes {
			points = broker.BytesInSeries
		}
		if len(points) == 0 {
			continue
		}
		values := metrics.Values(points)
		if !m.jmxChartBytes {
			// Heap is stored as a fraction; the chart reads better in percent.
			for j := range values {
				values[j] *= 100
			}
		}
		series = append(series, values)
		styles = append(styles, th.SeriesStyle(i))
		names = append(names, fmt.Sprintf("broker %d", broker.BrokerID))
	}

	if len(series) == 0 {
		return th.Dim.Render("  " + m.waitingText())
	}

	plotHeight := max(height-2, 1) // one row for the axis, one for the legend
	lines := m.multiLineChart(th, series, styles, width, plotHeight)
	lines = append(lines, m.chartLegend(th, names, len(names), width))
	return lipgloss.JoinVertical(lipgloss.Left, lines...)
}
