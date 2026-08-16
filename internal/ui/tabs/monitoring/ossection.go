package monitoring

// The OS section: what node_exporter on each Kafka node reports — CPU, memory,
// load, disk and network, plus how full the filesystem holding the broker's
// data directory is. A braille chart on top (the metric cycles with `t`) and a
// per-node table underneath.

import (
	"fmt"
	"time"

	"github.com/charmbracelet/lipgloss"

	"github.com/Cyber-Watcher/kafman/internal/metrics"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/panel"
	"github.com/Cyber-Watcher/kafman/internal/ui/ktx"
	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

// osChartKind is which metric the OS chart plots; `t` cycles it.
type osChartKind int

const (
	osChartCPU osChartKind = iota
	osChartNet
	osChartDisk
	osChartCount
)

func (c osChartKind) String() string {
	switch c {
	case osChartNet:
		return "Net"
	case osChartDisk:
		return "Disk"
	default:
		return "CPU"
	}
}

// osView is the section's whole content.
func (m *Model) osView(k *ktx.Context, width, height int) string {
	th := k.Theme

	if !m.view.NodesConfigured {
		return notConfigured(th, width, height,
			"node_exporter not configured for this cluster — add a node-exporter: section to the config")
	}

	chart, table := osHeights(height, len(m.view.Nodes))

	sections := make([]string, 0, 2)
	if chart > 0 {
		sections = append(sections, m.osChartPanel(k, width, chart))
	}
	sections = append(sections, panel.Panel{
		Title:    "Nodes",
		TopRight: m.osCounter(),
		Width:    width,
		Height:   table,
	}.Render(th, m.osBody(k, width-2)))

	return lipgloss.JoinVertical(lipgloss.Left, sections...)
}

// osHeights splits the section between the chart and the node table. The
// table is the primary reading — it is satisfied first, the chart takes the
// rest and drops out entirely below a height where a shape can be read.
func osHeights(available, nodes int) (chart, table int) {
	const minChart = 5

	// Borders and the header around the rows, plus one filesystem line and a
	// blank line per node under the table.
	want := max(nodes, 1)*2 + 4

	switch {
	case available >= want+minChart:
		return available - want, want
	case available >= want:
		return 0, available
	default:
		return 0, max(available, 0)
	}
}

// osCounter says how fresh the scrape is, same as the JMX panel.
func (m *Model) osCounter() string {
	for _, node := range m.view.Nodes {
		if node.OK() {
			return "scraped " + humanAge(time.Since(node.At))
		}
	}
	return ""
}

// osChartPanel draws the selected metric over the window, one line per node in
// its series colour.
func (m *Model) osChartPanel(k *ktx.Context, width, height int) string {
	th := k.Theme

	title := "CPU  %"
	switch m.osChart {
	case osChartNet:
		title = "Network rx+tx  B/s"
	case osChartDisk:
		title = "Disk r+w  B/s"
	}

	return panel.Panel{
		Title:    title,
		TopRight: shortWindow(windows[m.window]),
		Width:    width,
		Height:   height,
	}.Render(th, m.osChartBody(th, width-2, height-2))
}

func (m *Model) osChartBody(th *theme.Theme, width, height int) string {
	series := make([][]float64, 0, len(m.view.Nodes))
	styles := make([]lipgloss.Style, 0, len(m.view.Nodes))
	names := make([]string, 0, len(m.view.Nodes))

	for i, node := range m.view.Nodes {
		values := m.osChartValues(node)
		if len(values) == 0 {
			continue
		}
		series = append(series, values)
		styles = append(styles, th.SeriesStyle(i))
		names = append(names, fmt.Sprintf("node %d", node.BrokerID))
	}

	if len(series) == 0 {
		return th.Dim.Render("  " + m.waitingText())
	}

	plotHeight := max(height-2, 1) // one row for the axis, one for the legend
	lines := m.multiLineChart(th, series, styles, width, plotHeight)
	lines = append(lines, m.chartLegend(th, names, len(names), width))
	return lipgloss.JoinVertical(lipgloss.Left, lines...)
}

// osChartValues is one node's line for the selected metric. Directions are
// summed — one line per node keeps three nodes readable where six lines would
// not be.
func (m *Model) osChartValues(node metrics.NodeStat) []float64 {
	switch m.osChart {
	case osChartNet:
		return sumSeries(node.NetRxSeries, node.NetTxSeries)
	case osChartDisk:
		return sumSeries(node.DiskReadSeries, node.DiskWriteSeries)
	default:
		return metrics.Values(node.CPUSeries)
	}
}

// sumSeries adds two point series sample by sample; the tail of the longer one
// is kept as is.
func sumSeries(a, b []metrics.Point) []float64 {
	longer, shorter := metrics.Values(a), metrics.Values(b)
	if len(shorter) > len(longer) {
		longer, shorter = shorter, longer
	}
	for i, value := range shorter {
		longer[i] += value
	}
	return longer
}

func (m *Model) osBody(k *ktx.Context, width int) string {
	th := k.Theme

	if len(m.view.Nodes) == 0 {
		return th.Dim.Render("  " + m.waitingText())
	}

	// Two gauges and the fixed columns: lead-in, id, the gauge percent labels,
	// loads, disk r/w and net rx/tx.
	const fixed = 2 + 4 + 1 + 6 + 1 + 6 + 1 + 15 + 1 + 15 + 1 + 15
	gaugeWidth := max(min((width-1-fixed)/2, 12), 4)

	header := fmt.Sprintf("  %-4s %-*s %-*s %-15s %-15s %-15s",
		"ID", gaugeWidth+6, "CPU", gaugeWidth+6, "RAM", "LOAD 1/5/15", "DISK R/W", "NET RX/TX")
	lines := []string{th.Dim.Render(panel.Truncate(header, width-1))}

	for _, node := range m.view.Nodes {
		if !node.OK() {
			// The node keeps its row: "this exporter is unreachable" is itself
			// worth seeing, and a missing row would read as a missing node.
			lines = append(lines, th.Critical.Render(panel.Truncate(
				fmt.Sprintf("  %-3d %s", node.BrokerID, scrapeError(node.Err)), width-1)))
			continue
		}

		cpu, cpuKnown := lastValue(node.CPUSeries)
		cpuLabel := "—"
		if cpuKnown {
			cpuLabel = percent(cpu)
		}

		ram := node.MemUsedFraction()
		ramLabel := "—"
		if node.Has("mem") {
			ramLabel = percent(ram * 100)
		}

		loads := "—"
		if node.Has("load") {
			loads = fmt.Sprintf("%.2f/%.2f/%.2f", node.Load1, node.Load5, node.Load15)
		}

		// One-row history sparklines on a fixed 0-100 scale, coloured by
		// headroom — the same charts the Busiest topics rows carry.
		line := th.Value.Render(fmt.Sprintf("  %-3d ", node.BrokerID)) +
			headroomStyle(th, cpu/100).Render(m.historyCell(node.CPUSeries, cpu, gaugeWidth, 100)) +
			th.Value.Render(fmt.Sprintf(" %-5s ", cpuLabel)) +
			headroomStyle(th, ram).Render(m.historyCell(node.RAMSeries, ram*100, gaugeWidth, 100)) +
			th.Value.Render(fmt.Sprintf(" %-5s %-15s %-15s %-15s",
				ramLabel, loads,
				ratePair(node.DiskReadSeries, node.DiskWriteSeries),
				ratePair(node.NetRxSeries, node.NetTxSeries)))
		lines = append(lines, panel.Truncate(line, width-1))
	}

	// One filesystem line per node: the root filesystem and the one holding
	// the broker's data directory. A gap is named, never papered over.
	lines = append(lines, "")
	for _, node := range m.view.Nodes {
		if !node.OK() {
			continue
		}
		lines = append(lines, th.Dim.Render(panel.Truncate(m.fsLine(node), width-1)))
	}

	return lipgloss.JoinVertical(lipgloss.Left, lines...)
}

// fsLine is one node's filesystem summary under the table.
func (m *Model) fsLine(node metrics.NodeStat) string {
	root := "root: —"
	if fs, ok := node.FS("/"); ok {
		root = fmt.Sprintf("root /: %s used of %s",
			percent(fs.UsedFraction()*100), humanBytes(int64(fs.SizeBytes)))
	}

	dataDir := "data dir: unknown — log.dirs could not be matched"
	if node.DataDirKnown {
		dataDir = fmt.Sprintf("data dir %s: %s used of %s",
			node.DataDirPath,
			percent(node.DataDirFS.UsedFraction()*100),
			humanBytes(int64(node.DataDirFS.SizeBytes)))
	}

	return fmt.Sprintf("  node %d   %s · %s", node.BrokerID, root, dataDir)
}

// ratePair renders the last read/write (or rx/tx) rates, or a dash while the
// counters still need their second scrape.
func ratePair(a, b []metrics.Point) string {
	av, aKnown := lastValue(a)
	bv, bKnown := lastValue(b)
	if !aKnown || !bKnown {
		return "—"
	}
	return humanBytes(int64(av)) + "/" + humanBytes(int64(bv))
}

func lastValue(points []metrics.Point) (float64, bool) {
	if len(points) == 0 {
		return 0, false
	}
	return points[len(points)-1].Value, true
}
