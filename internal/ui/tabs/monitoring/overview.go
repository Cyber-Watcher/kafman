package monitoring

// The Overview section: throughput, busiest topics, broker load and consumer
// lag — everything the Kafka API can answer on its own.

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/Cyber-Watcher/kafman/internal/metrics"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/panel"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/spark"
	"github.com/Cyber-Watcher/kafman/internal/ui/ktx"
	"github.com/Cyber-Watcher/kafman/internal/ui/msg"
	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

// overviewChartKind is which chart the Overview throughput panel draws; `t`
// cycles it.
type overviewChartKind int

const (
	chartCluster overviewChartKind = iota
	chartByTopic
	chartRetention
	overviewChartCount
)

func (c overviewChartKind) String() string {
	switch c {
	case chartByTopic:
		return "By topic"
	case chartRetention:
		return "Retention"
	default:
		return "Cluster"
	}
}

// handleOverviewKey handles what only the Overview section responds to.
func (m *Model) handleOverviewKey(key tea.KeyMsg) tea.Cmd {
	switch key.String() {
	case "t":
		m.overviewChart = (m.overviewChart + 1) % overviewChartCount
		return nil

	case "enter":
		// The dashboard is also a launchpad: a topic opens in Messages, a
		// group in Manage, the same jumps Overview makes.
		if m.focus == focusGroups {
			if row := m.groupTable.SelectedRow(); row != nil {
				if group, ok := row.Data.(metrics.GroupSeries); ok {
					return func() tea.Msg { return msg.FocusGroup{Group: group.Name} }
				}
			}
			return nil
		}
		if row := m.topicTable.SelectedRow(); row != nil {
			if topic, ok := row.Data.(metrics.NamedSeries); ok {
				return func() tea.Msg { return msg.FocusTopic{Topic: topic.Name} }
			}
		}
		return nil
	}

	if m.focus == focusGroups {
		return m.groupTable.Update(key)
	}
	return m.topicTable.Update(key)
}

// overviewView stacks the three Overview panels.
func (m *Model) overviewView(k *ktx.Context, width, height int) string {
	chart, rates, groups := panelHeights(height)

	sections := make([]string, 0, 3)
	sections = append(sections, m.throughputPanel(k, width, chart))

	if rates > 0 {
		middleWidth := width / 2
		sections = append(sections, lipgloss.JoinHorizontal(lipgloss.Top,
			m.topicsPanel(k, middleWidth, rates),
			m.brokersPanel(k, width-middleWidth, rates),
		))
	}
	if groups > 0 {
		sections = append(sections, m.groupsPanel(k, width, groups))
	}

	return lipgloss.JoinVertical(lipgloss.Left, sections...)
}

// panelHeights splits the available rows between the chart and the two lists.
//
// The three panels want 24 rows between them and a terminal does not always
// have that many. Rather than overflowing — which pushes the status bar off the
// screen — the lists shrink to their minimum, and then drop out entirely: a
// panel two rows tall shows a border and nothing else, which is worse than not
// being there. The chart is kept longest because it is the one thing on this tab
// that cannot be read anywhere else.
func panelHeights(available int) (chart, rates, groups int) {
	const (
		wantChart  = 6
		wantList   = 9
		minList    = 6
		minChart   = 5
		panelFloor = 4 // a border, a header and two rows
	)

	switch {
	case available >= wantChart+2*wantList:
		// Everything fits; the extra rows grow the chart.
		return available - 2*wantList, wantList, wantList

	case available >= minChart+2*minList:
		// Split what is left after the chart's minimum between the two lists.
		spare := available - minChart
		rates = spare / 2
		return available - rates - (spare - rates), rates, spare - rates

	case available >= minChart+minList:
		// Only one list fits: consumer lag is the one an operator watches.
		return available - minList, 0, minList

	case available >= panelFloor:
		return available, 0, 0

	default:
		return max(available, 0), 0, 0
	}
}

// throughputPanel is the message rate over the window: the cluster total as a
// filled area, or — toggled with `t` — one line per busy topic.
func (m *Model) throughputPanel(k *ktx.Context, width, height int) string {
	th := k.Theme

	title := "Throughput  msg/s"
	body := m.chartBody(k, width-2, height-2)
	switch m.overviewChart {
	case chartByTopic:
		title = "Throughput by topic  msg/s"
		body = m.topicsChartBody(k, width-2, height-2)
	case chartRetention:
		title = "Retention deletes by topic  rec/s"
		body = m.retentionChartBody(k, width-2, height-2)
	}
	return panel.Panel{
		Title:    title,
		TopRight: m.throughputCounter(),
		Width:    width,
		Height:   height,
	}.Render(th, body)
}

func (m *Model) throughputCounter() string {
	if !m.view.Ready() {
		return shortWindow(windows[m.window])
	}
	last := m.view.Cluster[len(m.view.Cluster)-1].Value
	return fmt.Sprintf("now %s · %s", formatRate(last), shortWindow(windows[m.window]))
}

func (m *Model) chartBody(k *ktx.Context, width, height int) string {
	th := k.Theme

	if !m.view.Ready() {
		return th.Dim.Render("  " + m.waitingText())
	}

	values := metrics.Values(m.view.Cluster)
	peak := metrics.Max(m.view.Cluster)

	// The y-axis labels are as wide as the largest number they have to carry.
	labels := axisLabels(peak, height-1)
	labelWidth := 0
	for _, label := range labels {
		labelWidth = max(labelWidth, len(label))
	}

	plotWidth := max(width-labelWidth-2, 8)
	rows := spark.Chart(values, plotWidth, height-1, true)
	if !m.plain {
		rows = spark.BrailleChart(values, plotWidth, height-1)
	}

	lines := make([]string, 0, height)
	for i, row := range rows {
		lines = append(lines,
			th.Dim.Render(pad(labels[i], labelWidth)+" "+th.Glyph("┤", "|"))+
				th.Success.Render(row))
	}

	// The time axis: the window's span across the plot.
	axis := th.Dim.Render(strings.Repeat(" ", labelWidth+1) + timeAxis(windows[m.window], plotWidth))
	return lipgloss.JoinVertical(lipgloss.Left, append(lines, axis)...)
}

// chartSeries is how many topics the per-topic chart plots: enough to see who
// is busy, few enough that the lines stay tellable apart.
const chartSeries = 5

// topicsChartBody draws the busiest topics as one dotted line each, sharing a
// zero-based scale, with a legend underneath naming the lines in their
// colours.
func (m *Model) topicsChartBody(k *ktx.Context, width, height int) string {
	th := k.Theme

	if !m.view.Ready() || len(m.view.Topics) == 0 {
		return th.Dim.Render("  " + m.waitingText())
	}

	topics := m.view.Topics[:min(chartSeries, len(m.view.Topics))]
	series := make([][]float64, len(topics))
	styles := make([]lipgloss.Style, len(topics))
	names := make([]string, len(topics))
	for i, topic := range topics {
		series[i] = metrics.Values(topic.Points)
		styles[i] = th.SeriesStyle(i)
		names[i] = topic.Name
	}

	plotHeight := max(height-2, 1) // one row for the axis, one for the legend
	lines := m.multiLineChart(th, series, styles, width, plotHeight)
	lines = append(lines, m.chartLegend(th, names, len(topics), width))
	return lipgloss.JoinVertical(lipgloss.Left, lines...)
}

// retentionChartBody draws the deletion rate of the busiest topics — how fast
// retention (or compaction) advances each topic's oldest offset.
func (m *Model) retentionChartBody(k *ktx.Context, width, height int) string {
	th := k.Theme

	topics := m.view.Topics[:min(chartSeries, len(m.view.Topics))]
	series := make([][]float64, 0, len(topics))
	styles := make([]lipgloss.Style, 0, len(topics))
	names := make([]string, 0, len(topics))
	for i, topic := range topics {
		if len(topic.Head) == 0 {
			continue
		}
		series = append(series, metrics.Values(topic.Head))
		styles = append(styles, th.SeriesStyle(i))
		names = append(names, topic.Name)
	}

	if len(series) == 0 {
		return th.Dim.Render("  " + m.waitingText())
	}

	plotHeight := max(height-2, 1) // one row for the axis, one for the legend
	lines := m.multiLineChart(th, series, styles, width, plotHeight)
	lines = append(lines, m.chartLegend(th, names, len(names), width))
	return lipgloss.JoinVertical(lipgloss.Left, lines...)
}

// chartLegend names the plotted lines in their colours. Entries that do not
// fit are counted rather than silently dropped.
func (m *Model) chartLegend(th *theme.Theme, names []string, total, width int) string {
	marker := th.Glyph("──", "--")

	var b strings.Builder
	used, shown := 1, 0
	b.WriteString(" ")
	for i, name := range names {
		entry := marker + " " + name
		cost := lipgloss.Width(entry) + 2
		if used+cost > width && shown > 0 {
			break
		}
		b.WriteString("  ")
		b.WriteString(th.SeriesStyle(i).Render(entry))
		used += cost
		shown++
	}
	if rest := total - shown; rest > 0 {
		b.WriteString(th.Dim.Render(fmt.Sprintf("  +%d", rest)))
	}
	return b.String()
}

// topicsPanel lists the busiest topics with their own sparklines.
func (m *Model) topicsPanel(k *ktx.Context, width, height int) string {
	th := k.Theme

	content := m.topicTable.View(th, width-2, height-2)
	if len(m.view.Topics) == 0 {
		content = th.Dim.Render("  " + m.waitingText())
	}

	return panel.Panel{
		Title:    "Busiest topics",
		TopRight: itoa(len(m.view.Topics)),
		Focused:  m.focus == focusTopics && !m.selectorFocused,
		Width:    width,
		Height:   height,
	}.Render(th, content)
}

// brokersPanel shows what each broker carries and how fast the cluster's disks
// are filling.
func (m *Model) brokersPanel(k *ktx.Context, width, height int) string {
	th := k.Theme

	body := m.brokersBody(k, width-2)
	return panel.Panel{
		Title:    "Brokers",
		TopRight: m.writtenCounter(),
		Width:    width,
		Height:   height,
	}.Render(th, body)
}

// writtenCounter is the disk growth rate, always marked as an estimate: it is
// the sum over every replica and includes compaction and retention, which is not
// the same as what producers sent.
func (m *Model) writtenCounter() string {
	if len(m.view.Written) == 0 {
		return ""
	}
	last := m.view.Written[len(m.view.Written)-1].Value
	return "disk ≈ " + humanBytes(int64(last)) + "/s"
}

func (m *Model) brokersBody(k *ktx.Context, width int) string {
	th := k.Theme

	if len(m.view.Brokers) == 0 {
		return th.Dim.Render("  " + m.waitingText())
	}

	// Gauges are relative to the busiest broker: the question this panel answers
	// is whether the load is even, not what the absolute maximum could be.
	var peakLeaders, peakBytes int64
	for _, broker := range m.view.Brokers {
		peakLeaders = max(peakLeaders, int64(broker.Leaders))
		peakBytes = max(peakBytes, broker.Bytes)
	}

	// Everything but the two gauges is fixed width, so they take what is left,
	// split evenly and never below a width where a bar still means something.
	// Everything that is not a gauge: two leading spaces, the marker, the id,
	// the counts, the size, the replica count and the separators between them —
	// plus two columns held back for the "behind" marker, so a row does not grow
	// exactly when it has something to say.
	const fixed = 2 + 1 + 3 + 1 + 5 + 1 + 9 + 1 + 5 + 4 + 2
	gaugeWidth := max(min((width-1-fixed)/2, 16), 4)

	header := fmt.Sprintf("  %-4s %-*s %-*s %5s  ",
		"ID", gaugeWidth+6, "LEADERS", gaugeWidth+10, "ON DISK", "REPL")
	lines := []string{th.Dim.Render(panel.Truncate(header, width-1))}

	for i, broker := range m.view.Brokers {
		marker := " "
		if broker.IsController {
			marker = th.Glyph("◆", "*")
		}

		style := th.Value
		if broker.OutOfSync > 0 {
			// A broker holding replicas that are behind is the one thing on this
			// panel worth spotting without reading the numbers.
			style = th.Warning
		}

		// The pieces are styled one by one: the gauges carry the broker's
		// series colour, and a nested style inside one whole-line render would
		// cut the outer style short at its reset.
		line := style.Render(fmt.Sprintf("  %s%-3d ", marker, broker.ID)) +
			m.gauge(fraction(int64(broker.Leaders), peakLeaders), gaugeWidth, th.SeriesStyle(i), th) +
			style.Render(fmt.Sprintf(" %-5d ", broker.Leaders)) +
			m.gauge(fraction(broker.Bytes, peakBytes), gaugeWidth, th.SeriesStyle(i), th) +
			style.Render(fmt.Sprintf(" %-9s %5d", humanBytes(broker.Bytes), broker.Replicas))
		if broker.OutOfSync > 0 {
			line += style.Render(" " + th.Glyph("▲", "!"))
		}
		lines = append(lines, panel.Truncate(line, width-1))
	}

	// Replication health as a trend: the two counts that say an election or a
	// rebalance of leadership is due. Coloured only while non-zero — a zero is
	// good news and stays dim.
	if len(m.view.URP) > 0 || len(m.view.NotPreferred) > 0 {
		lines = append(lines, "", panel.Truncate(m.replicationLine(th), width-1))
	}

	// Heap, GC and the idle ratios come from an exporter, not from Kafka's own
	// API; the panel says where they are rather than leaving a gap.
	note := "  heap, GC and handler idle need a JMX exporter — see jmx: in the config"
	if m.view.JMXConfigured {
		note = "  heap, GC and handler idle are in the JMX section (Tab, then ↑/↓)"
	}
	lines = append(lines, th.Dim.Render(panel.Truncate(note, width-1)))
	return lipgloss.JoinVertical(lipgloss.Left, lines...)
}

// replicationLine is the URP and not-preferred-leader trend under the broker
// table.
func (m *Model) replicationLine(th *theme.Theme) string {
	half := func(label string, points []metrics.Point) string {
		current := 0.0
		if len(points) > 0 {
			current = points[len(points)-1].Value
		}
		style := th.Dim
		if current > 0 {
			style = th.Warning
		}
		spark := m.historyCell(points, current, 8, max(metrics.Max(points), 1))
		return style.Render(fmt.Sprintf("%s %d ", label, int(current))) + style.Render(spark)
	}

	return "  " + half("replication: URP", m.view.URP) +
		th.Dim.Render(" · ") + half("not-preferred leaders", m.view.NotPreferred)
}

// groupsPanel is consumer lag with its trend.
func (m *Model) groupsPanel(k *ktx.Context, width, height int) string {
	th := k.Theme

	content := m.groupTable.View(th, width-2, height-2)
	if len(m.view.Groups) == 0 {
		content = th.Dim.Render("  " + m.waitingText())
	}

	var total int64
	for _, group := range m.view.Groups {
		total += int64(group.Last)
	}

	return panel.Panel{
		Title:    "Consumer lag",
		TopRight: "total " + humanCount(total),
		Focused:  m.focus == focusGroups && !m.selectorFocused,
		Width:    width,
		Height:   height,
	}.Render(th, content)
}
