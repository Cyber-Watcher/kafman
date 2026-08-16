package monitoring

// The Groups section: is the cluster's output being kept up with? A
// produce-vs-consume chart on top (or, toggled with `t`, one consume line per
// group) and a table of every group with its lag, read rate, member history
// and never-committed partitions underneath.

import (
	"fmt"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/Cyber-Watcher/kafman/internal/metrics"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/panel"
	"github.com/Cyber-Watcher/kafman/internal/ui/ktx"
	"github.com/Cyber-Watcher/kafman/internal/ui/msg"
	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

// handleGroupsKey handles what only the Groups section responds to.
func (m *Model) handleGroupsKey(key tea.KeyMsg) tea.Cmd {
	switch key.String() {
	case "t":
		m.groupsChartPerGroup = !m.groupsChartPerGroup
		return nil

	case "enter":
		if row := m.sectionGroupTable.SelectedRow(); row != nil {
			if group, ok := row.Data.(metrics.GroupSeries); ok {
				return func() tea.Msg { return msg.FocusGroup{Group: group.Name} }
			}
		}
		return nil
	}
	return m.sectionGroupTable.Update(key)
}

// groupsView is the section's whole content.
func (m *Model) groupsView(k *ktx.Context, width, height int) string {
	th := k.Theme

	chart, table := groupsHeights(height, len(m.view.Groups))

	sections := make([]string, 0, 2)
	if chart > 0 {
		sections = append(sections, m.groupsChartPanel(k, width, chart))
	}

	content := m.sectionGroupTable.View(th, width-2, table-2)
	if len(m.view.Groups) == 0 {
		content = th.Dim.Render("  " + m.waitingText())
	}
	var total int64
	for _, group := range m.view.Groups {
		total += int64(group.Last)
	}
	sections = append(sections, panel.Panel{
		Title:    "Groups",
		TopRight: "total lag " + humanCount(total),
		Focused:  m.section == sectionGroups && !m.selectorFocused,
		Width:    width,
		Height:   table,
	}.Render(th, content))

	return lipgloss.JoinVertical(lipgloss.Left, sections...)
}

// groupsHeights splits the section between the chart and the group table. The
// table is the primary reading — it is satisfied first, the chart takes the
// rest and drops out entirely below a height where a shape can be read.
func groupsHeights(available, groups int) (chart, table int) {
	const minChart = 5

	// Borders and the header around the rows, capped: past ten rows the
	// chart's shape says more than more rows would.
	want := min(max(groups, 1)+3, 13)

	switch {
	case available >= want+minChart:
		return available - want, want
	case available >= want:
		return 0, available
	default:
		return 0, max(available, 0)
	}
}

// groupsChartPanel draws produce against consume, or one consume line per
// group when toggled.
func (m *Model) groupsChartPanel(k *ktx.Context, width, height int) string {
	th := k.Theme

	title := "Produce vs consume  msg/s"
	if m.groupsChartPerGroup {
		title = "Consume by group  msg/s"
	}

	return panel.Panel{
		Title:    title,
		TopRight: m.groupsChartCounter(),
		Width:    width,
		Height:   height,
	}.Render(th, m.groupsChartBody(th, width-2, height-2))
}

// groupsChartCounter is the headline pair: what the cluster writes against
// what its groups read.
func (m *Model) groupsChartCounter() string {
	write, read := "—", "—"
	if len(m.view.Cluster) > 0 {
		write = formatRate(m.view.Cluster[len(m.view.Cluster)-1].Value)
	}
	if len(m.view.Consumed) > 0 {
		read = formatRate(m.view.Consumed[len(m.view.Consumed)-1].Value)
	}
	return fmt.Sprintf("write %s · read %s · %s", write, read, shortWindow(windows[m.window]))
}

func (m *Model) groupsChartBody(th *theme.Theme, width, height int) string {
	series := make([][]float64, 0, chartSeries)
	styles := make([]lipgloss.Style, 0, chartSeries)
	names := make([]string, 0, chartSeries)

	if m.groupsChartPerGroup {
		groups := m.view.Groups[:min(chartSeries, len(m.view.Groups))]
		for i, group := range groups {
			if len(group.Read) == 0 {
				continue
			}
			series = append(series, metrics.Values(group.Read))
			styles = append(styles, th.SeriesStyle(i))
			names = append(names, group.Name)
		}
	} else if len(m.view.Cluster) > 0 {
		series = append(series, metrics.Values(m.view.Cluster))
		styles = append(styles, th.SeriesStyle(0))
		names = append(names, "produce")
		if len(m.view.Consumed) > 0 {
			series = append(series, metrics.Values(m.view.Consumed))
			styles = append(styles, th.SeriesStyle(1))
			names = append(names, "consume")
		}
	}

	if len(series) == 0 {
		return th.Dim.Render("  " + m.waitingText())
	}

	plotHeight := max(height-2, 1) // one row for the axis, one for the legend
	lines := m.multiLineChart(th, series, styles, width, plotHeight)
	lines = append(lines, m.chartLegend(th, names, len(names), width))
	return lipgloss.JoinVertical(lipgloss.Left, lines...)
}
