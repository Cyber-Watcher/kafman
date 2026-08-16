// Package monitoring implements the Monitoring tab: a live dashboard of what
// the cluster is doing right now.
//
// The tab is split into sections behind a Manage-style selector: Overview is
// the Kafka-API dashboard, JMX what the brokers' exporters report, OS what
// node_exporter on the nodes reports. Every number here comes from a source
// that is named, and every one that is an estimate says so. A collector polls
// in the background on its own goroutine; this tab only draws what it has and
// asks for a redraw when a poll lands. All sections are views over the same
// collector snapshot.
package monitoring

import (
	"context"
	"fmt"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/Cyber-Watcher/kafman/internal/kafka"
	"github.com/Cyber-Watcher/kafman/internal/metrics"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/chrome"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/panel"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/table"
	"github.com/Cyber-Watcher/kafman/internal/ui/ktx"
	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

// windows are the spans `+` and `-` cycle through.
var windows = []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute, time.Hour}

// defaultWindow is where the tab starts: long enough to show a shape, short
// enough that a change is visible while watching.
const defaultWindow = 1

// section is one entry of the left-hand selector.
type section int

const (
	sectionOverview section = iota
	sectionGroups
	sectionJMX
	sectionOS
	sectionCount
)

func (s section) String() string {
	switch s {
	case sectionGroups:
		return "Groups"
	case sectionJMX:
		return "JMX"
	case sectionOS:
		return "OS"
	default:
		return "Overview"
	}
}

// sectionsWidth is the selector panel's width. Narrower than Manage's list:
// the longest entry here is "Overview", and the charts want every column.
const sectionsWidth = 14

// Panels inside the Overview section that take focus, in Tab order.
type focusTarget int

const (
	focusTopics focusTarget = iota
	focusGroups
)

// Model is the Monitoring tab.
type Model struct {
	client    kafka.Facade
	collector *metrics.Collector
	interval  time.Duration

	view   metrics.View
	window int

	section         section
	selectorFocused bool

	topicTable *table.Model
	groupTable *table.Model
	focus      focusTarget

	// sectionGroupTable is the Groups section's own table — sharing the
	// Overview lag table would tangle the two sections' focus rings.
	sectionGroupTable *table.Model

	// overviewChart is which Overview throughput chart is on: the cluster
	// total, one line per busy topic, or the retention deletes; `t` cycles.
	overviewChart overviewChartKind

	// groupsChartPerGroup swaps the Groups chart from produce-vs-consume
	// totals to one consume line per group; `t` toggles it there.
	groupsChartPerGroup bool

	// jmxChartBytes swaps the JMX section's chart from heap to bytes-in; `t`.
	jmxChartBytes bool

	// osChart cycles the OS section's chart metric; `t`.
	osChart osChartKind

	plain bool

	// theme is what View last rendered with. Rows are built in Update, where
	// the theme is out of reach, so it is remembered here for the series
	// colours, the same way plain is.
	theme *theme.Theme
}

// New builds the tab. The interval comes from the configuration, because the
// collector's cost is paid by the cluster and only the operator knows what it
// can afford.
func New(interval time.Duration) *Model {
	m := &Model{interval: interval, window: defaultWindow}

	m.topicTable = table.New(
		table.Column{Title: "TOPIC", Width: 0, TruncateFront: true},
		table.Column{Title: "MSG/S", Width: 9, Align: table.Right},
		table.Column{Title: "RETAINED", Width: 9, Align: table.Right},
		table.Column{Title: "LAST " + shortWindow(windows[defaultWindow]), Width: 22},
	)
	m.groupTable = table.New(
		table.Column{Title: "GROUP", Width: 0, TruncateFront: true},
		table.Column{Title: "LAG", Width: 10, Align: table.Right},
		table.Column{Title: "TREND", Width: 20},
		table.Column{Title: "", Width: 11},
		table.Column{Title: "STATE", Width: 13},
	)
	m.sectionGroupTable = table.New(
		table.Column{Title: "GROUP", Width: 0, TruncateFront: true},
		table.Column{Title: "LAG", Width: 10, Align: table.Right},
		table.Column{Title: "READ/S", Width: 8, Align: table.Right},
		table.Column{Title: "MEMBERS", Width: 11},
		table.Column{Title: "TREND", Width: 12},
		table.Column{Title: "NO COMMIT", Width: 9, Align: table.Right},
		table.Column{Title: "STATE", Width: 13},
	)
	m.topicTable.Focus()
	return m
}

// Title implements ui.Tab.
func (m *Model) Title() string { return "Monitoring" }

// SetClient implements ui.ClientAware. It owns the collector's lifetime: one per
// connection, stopped when the connection goes.
func (m *Model) SetClient(client kafka.Facade) {
	m.stopCollector()

	m.client = client
	m.view = metrics.View{}
	m.topicTable.SetRows(nil)
	m.groupTable.SetRows(nil)
	m.sectionGroupTable.SetRows(nil)
	m.section = sectionOverview
	m.selectorFocused = false
	m.focus = focusTopics
	m.applyFocus()

	if client != nil {
		m.collector = metrics.NewCollector(client, m.interval)
	}
}

func (m *Model) stopCollector() {
	if m.collector != nil {
		m.collector.Stop()
		m.collector = nil
	}
}

// Close stops the background poller. The root calls it on shutdown.
func (m *Model) Close() { m.stopCollector() }

// Init starts the collector and begins waiting for its updates.
func (m *Model) Init() tea.Cmd {
	if m.collector == nil {
		return nil
	}
	m.collector.Start(context.Background())
	return m.await()
}

// await blocks on the collector's channel inside a command, the same recursive
// pattern the record stream uses: one command per update, re-armed each time.
func (m *Model) await() tea.Cmd {
	collector := m.collector
	if collector == nil {
		return nil
	}
	return func() tea.Msg {
		if _, ok := <-collector.Updates(); !ok {
			return nil
		}
		return polledMsg{}
	}
}

// polledMsg says a poll finished; the data is fetched from the collector.
type polledMsg struct{}

// Update implements ui.View.
func (m *Model) Update(message tea.Msg) tea.Cmd {
	switch typed := message.(type) {
	case polledMsg:
		m.refresh()
		return m.await()

	case tea.KeyMsg:
		return m.handleKey(typed)
	}
	return nil
}

func (m *Model) handleKey(key tea.KeyMsg) tea.Cmd {
	// Keys that mean the same thing everywhere on this tab.
	switch key.String() {
	case "tab":
		m.cycleFocus(true)
		return nil

	case "shift+tab":
		m.cycleFocus(false)
		return nil

	case "+", "=":
		if m.window < len(windows)-1 {
			m.window++
			m.refresh()
		}
		return nil

	case "-", "_":
		if m.window > 0 {
			m.window--
			m.refresh()
		}
		return nil

	case "f5":
		if m.collector != nil {
			m.collector.Refresh()
		}
		return nil
	}

	if m.selectorFocused {
		switch key.String() {
		case "up", "k":
			m.section = (m.section - 1 + sectionCount) % sectionCount
		case "down", "j":
			m.section = (m.section + 1) % sectionCount
		case "enter":
			m.selectorFocused = false
			m.applyFocus()
		}
		return nil
	}

	switch m.section {
	case sectionGroups:
		return m.handleGroupsKey(key)
	case sectionJMX:
		if key.String() == "t" {
			m.jmxChartBytes = !m.jmxChartBytes
		}
		return nil
	case sectionOS:
		if key.String() == "t" {
			m.osChart = (m.osChart + 1) % osChartCount
		}
		return nil
	default:
		return m.handleOverviewKey(key)
	}
}

// cycleFocus is the Tab ring. Overview cycles its two tables and the selector;
// the other sections have no focusable content, so Tab flips to the selector
// and back.
func (m *Model) cycleFocus(forward bool) {
	if m.section != sectionOverview {
		m.selectorFocused = !m.selectorFocused
		m.applyFocus()
		return
	}

	switch {
	case m.selectorFocused:
		m.selectorFocused = false
		if forward {
			m.focus = focusTopics
		} else {
			m.focus = focusGroups
		}
	case m.focus == focusTopics:
		if forward {
			m.focus = focusGroups
		} else {
			m.selectorFocused = true
		}
	default: // focusGroups
		if forward {
			m.selectorFocused = true
		} else {
			m.focus = focusTopics
		}
	}
	m.applyFocus()
}

func (m *Model) applyFocus() {
	m.topicTable.Blur()
	m.groupTable.Blur()
	m.sectionGroupTable.Blur()
	if m.selectorFocused {
		return
	}
	switch m.section {
	case sectionOverview:
		if m.focus == focusGroups {
			m.groupTable.Focus()
			return
		}
		m.topicTable.Focus()
	case sectionGroups:
		m.sectionGroupTable.Focus()
	}
}

// refresh takes a snapshot from the collector and rebuilds the tables.
func (m *Model) refresh() {
	if m.collector == nil {
		return
	}
	m.view = m.collector.View(windows[m.window])
	m.rebuild()
}

func (m *Model) rebuild() {
	const sparkWidth = 20

	rows := make([]table.Row, 0, len(m.view.Topics))
	for i, topic := range m.view.Topics {
		retained := "—"
		if topic.RetainedKnown {
			retained = humanCount(topic.Retained)
		}
		row := table.Row{
			Cells: []string{
				topic.Name,
				formatRate(topic.Last),
				retained,
				m.sparkline(metrics.Values(topic.Points), sparkWidth),
			},
			Data: topic,
		}
		row.CellStyles = m.seriesCell(i, 3, len(row.Cells))
		rows = append(rows, row)
	}
	m.topicTable.SetRows(rows)

	rows = make([]table.Row, 0, len(m.view.Groups))
	for i, group := range m.view.Groups {
		row := table.Row{
			Cells: []string{
				group.Name,
				humanCount(int64(group.Last)),
				m.sparkline(metrics.Values(group.Points), 18),
				trendLabel(group.Trend, m.plain),
				group.State,
			},
			Data: group,
		}
		row.CellStyles = m.seriesCell(i, 2, len(row.Cells))
		if group.Trend == metrics.TrendGrowing && group.Last > 0 {
			style := warningStyle
			row.Style = &style
		}
		rows = append(rows, row)
	}
	m.groupTable.SetRows(rows)

	rows = make([]table.Row, 0, len(m.view.Groups))
	for _, group := range m.view.Groups {
		read := "—"
		if group.ReadKnown {
			read = formatRate(group.ReadLast)
		}
		members := fmt.Sprintf("%3d %s", group.MembersNow,
			m.sparkline(metrics.Values(group.Members), 6))
		noCommit := ""
		if group.NoCommit > 0 {
			noCommit = itoa(group.NoCommit)
		}
		row := table.Row{
			Cells: []string{
				group.Name,
				humanCount(int64(group.Last)),
				read,
				members,
				trendLabel(group.Trend, m.plain),
				noCommit,
				group.State,
			},
			Data: group,
		}
		if group.NoCommit > 0 || (group.Trend == metrics.TrendGrowing && group.Last > 0) {
			style := warningStyle
			row.Style = &style
		}
		rows = append(rows, row)
	}
	m.sectionGroupTable.SetRows(rows)
}

// AwaitUpdate returns a command that waits for the collector's next poll and
// hands it to Update.
//
// The event loop always has exactly one of these in flight — Init arms the
// first, and every polledMsg arms the next — which is why F5 only pokes the
// collector rather than arming another: two waiters would each consume an
// update and re-arm, doubling on every poll. An end-to-end test drives this
// explicitly, because it has no event loop to do it.
func (m *Model) AwaitUpdate() tea.Cmd { return m.await() }

// Groups is the consumer lag the tab last drew, which is what an end-to-end
// test checks when it asks whether a trend reached the screen.
func (m *Model) Groups() []metrics.GroupSeries { return m.view.Groups }

// JMX is what the exporters reported in the last poll, which is what an
// end-to-end test checks when it asks whether the scrape reached the screen.
func (m *Model) JMX() []metrics.JMXStat { return m.view.JMX }

// Nodes is what node_exporter reported in the last poll, for the same
// end-to-end checks.
func (m *Model) Nodes() []metrics.NodeStat { return m.view.Nodes }

// Rate is the cluster message rate the tab last drew, which is what an
// end-to-end test checks when it asks whether the dashboard is moving.
func (m *Model) Rate() float64 {
	if len(m.view.Cluster) == 0 {
		return 0
	}
	return m.view.Cluster[len(m.view.Cluster)-1].Value
}

// ConsumeRate is the cluster-wide read rate the tab last drew, the other half
// of the same end-to-end check.
func (m *Model) ConsumeRate() float64 {
	if len(m.view.Consumed) == 0 {
		return 0
	}
	return m.view.Consumed[len(m.view.Consumed)-1].Value
}

// Shortcuts implements ui.ShortcutProvider.
func (m *Model) Shortcuts() []chrome.Shortcut {
	if m.selectorFocused {
		return []chrome.Shortcut{
			{Key: "↑/↓", Label: "Section"},
			{Key: "Tab", Label: "Content"},
		}
	}

	common := []chrome.Shortcut{
		{Key: "+/-", Label: "Window " + shortWindow(windows[m.window])},
		{Key: "F5", Label: "Poll now"},
	}

	switch m.section {
	case sectionGroups:
		chart := "By group"
		if m.groupsChartPerGroup {
			chart = "Totals"
		}
		return append([]chrome.Shortcut{
			{Key: "Tab", Label: "Sections"},
			{Key: "Enter", Label: "Open in Manage"},
			{Key: "t", Label: chart},
		}, common...)

	case sectionJMX:
		chart := "Bytes in"
		if m.jmxChartBytes {
			chart = "Heap"
		}
		return append([]chrome.Shortcut{
			{Key: "Tab", Label: "Sections"},
			{Key: "t", Label: chart},
		}, common...)

	case sectionOS:
		return append([]chrome.Shortcut{
			{Key: "Tab", Label: "Sections"},
			{Key: "t", Label: ((m.osChart + 1) % osChartCount).String()},
		}, common...)

	default:
		open := "Open in Messages"
		if m.focus == focusGroups {
			open = "Open in Manage"
		}
		return append([]chrome.Shortcut{
			{Key: "Tab", Label: "Panel"},
			{Key: "Enter", Label: open},
			{Key: "t", Label: ((m.overviewChart + 1) % overviewChartCount).String()},
		}, common...)
	}
}

// ------------------------------------------------------------------- view ---

// View implements ui.View.
func (m *Model) View(k *ktx.Context) string {
	th := k.Theme
	width, height := k.WindowWidth, k.AvailableHeight

	if m.plain != th.PlainFonts || m.theme != th {
		m.plain = th.PlainFonts
		m.theme = th
		m.rebuild()
	}

	if m.client == nil {
		return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center,
			th.Dim.Render("not connected"))
	}

	sections := panel.Panel{
		Title:   "Sections",
		Focused: m.selectorFocused,
		Width:   sectionsWidth,
		Height:  height,
	}.Render(th, m.sectionList(k))

	contentWidth := width - sectionsWidth
	var content string
	switch m.section {
	case sectionGroups:
		content = m.groupsView(k, contentWidth, height)
	case sectionJMX:
		content = m.jmxView(k, contentWidth, height)
	case sectionOS:
		content = m.osView(k, contentWidth, height)
	default:
		content = m.overviewView(k, contentWidth, height)
	}

	return lipgloss.JoinHorizontal(lipgloss.Top, sections, content)
}

func (m *Model) sectionList(k *ktx.Context) string {
	th := k.Theme

	lines := make([]string, 0, int(sectionCount))
	for s := section(0); s < sectionCount; s++ {
		marker := "  "
		if s == m.section {
			marker = th.Glyph(" ▸", " >")
		}

		line := marker + " " + s.String()
		switch {
		case s == m.section && m.selectorFocused:
			line = th.TableRowSelected.Render(line)
		case s == m.section:
			line = th.Highlight.Render(line)
		default:
			line = th.Dim.Render(line)
		}
		lines = append(lines, line)
	}
	return lipgloss.JoinVertical(lipgloss.Left, lines...)
}
