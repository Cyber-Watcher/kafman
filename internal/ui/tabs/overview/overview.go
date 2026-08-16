// Package overview implements the Overview tab: one screen that answers
// "is this cluster healthy right now?".
//
// The panels load independently. Cluster metadata comes back in milliseconds,
// while log-dir sizes and consumer lag take considerably longer, and waiting for
// the slowest would leave the screen blank for seconds.
package overview

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/Cyber-Watcher/kafman/internal/kafka"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/chrome"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/panel"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/table"
	"github.com/Cyber-Watcher/kafman/internal/ui/ktx"
	"github.com/Cyber-Watcher/kafman/internal/ui/msg"
	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

// How many rows the two bottom panels show.
const topN = 8

// Panels that take focus, in Tab order.
type focusTarget int

const (
	focusTopics focusTarget = iota
	focusGroups
	focusCount
)

// Model is the Overview tab.
type Model struct {
	client kafka.Facade

	info   kafka.ClusterInfo
	health kafka.Health
	topics []kafka.Topic
	groups []kafka.ConsumerGroup

	topicSizes  map[string]int64
	brokerSizes map[int32]int64

	// quorum is the KRaft metadata quorum; quorumKnown says the cluster has
	// one — a ZooKeeper cluster legitimately does not.
	quorum      kafka.QuorumInfo
	quorumKnown bool

	loading map[string]bool
	errs    map[string]error

	topicTable *table.Model
	groupTable *table.Model
	focus      focusTarget
}

// New builds the tab.
func New() *Model {
	m := &Model{
		loading:     make(map[string]bool),
		errs:        make(map[string]error),
		topicSizes:  make(map[string]int64),
		brokerSizes: make(map[int32]int64),
	}

	m.topicTable = table.New(
		table.Column{Title: "TOPIC", Width: 0, TruncateFront: true},
		table.Column{Title: "PARTS", Width: 6, Align: table.Right},
		table.Column{Title: "RF", Width: 3, Align: table.Right},
		table.Column{Title: "ISR", Width: 9, Align: table.Right},
		table.Column{Title: "SIZE", Width: 10, Align: table.Right},
	)
	m.groupTable = table.New(
		table.Column{Title: "GROUP", Width: 0, TruncateFront: true},
		table.Column{Title: "STATE", Width: 13},
		table.Column{Title: "MEMBERS", Width: 8, Align: table.Right},
		table.Column{Title: "LAG", Width: 12, Align: table.Right},
	)
	m.topicTable.Focus()
	return m
}

// Title implements ui.Tab.
func (m *Model) Title() string { return "Overview" }

// SetClient implements ui.ClientAware.
func (m *Model) SetClient(client kafka.Facade) {
	m.client = client
	if client == nil {
		// The connection is gone: drop the data rather than show stale numbers
		// as though they were current.
		m.reset()
	}
}

func (m *Model) reset() {
	m.info = kafka.ClusterInfo{}
	m.health = kafka.Health{}
	m.quorum, m.quorumKnown = kafka.QuorumInfo{}, false
	m.topics, m.groups = nil, nil
	m.topicSizes = make(map[string]int64)
	m.brokerSizes = make(map[int32]int64)
	m.loading = make(map[string]bool)
	m.errs = make(map[string]error)
	m.topicTable.SetRows(nil)
	m.groupTable.SetRows(nil)
}

// Init implements ui.Tab: it starts every panel's load at once.
func (m *Model) Init() tea.Cmd {
	if m.client == nil {
		return nil
	}

	client := m.client
	for _, key := range []string{"health", "topics", "sizes", "groups"} {
		m.loading[key] = true
		delete(m.errs, key)
	}

	return tea.Batch(
		func() tea.Msg {
			health, err := client.ClusterHealth(context.Background())
			return msg.HealthLoaded{Health: health, Err: err}
		},
		func() tea.Msg {
			topics, err := client.ListTopics(context.Background(), false)
			return msg.TopicsLoaded{Topics: topics, Err: err}
		},
		func() tea.Msg {
			topicSizes, err := client.TopicSizes(context.Background())
			if err != nil {
				return msg.SizesLoaded{Err: err}
			}
			brokerSizes, err := client.BrokerSizes(context.Background())
			return msg.SizesLoaded{Topics: topicSizes, Brokers: brokerSizes, Err: err}
		},
		func() tea.Msg {
			groups, err := client.ConsumerGroupsWithLag(context.Background())
			return msg.GroupsLoaded{Groups: groups, Err: err}
		},
		func() tea.Msg {
			quorum, err := client.DescribeQuorum(context.Background())
			return quorumLoadedMsg{Quorum: quorum, Err: err}
		},
	)
}

// quorumLoadedMsg carries the KRaft metadata quorum, or the reason there is
// none.
type quorumLoadedMsg struct {
	Quorum kafka.QuorumInfo
	Err    error
}

// Shortcuts implements ui.ShortcutProvider.
func (m *Model) Shortcuts() []chrome.Shortcut {
	return []chrome.Shortcut{
		{Key: "Tab", Label: "Panel"},
		{Key: "Enter", Label: "Open"},
		{Key: "F5", Label: "Refresh"},
	}
}

// Update implements ui.View.
func (m *Model) Update(message tea.Msg) tea.Cmd {
	switch typed := message.(type) {
	case msg.Connected:
		m.info = typed.Info
		return nil

	case msg.HealthLoaded:
		m.loading["health"] = false
		if typed.Err != nil {
			m.fail("health", typed.Err)
			return nil
		}
		m.health = typed.Health
		return nil

	case msg.TopicsLoaded:
		m.loading["topics"] = false
		if typed.Err != nil {
			m.fail("topics", typed.Err)
			return nil
		}
		m.topics = typed.Topics
		m.rebuildTopicRows()
		return nil

	case msg.SizesLoaded:
		m.loading["sizes"] = false
		if typed.Err != nil {
			m.fail("sizes", typed.Err)
			return nil
		}
		m.topicSizes, m.brokerSizes = typed.Topics, typed.Brokers
		m.rebuildTopicRows()
		return nil

	case msg.GroupsLoaded:
		m.loading["groups"] = false
		if typed.Err != nil {
			m.fail("groups", typed.Err)
			return nil
		}
		m.groups = typed.Groups
		m.rebuildGroupRows()
		return nil

	case quorumLoadedMsg:
		// No quorum is a fact about the cluster (ZooKeeper-era), not a
		// failure; a real failure keeps the roles-based view, which stays
		// honest without the quorum.
		m.quorumKnown = typed.Err == nil
		m.quorum = typed.Quorum
		if typed.Err != nil && !errors.Is(typed.Err, kafka.ErrNoQuorum) {
			slog.Debug("describe quorum failed", "error", typed.Err)
		}
		return nil

	case tea.KeyMsg:
		return m.handleKey(typed)
	}
	return nil
}

func (m *Model) handleKey(key tea.KeyMsg) tea.Cmd {
	switch key.String() {
	case "tab":
		m.setFocus((m.focus + 1) % focusCount)
		return nil
	case "shift+tab":
		m.setFocus((m.focus - 1 + focusCount) % focusCount)
		return nil

	case "enter":
		// Overview is a dashboard, so every row leads somewhere.
		switch m.focus {
		case focusTopics:
			if row := m.topicTable.SelectedRow(); row != nil {
				if topic, ok := row.Data.(kafka.Topic); ok {
					return func() tea.Msg { return msg.FocusTopic{Topic: topic.Name} }
				}
			}
		case focusGroups:
			if row := m.groupTable.SelectedRow(); row != nil {
				if group, ok := row.Data.(kafka.ConsumerGroup); ok {
					return func() tea.Msg { return msg.FocusGroup{Group: group.Name} }
				}
			}
		}
		return nil
	}

	if m.focus == focusTopics {
		return m.topicTable.Update(key)
	}
	return m.groupTable.Update(key)
}

func (m *Model) setFocus(target focusTarget) {
	m.focus = target
	m.topicTable.Blur()
	m.groupTable.Blur()
	if target == focusTopics {
		m.topicTable.Focus()
		return
	}
	m.groupTable.Focus()
}

// rebuildTopicRows shows the largest topics, since size is the only volume
// signal available before the metrics collector lands in stage 12.
func (m *Model) rebuildTopicRows() {
	topics := make([]kafka.Topic, len(m.topics))
	copy(topics, m.topics)

	// Largest first; ties broken by name so the order is stable between refreshes.
	sortBySize(topics, m.topicSizes)
	if len(topics) > topN {
		topics = topics[:topN]
	}

	rows := make([]table.Row, 0, len(topics))
	for _, topic := range topics {
		size := "—"
		if bytes, ok := m.topicSizes[topic.Name]; ok {
			size = humanBytes(bytes)
		}

		row := table.Row{
			Cells: []string{
				topic.Name,
				fmt.Sprint(topic.Partitions),
				fmt.Sprint(topic.ReplicationFactor),
				fmt.Sprintf("%d/%d", topic.InSync, topic.TotalReplicas),
				size,
			},
			Data: topic,
		}
		rows = append(rows, row)
	}
	m.topicTable.SetRows(rows)
}

func (m *Model) rebuildGroupRows() {
	groups := m.groups
	if len(groups) > topN {
		groups = groups[:topN]
	}

	rows := make([]table.Row, 0, len(groups))
	for _, group := range groups {
		lag := "—"
		if group.LagKnown {
			lag = humanCount(group.TotalLag)
		}
		rows = append(rows, table.Row{
			Cells: []string{group.Name, group.State, fmt.Sprint(group.Members), lag},
			Data:  group,
		})
	}
	m.groupTable.SetRows(rows)
}

// View implements ui.View.
func (m *Model) View(k *ktx.Context) string {
	th := k.Theme
	width, height := k.WindowWidth, k.AvailableHeight

	if m.client == nil {
		return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center,
			th.Dim.Render("not connected"))
	}

	// Three bands: summary and health, brokers, then the two top-N tables.
	summaryHeight := 10
	brokersHeight := min(len(m.info.Brokers)+3, 8)
	tablesHeight := height - summaryHeight - brokersHeight
	if tablesHeight < 6 {
		// A short terminal loses the broker list before the tables, which carry
		// more information per line.
		brokersHeight = 0
		tablesHeight = height - summaryHeight
	}

	left := width / 2
	right := width - left

	top := lipgloss.JoinHorizontal(lipgloss.Top,
		panel.Panel{Title: "Cluster", Width: left, Height: summaryHeight}.
			Render(th, m.clusterBody(th)),
		panel.Panel{Title: "Health", Width: right, Height: summaryHeight}.
			Render(th, m.healthBody(th)),
	)

	sections := []string{top}

	if brokersHeight > 0 {
		sections = append(sections, panel.Panel{
			Title:    "Brokers",
			TopRight: fmt.Sprintf("%d online", len(m.info.Brokers)),
			Width:    width,
			Height:   brokersHeight,
		}.Render(th, m.brokersBody(th, width-2)))
	}

	bottom := lipgloss.JoinHorizontal(lipgloss.Top,
		panel.Panel{
			Title:    "Largest topics",
			TopRight: m.panelStatus("topics"),
			Focused:  m.focus == focusTopics,
			Width:    left,
			Height:   tablesHeight,
		}.Render(th, m.tableBody(th, "topics", m.topicTable, left, tablesHeight, "no topics")),
		panel.Panel{
			Title:    "Consumer groups by lag",
			TopRight: m.groupsCounter(),
			Focused:  m.focus == focusGroups,
			Width:    right,
			Height:   tablesHeight,
		}.Render(th, m.tableBody(th, "groups", m.groupTable, right, tablesHeight,
			"no consumer groups on this cluster")),
	)
	sections = append(sections, bottom)

	return lipgloss.JoinVertical(lipgloss.Left, sections...)
}

// fail records a panel's failure and writes it to the log.
//
// The screen shows the reason too, but a TUI's errors scroll away with the next
// refresh; the log is where someone looks afterwards, and a panel that failed
// silently there is a panel nobody can diagnose.
func (m *Model) fail(key string, err error) {
	m.errs[key] = err
	slog.Warn("overview panel failed", "panel", key, "error", err)
}

// tableBody renders a panel's table, or says why there is nothing in it.
//
// An empty grid under a border that reads "failed" tells nobody anything: the
// reason is what the person needs, and it is the one thing the old version threw
// away.
func (m *Model) tableBody(th *theme.Theme, key string, t *table.Model, width, height int, empty string) string {
	switch {
	case m.loading[key]:
		return "  " + th.Dim.Render("loading…")
	case m.errs[key] != nil:
		return "  " + th.Critical.Render(kafka.Explain(m.errs[key]))
	case t.TotalLen() == 0:
		return "  " + th.Dim.Render(empty)
	}
	return t.View(th, width-2, height-2)
}

// panelStatus reports loading or failure in the panel border, so a slow or
// broken panel is obvious without a row of its own.
func (m *Model) panelStatus(key string) string {
	if m.loading[key] {
		return "loading…"
	}
	if m.errs[key] != nil {
		return "failed"
	}
	return ""
}

func (m *Model) groupsCounter() string {
	if status := m.panelStatus("groups"); status != "" {
		return status
	}
	var total int64
	for _, g := range m.groups {
		total += g.TotalLag
	}
	return "total " + humanCount(total)
}

func (m *Model) clusterBody(th *theme.Theme) string {
	row := func(label, value string) string {
		return "  " + th.Label.Render(padRight(label, 13)) + th.Value.Render(value)
	}

	// The controller line: the raw DescribeQuorum names the KRaft leader —
	// the one thing metadata responses lie about. Without a quorum (ZooKeeper,
	// or the call failed) the line falls back to what is honestly known: the
	// elected controller there, or the controller-role nodes on KRaft.
	label, controller := "Controller", "—"
	if m.quorumKnown {
		label = "Quorum"
		controller = fmt.Sprintf("leader node %d · epoch %d · HW %s",
			m.quorum.LeaderID, m.quorum.LeaderEpoch, humanCount(m.quorum.HighWatermark))
	} else {
		var controllers []string
		for _, b := range m.info.Brokers {
			if b.IsController {
				controllers = append(controllers, fmt.Sprint(b.ID))
				controller = fmt.Sprintf("broker %d (%s)", b.ID, b.Addr)
			}
		}
		if len(controllers) > 1 {
			controller = fmt.Sprintf("nodes %s (KRaft quorum)", strings.Join(controllers, ", "))
		}
	}

	var totalSize int64
	for _, size := range m.brokerSizes {
		totalSize += size
	}

	lines := []string{
		row("Cluster ID", orDash(m.info.ClusterID)),
		row(label, controller),
		row("Brokers", fmt.Sprint(len(m.info.Brokers))),
		row("Topics", fmt.Sprintf("%d  (%d internal)", len(m.topics), m.health.Topics-len(m.topics))),
		row("Partitions", fmt.Sprint(m.health.Partitions)),
		row("Replicas", fmt.Sprintf("%d  (RF avg %.1f)", m.health.TotalReplicas, m.health.ReplicationFactor())),
		row("Groups", m.groupSummary()),
	}
	if totalSize > 0 {
		lines = append(lines, row("On disk", humanBytes(totalSize)))
	}
	return lipgloss.JoinVertical(lipgloss.Left, lines...)
}

func (m *Model) groupSummary() string {
	if m.loading["groups"] {
		return "…"
	}
	var stable int
	for _, g := range m.groups {
		if g.State == kafka.GroupStable {
			stable++
		}
	}
	return fmt.Sprintf("%d  (%d stable)", len(m.groups), stable)
}

// healthBody is the panel that decides whether the user needs to act.
func (m *Model) healthBody(th *theme.Theme) string {
	if m.loading["health"] {
		return "  " + th.Dim.Render("loading…")
	}
	if err := m.errs["health"]; err != nil {
		return "  " + th.Critical.Render("failed: "+err.Error())
	}

	// A zero count is healthy and renders green whatever the row means; a
	// non-zero one takes the colour of its severity.
	line := func(label string, count int, marker string, severity lipgloss.Style) string {
		if count == 0 {
			return th.Success.Render(fmt.Sprintf("  %s %s%d",
				th.Glyph("●", "*"), padRight(label, 26), count))
		}
		return severity.Render(fmt.Sprintf("  %s %s%s",
			marker, padRight(label, 26), humanCount(int64(count))))
	}

	lines := []string{
		th.Success.Render(fmt.Sprintf("  %s %s%d / %d partitions",
			th.Glyph("●", "*"), padRight("In sync", 26), m.health.InSync, m.health.Partitions)),
		line("Under-replicated", m.health.UnderReplicated, th.Glyph("▲", "!"), th.Warning),
		line("Offline", m.health.Offline, th.Glyph("✖", "x"), th.Critical),
	}

	if m.health.Worst != nil {
		lines = append(lines, "",
			"  "+th.Dim.Render("worst: ")+th.Warning.Render(m.health.Worst.String()))
	} else if m.health.OK() {
		lines = append(lines, "", "  "+th.Success.Render("every partition is fully replicated"))
	}
	return lipgloss.JoinVertical(lipgloss.Left, lines...)
}

func (m *Model) brokersBody(th *theme.Theme, width int) string {
	if len(m.info.Brokers) == 0 {
		return "  " + th.Dim.Render("no brokers reported")
	}

	header := fmt.Sprintf("  %-4s %-28s %-10s %-12s %-18s %s",
		"ID", "HOST", "RACK", "ON DISK", "QUORUM", "ROLE")
	lines := []string{th.TableHeader.Render(truncate(header, width))}

	for _, b := range m.info.Brokers {
		size := "—"
		if bytes, ok := m.brokerSizes[b.ID]; ok {
			size = humanBytes(bytes)
		}
		lines = append(lines, fmt.Sprintf("  %-4d %-28s %-10s %-12s %s %s",
			b.ID, truncate(b.Addr, 28), orDash(b.Rack), size,
			m.quorumLabel(th, b.ID), roleLabel(th, b)))
	}
	return lipgloss.JoinVertical(lipgloss.Left, lines...)
}

// quorumLabel names the node's place in the metadata quorum, cut and padded
// to exactly its column: a label that runs long would shift every column
// after it. The leader is the answer the panel exists for, so it gets the
// colour.
func (m *Model) quorumLabel(th *theme.Theme, id int32) string {
	const width = 18

	fit := func(style lipgloss.Style, label string) string {
		return style.Render(padRight(truncate(label, width), width))
	}

	if !m.quorumKnown {
		return fit(th.Dim, "—")
	}

	role := m.quorum.Role(id)
	switch role {
	case "leader":
		return fit(th.Success, th.Glyph("★ ", "* ")+"leader")
	case "follower", "observer":
		label := role
		// A replica that is behind on the metadata log is worth a number.
		for _, replica := range append(m.quorum.Voters, m.quorum.Observers...) {
			if replica.ID == id && replica.Lag > 0 {
				label = fmt.Sprintf("%s (lag %s)", role, humanCount(replica.Lag))
			}
		}
		return fit(th.Value, label)
	default:
		return fit(th.Dim, "—")
	}
}

// roleLabel renders what the node actually is. On KRaft it is the node's own
// process.roles; on a ZooKeeper cluster the elected controller is marked. The
// KRaft quorum leader is not exposed to clients, so it is never claimed.
func roleLabel(th *theme.Theme, b kafka.Broker) string {
	if b.Roles != "" {
		label := strings.ReplaceAll(b.Roles, ",", "+")
		if b.IsController {
			return th.Success.Render(th.Glyph("◆ ", "* ") + label)
		}
		return th.Value.Render(label)
	}
	if b.IsController {
		return th.Success.Render(th.Glyph("◆ ", "* ") + "controller")
	}
	return th.Value.Render("broker")
}
