// Package messages implements the Messages tab: pick topics, stream records,
// inspect one.
//
// Reading is always by direct partition assignment and never commits an offset,
// so pointing kafman at production cannot disturb a consumer group. The query
// line at the bottom is a status line in this stage; stage 7 turns it into the
// query language input.
package messages

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/atotto/clipboard"

	"github.com/Cyber-Watcher/kafman/internal/kafka"
	"github.com/Cyber-Watcher/kafman/internal/query"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/chrome"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/modal"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/panel"
	"github.com/Cyber-Watcher/kafman/internal/ui/ktx"
	"github.com/Cyber-Watcher/kafman/internal/ui/msg"
)

// Focus targets within the tab.
type focusTarget int

const (
	focusTopics focusTarget = iota
	focusRecords
	focusQuery
)

// Model is the Messages tab.
type Model struct {
	client kafka.Facade

	topics  *topicList
	records *recordList
	details *recordDetails
	produce *produceForm
	query   *queryLine

	// exportDir is where exports are written; empty means the working directory.
	exportDir string
	cluster   string

	// active is the query the running read is filtering with.
	active *query.Query

	focus       focusTarget
	hideTopics  bool
	loadingList bool
	listErr     error

	// session is the running read, nil when idle.
	session *kafka.ReadSession
	reading bool
	readErr error

	start kafka.Start
	limit int

	// followOverride is set by the f key; when nil, following is decided by the
	// start position.
	followOverride *bool

	matched int64
	read    int64

	bufferSize int
}

// New builds the tab.
func New(bufferSize int, history []string) *Model {
	m := &Model{
		topics:     newTopicList(),
		records:    newRecordList(bufferSize),
		query:      newQueryLine(history),
		bufferSize: bufferSize,
		start:      kafka.Start{Kind: kafka.StartLastN, N: 100},
	}
	// The topic panel starts focused: choosing what to read comes first.
	m.setFocus(focusTopics)
	return m
}

// Title implements ui.Tab.
func (m *Model) Title() string { return "Messages" }

// SetContext records which cluster is being read and where exports go.
func (m *Model) SetContext(cluster, exportDir string) {
	m.cluster, m.exportDir = cluster, exportDir
}

// SetClient implements ui.ClientAware.
func (m *Model) SetClient(client kafka.Facade) {
	if m.session != nil {
		m.session.Stop()
		m.session = nil
		m.reading = false
	}
	m.client = client
	if client == nil {
		m.topics.SetTopics(nil)
		m.records.Clear()
	}
}

// Init implements ui.Tab: it loads the topic list.
func (m *Model) Init() tea.Cmd {
	if m.client == nil {
		return nil
	}
	m.loadingList = true
	m.listErr = nil

	client := m.client
	return func() tea.Msg {
		topics, err := client.ListTopics(context.Background(), true)
		return msg.TopicsLoaded{Topics: topics, Err: err}
	}
}

// SetReadOptions configures the next read, overriding what the query says.
func (m *Model) SetReadOptions(start kafka.Start, follow bool, limit int) {
	m.start, m.limit = start, limit
	m.followOverride = &follow
}

// following decides whether a read keeps running once it reaches the end.
//
// Reading from the end only makes sense while following: there is nothing there
// yet. Reading from anywhere else is a search through existing data and must
// terminate, or a filtered query that matches nothing looks like a hang. The f
// key overrides the default either way.
func (m *Model) following(start kafka.Start) bool {
	if m.followOverride != nil {
		return *m.followOverride
	}
	return start.Kind == kafka.StartEnd
}

// Reading reports whether a read is in progress.
func (m *Model) Reading() bool { return m.reading }

// RecordCount is how many records are buffered.
func (m *Model) RecordCount() int { return m.records.Len() }

// AcceptsText reports whether the topic filter is capturing keystrokes, so the
// root does not treat letters as global shortcuts.
func (m *Model) AcceptsText() bool {
	if m.produce != nil || m.focus == focusQuery {
		return true
	}
	return m.focus == focusTopics && m.topics.Filtering()
}

// SelectTopics ticks exactly the named topics. Used by tests and by jumps from
// another tab.
func (m *Model) SelectTopics(names ...string) {
	m.topics.ClearSelection()
	for _, name := range names {
		m.topics.selected[name] = true
	}
	m.topics.rebuild()
}

// SetQuery replaces the query text.
func (m *Model) SetQuery(text string) { m.query.SetText(text) }

// StartReading begins a read with the current selection and query.
func (m *Model) StartReading() tea.Cmd { return m.startReading() }

// ReadCount is how many records were consumed, before filtering.
func (m *Model) ReadCount() int64 { return m.read }

// QueryHistory returns the queries run in this session, for saving to the config.
func (m *Model) QueryHistory() []string { return m.query.History() }

// Shortcuts implements ui.ShortcutProvider.
func (m *Model) Shortcuts() []chrome.Shortcut {
	if m.produce != nil {
		return m.produce.Shortcuts()
	}
	if m.details != nil {
		return m.details.Shortcuts()
	}
	if m.focus == focusTopics {
		return []chrome.Shortcut{
			{Key: "Space", Label: "Select"},
			{Key: "Enter", Label: "Read"},
			{Key: "/", Label: "Query"},
			{Key: "Ctrl+U", Label: "Clear"},
			{Key: "F4", Label: "Internal"},
			{Key: "Tab", Label: "Panel"},
		}
	}

	if m.focus == focusQuery {
		return []chrome.Shortcut{
			{Key: "Enter", Label: "Run"},
			{Key: "↑/↓", Label: "History"},
			{Key: "→", Label: "Complete"},
			{Key: "Esc", Label: "Back"},
		}
	}

	shortcuts := []chrome.Shortcut{
		{Key: "Enter", Label: "Open"},
		{Key: "/", Label: "Query"},
		{Key: "P", Label: "Produce"},
		{Key: "E", Label: "Export"},
		{Key: "C", Label: "Copy"},
		{Key: "f", Label: followLabel(m.records.Following())},
		{Key: "t", Label: "Time"},
		{Key: "r", Label: "Order"},
		{Key: "Tab", Label: "Panel"},
	}
	if m.reading {
		shortcuts = append([]chrome.Shortcut{{Key: "Esc", Label: "Stop"}}, shortcuts...)
	}
	return shortcuts
}

func followLabel(following bool) string {
	if following {
		return "Unfollow"
	}
	return "Follow  "
}

// Update implements ui.View.
func (m *Model) Update(message tea.Msg) tea.Cmd {
	switch typed := message.(type) {
	case msg.TopicsLoaded:
		m.loadingList = false
		if typed.Err != nil {
			m.listErr = typed.Err
			return nil
		}
		m.topics.SetTopics(typed.Topics)
		return nil

	case msg.FocusTopic:
		// Another tab asked for a topic: select it and start reading.
		m.topics.SelectOnly(typed.Topic)
		return m.startReading()

	case producedMsg:
		return m.onProduced(typed)

	case queryValidatedMsg:
		m.query.Validated(typed)
		return nil

	case recordsMsg:
		return m.onRecords(typed)

	case readEndedMsg:
		m.reading = false
		m.session = nil
		m.readErr = typed.Err
		if typed.Err != nil {
			return notify("read failed: "+kafka.Explain(typed.Err), msg.NotifyError)
		}
		return nil

	case tea.KeyMsg:
		return m.handleKey(typed)
	}
	return nil
}

func (m *Model) onRecords(message recordsMsg) tea.Cmd {
	kept := message.batch.Records
	if m.active.HasFilter() {
		kept = kept[:0:0]
		for _, record := range message.batch.Records {
			if m.active.Match(recordAdapter{record}) {
				kept = append(kept, record)
			}
		}
	}

	m.records.Append(kept)
	m.read = message.batch.Read
	m.matched += int64(len(kept))

	// The limit counts matching records, not records read: `limit 10` means ten
	// results, however many had to be scanned to find them.
	if m.active != nil && m.active.Limit > 0 && m.matched >= int64(m.active.Limit) {
		m.stopReading()
		return notify("limit reached", msg.NotifyInfo)
	}

	// The details overlay follows the selection, so stepping through records
	// with the arrows updates it.
	if m.details != nil {
		if selected := m.records.Selected(); selected != nil {
			m.details.SetRow(*selected)
		}
	}
	return awaitBatch(message.session)
}

func (m *Model) handleKey(key tea.KeyMsg) tea.Cmd {
	if m.produce != nil {
		return m.updateProduceForm(key)
	}

	// The details overlay is open: the tab still steers the list underneath,
	// unless the overlay has taken the arrows for scrolling the value.
	if m.details != nil {
		switch key.String() {
		case "esc":
			// Esc undoes the innermost thing: first the value focus, then the
			// overlay itself.
			if m.details.Focused() {
				m.details.Blur()
				return nil
			}
			m.details = nil
			return nil
		case "up", "down":
			if !m.details.Focused() {
				m.records.Update(key)
				if selected := m.records.Selected(); selected != nil {
					m.details.SetRow(*selected)
				}
				return nil
			}
		case "c", "C":
			return m.copySelected()
		case "e", "E":
			return m.exportSelected()
		}
		return m.details.Update(key)
	}

	switch key.String() {
	case "/", ":":
		m.setFocus(focusQuery)
		return m.query.Focus()

	case "tab", "shift+tab":
		m.toggleFocus()
		return nil

	case "ctrl+o":
		m.hideTopics = !m.hideTopics
		if m.hideTopics {
			m.setFocus(focusRecords)
		}
		return nil

	case "esc":
		// Esc always means "undo the innermost thing": the query line, then the
		// topic filter, then a running read. The root never quits on Esc.
		if m.focus == focusQuery {
			m.setFocus(focusRecords)
			return nil
		}
		if m.focus == focusTopics && m.topics.Filtering() {
			return m.topics.Update(key)
		}
		if m.reading {
			m.stopReading()
			return notify("read stopped", msg.NotifyInfo)
		}
		return nil

	case "enter":
		if m.focus == focusQuery {
			return m.startReading()
		}
		if m.focus == focusTopics {
			return m.startReading()
		}
		if selected := m.records.Selected(); selected != nil {
			m.details = newRecordDetails(*selected)
		}
		return nil

	case "s":
		if m.focus == focusRecords {
			break
		}
		m.cycleStart()
		return nil

	case "p", "P":
		if m.focus == focusQuery {
			break
		}
		return m.openProduceForm()

	case "e", "E":
		if m.focus == focusQuery {
			break
		}
		return m.exportSelected()

	case "ctrl+e":
		return m.exportAll()

	case "c", "C":
		if m.focus == focusQuery {
			break
		}
		return m.copySelected()
	}

	switch m.focus {
	case focusQuery:
		return m.query.Update(key)
	case focusTopics:
		return m.topics.Update(key)
	default:
		return m.records.Update(key)
	}
}

// toggleFocus cycles topics → records → query, skipping the topic panel when
// it is hidden.
func (m *Model) toggleFocus() {
	switch m.focus {
	case focusTopics:
		m.setFocus(focusRecords)
	case focusRecords:
		m.setFocus(focusQuery)
	default:
		if m.hideTopics {
			m.setFocus(focusRecords)
			return
		}
		m.setFocus(focusTopics)
	}
}

func (m *Model) setFocus(target focusTarget) {
	m.focus = target
	m.topics.Blur()
	m.records.Blur()
	m.query.Blur()

	switch target {
	case focusTopics:
		m.topics.Focus()
	case focusRecords:
		m.records.Focus()
	case focusQuery:
		_ = m.query.Focus()
	}
}

// cycleStart steps through the useful starting positions. Stage 7 replaces this
// with the "from" clause of the query language.
func (m *Model) cycleStart() {
	switch {
	case m.start.Kind == kafka.StartLastN && m.start.N == 100:
		m.start = kafka.Start{Kind: kafka.StartLastN, N: 1000}
	case m.start.Kind == kafka.StartLastN:
		m.start = kafka.Start{Kind: kafka.StartBeginning}
	case m.start.Kind == kafka.StartBeginning:
		m.start = kafka.Start{Kind: kafka.StartEnd}
	default:
		m.start = kafka.Start{Kind: kafka.StartLastN, N: 100}
	}
}

func (m *Model) startReading() tea.Cmd {
	if m.client == nil {
		return notify("not connected", msg.NotifyWarning)
	}
	topics := m.topics.Selected()
	if len(topics) == 0 {
		return notify("select a topic with Space first", msg.NotifyWarning)
	}

	parsed, err := m.query.Parse()
	if err != nil {
		return notify(err.Error(), msg.NotifyError)
	}

	m.stopReading()
	m.records.Clear()
	m.read, m.matched, m.readErr = 0, 0, nil
	m.active = parsed
	m.query.Remember(parsed.Source)

	if parsed.Order != nil {
		m.records.Sort(parsed.Order.Var.Name, parsed.Order.Descending)
	} else {
		m.records.Sort("", false)
	}
	m.setFocus(focusRecords)

	// A filtered read has to keep going past the limit until enough records
	// match, so the reader itself is left unbounded and the tab stops it.
	readLimit := parsed.Limit
	if parsed.HasFilter() {
		readLimit = 0
	}

	start := fromClause(parsed.From, m.start)
	session, err := m.client.ReadRecords(context.Background(), kafka.ReadOptions{
		Topics: topics,
		Start:  start,
		Follow: m.following(start),
		Limit:  readLimit,
	})
	if err != nil {
		m.readErr = err
		return notify(err.Error(), msg.NotifyError)
	}

	m.session = session
	m.reading = true
	return awaitBatch(session)
}

func (m *Model) stopReading() {
	if m.session != nil {
		m.session.Stop()
		m.session = nil
	}
	m.reading = false
}

// ---------------------------------------------------- produce, export, copy ---

func (m *Model) openProduceForm() tea.Cmd {
	if m.client == nil {
		return notify("not connected", msg.NotifyWarning)
	}

	// The topic comes from the selection, or from the record being looked at.
	topic := ""
	if selected := m.topics.Selected(); len(selected) > 0 {
		topic = selected[0]
	} else if row := m.records.Selected(); row != nil {
		topic = row.record.Topic
	}
	if topic == "" {
		return notify("select a topic first", msg.NotifyWarning)
	}

	m.produce = newProduceForm(topic)
	return nil
}

func (m *Model) updateProduceForm(key tea.KeyMsg) tea.Cmd {
	cmd := m.produce.Update(key)

	switch {
	case m.produce.Cancelled():
		m.produce = nil
		return nil

	case m.produce.Submitted():
		req, err := m.produce.Request()
		if err != nil {
			m.produce.setError(err)
			return nil
		}
		m.produce.markSending()

		client := m.client
		return func() tea.Msg {
			result, err := client.Produce(context.Background(), req)
			return producedMsg{Result: result, Err: err}
		}
	}
	return cmd
}

func (m *Model) onProduced(message producedMsg) tea.Cmd {
	if message.Err != nil {
		if m.produce != nil {
			m.produce.setError(message.Err)
		}
		return notify(kafka.Explain(message.Err), msg.NotifyError)
	}

	m.produce = nil
	return notify(fmt.Sprintf("published to %s partition %d at offset %d",
		message.Result.Topic, message.Result.Partition, message.Result.Offset),
		msg.NotifySuccess)
}

// exportSelected writes the record under the cursor.
func (m *Model) exportSelected() tea.Cmd {
	row := m.records.Selected()
	if row == nil {
		return notify("no record selected", msg.NotifyWarning)
	}
	return m.export([]kafka.Record{row.record}, "record")
}

// exportAll writes everything in the buffer.
func (m *Model) exportAll() tea.Cmd {
	records := m.records.Records()
	if len(records) == 0 {
		return notify("nothing to export", msg.NotifyWarning)
	}
	return m.export(records, fmt.Sprintf("%d records", len(records)))
}

func (m *Model) export(records []kafka.Record, what string) tea.Cmd {
	path := kafka.ExportPath(m.exportDir)
	query := ""
	if m.active != nil {
		query = m.active.Source
	}

	export := kafka.BuildExport(m.cluster, query, records)
	if err := kafka.WriteExport(path, export); err != nil {
		return notify(err.Error(), msg.NotifyError)
	}
	return notify(fmt.Sprintf("exported %s to %s", what, path), msg.NotifySuccess)
}

// copySelected puts the record on the clipboard.
func (m *Model) copySelected() tea.Cmd {
	row := m.records.Selected()
	if row == nil {
		return notify("no record selected", msg.NotifyWarning)
	}

	text := row.value.Pretty
	if m.details != nil {
		// From the details view, the whole record is more useful than the value.
		text = m.details.Text()
	}

	if err := clipboard.WriteAll(text); err != nil {
		// A headless terminal or a missing xclip is common enough that this must
		// explain itself rather than fail silently.
		return notify("could not reach the clipboard: "+err.Error(), msg.NotifyWarning)
	}
	return notify("copied to the clipboard", msg.NotifySuccess)
}

func notify(text string, level msg.NotifyLevel) tea.Cmd {
	return func() tea.Msg { return msg.Notify{Text: text, Level: level} }
}

// ------------------------------------------------------------------- view ---

// View implements ui.View.
func (m *Model) View(k *ktx.Context) string {
	th := k.Theme
	width, height := k.WindowWidth, k.AvailableHeight

	if m.client == nil {
		return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center,
			th.Dim.Render("not connected"))
	}

	const queryHeight = 4
	bodyHeight := height - queryHeight

	var body string
	if m.hideTopics {
		body = m.recordsPanel(k, width, bodyHeight)
	} else {
		// The panel is sized to the longest topic name, clamped so it neither
		// squeezes the records nor leaves a column of blank space.
		topicsWidth := min(max(m.topics.LongestName()+10, 26), width/3)
		body = lipgloss.JoinHorizontal(lipgloss.Top,
			m.topicsPanel(k, topicsWidth, bodyHeight),
			m.recordsPanel(k, width-topicsWidth, bodyHeight),
		)
	}

	frame := lipgloss.JoinVertical(lipgloss.Left, body, m.queryPanel(k, width, queryHeight))

	if m.produce != nil {
		inner := m.produce.View(k.WithHeight(height - 8))
		box := th.ModalBorder.Width(min(width-8, 100)).Render(inner)
		return modal.Overlay(frame, box)
	}
	if m.details != nil {
		inner := m.details.View(k.WithHeight(height - 8))
		border := th.ModalBorder
		if m.details.Focused() {
			// The accent border mirrors the focused-panel convention: the value
			// owns the arrow keys right now.
			border = border.BorderForeground(th.Accent)
		}
		box := border.Width(min(width-8, 100)).Render(inner)
		return modal.Overlay(frame, box)
	}
	return frame
}

// Breadcrumb lets the root show where we are inside the tab.
func (m *Model) Breadcrumb() []string {
	switch {
	case m.produce != nil:
		return []string{m.produce.Name()}
	case m.details != nil:
		return []string{m.details.Name()}
	default:
		return nil
	}
}

func (m *Model) topicsPanel(k *ktx.Context, width, height int) string {
	th := k.Theme

	content := m.topics.View(th, width-2, height-2)
	switch {
	case m.loadingList:
		content = th.Dim.Render("  loading topics…")
	case m.listErr != nil:
		content = th.Critical.Render("  " + m.listErr.Error())
	}

	return panel.Panel{
		Title:    "Topics",
		TopRight: m.topics.Counter(),
		Focused:  m.focus == focusTopics,
		Width:    width,
		Height:   height,
	}.Render(th, content)
}

func (m *Model) recordsPanel(k *ktx.Context, width, height int) string {
	th := k.Theme

	content := m.records.View(th, width-2, height-2)
	if m.records.Len() == 0 {
		content = th.Dim.Render("  " + m.emptyMessage())
	}

	return panel.Panel{
		Title:    "Records",
		TopRight: m.recordsCounter(th),
		Focused:  m.focus == focusRecords,
		Width:    width,
		Height:   height,
	}.Render(th, content)
}

func (m *Model) emptyMessage() string {
	switch {
	case m.readErr != nil:
		return m.readErr.Error()
	case m.reading:
		return "waiting for records…"
	case len(m.topics.Selected()) == 0:
		return "select a topic with Space, then press Enter"
	default:
		return "press Enter to read " + strings.Join(m.topics.Selected(), ", ")
	}
}

// recordsCounter is the "132 / 8_412  Live" text in the panel border.
func (m *Model) recordsCounter(th *ktxTheme) string {
	counter := fmt.Sprintf("%s / %s", humanCount(m.matched), humanCount(m.read))
	if m.reading {
		return counter + "  " + th.Warning.Render("Live")
	}
	return counter
}

// queryPanel draws the query input, with the parse error underneath it.
func (m *Model) queryPanel(k *ktx.Context, width, height int) string {
	th := k.Theme

	lines := []string{m.query.View(th, width-2)}
	if errLine := m.query.ErrorLine(th, width-2); errLine != "" {
		lines = append(lines, errLine)
	} else {
		lines = append(lines, th.Dim.Render(" "+m.readSummary()))
	}

	return panel.Panel{
		Title:    "Query",
		TopRight: m.queryCounter(),
		Focused:  m.focus == focusQuery,
		Width:    width,
		Height:   height,
	}.Render(th, strings.Join(lines, "\n"))
}

// readSummary describes what pressing Enter would do right now.
func (m *Model) readSummary() string {
	topics := m.topics.Selected()
	if len(topics) == 0 {
		return "select a topic with Space, then Enter to read"
	}
	summary := "Enter reads " + strings.Join(topics, ", ") + " " + startText(m.start)
	if m.following(m.start) {
		summary += ", following"
	}
	return summary
}

func (m *Model) queryCounter() string {
	if m.query.Err() != nil {
		return "invalid"
	}
	if m.active.HasFilter() {
		return "filtered"
	}
	return ""
}

func startText(start kafka.Start) string {
	switch start.Kind {
	case kafka.StartBeginning:
		return "beginning"
	case kafka.StartLastN:
		return fmt.Sprintf("end - %d", start.N)
	case kafka.StartOffset:
		return fmt.Sprintf("offset %d", start.Offset)
	case kafka.StartTime:
		return start.Time.Format(time.RFC3339)
	default:
		return "end"
	}
}
