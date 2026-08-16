package manage

import (
	"context"
	"fmt"
	"strconv"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/Cyber-Watcher/kafman/internal/kafka"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/chrome"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/modal"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/panel"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/table"
	"github.com/Cyber-Watcher/kafman/internal/ui/ktx"
	"github.com/Cyber-Watcher/kafman/internal/ui/msg"
)

// mode is what the topics section is showing.
type mode int

const (
	modeList mode = iota
	modeCreate
	modeConfig
	modeConfirmDelete
	modeConfirmTruncate
	modeAddPartitions
)

// topicsSection is the Topics section of the Manage tab.
type topicsSection struct {
	client kafka.Facade

	table   *table.Model
	topics  []kafka.Topic
	sizes   map[string]int64
	loading bool
	err     error

	mode      mode
	create    *createForm
	configs   *configView
	partition *partitionForm
	confirm   *modal.Confirm

	// pending is the topic a confirmation applies to.
	pending string
}

func newTopicsSection() *topicsSection {
	s := &topicsSection{sizes: make(map[string]int64)}

	s.table = table.New(
		table.Column{Title: "NAME", Width: 0, TruncateFront: true},
		table.Column{Title: "PARTS", Width: 6, Align: table.Right},
		table.Column{Title: "RF", Width: 3, Align: table.Right},
		table.Column{Title: "ISR", Width: 9, Align: table.Right},
		table.Column{Title: "SIZE", Width: 10, Align: table.Right},
	)
	s.table.Focus()
	return s
}

// SetClient hands over the live client.
func (s *topicsSection) SetClient(client kafka.Facade) {
	s.client = client
	if client == nil {
		s.topics = nil
		s.table.SetRows(nil)
	}
}

// Init loads the topic list and sizes.
func (s *topicsSection) Init() tea.Cmd {
	if s.client == nil {
		return nil
	}
	s.loading, s.err = true, nil

	client := s.client
	return tea.Batch(
		func() tea.Msg {
			topics, err := client.ListTopics(context.Background(), true)
			return msg.TopicsLoaded{Topics: topics, Err: err}
		},
		func() tea.Msg {
			sizes, err := client.TopicSizes(context.Background())
			return msg.SizesLoaded{Topics: sizes, Err: err}
		},
	)
}

// Capturing reports whether a form owns the keyboard, including Tab.
func (s *topicsSection) Capturing() bool { return s.mode != modeList }

// AcceptsText reports whether typed characters go to a field.
func (s *topicsSection) AcceptsText() bool {
	return s.mode == modeCreate || s.mode == modeAddPartitions ||
		s.mode == modeConfirmDelete || (s.mode == modeConfig && s.configs.editing())
}

// Breadcrumb names the open form.
func (s *topicsSection) Breadcrumb() []string {
	switch s.mode {
	case modeCreate:
		return []string{"Create"}
	case modeConfig:
		return []string{s.configs.topic, "Configs"}
	case modeAddPartitions:
		return []string{s.pending, "Partitions"}
	default:
		return nil
	}
}

// Shortcuts implements ui.ShortcutProvider.
func (s *topicsSection) Shortcuts() []chrome.Shortcut {
	switch s.mode {
	case modeCreate, modeAddPartitions:
		return []chrome.Shortcut{
			{Key: "Tab", Label: "Next field"},
			{Key: "Enter", Label: "Confirm"},
			{Key: "Esc", Label: "Cancel"},
		}
	case modeConfig:
		return s.configs.Shortcuts()
	case modeConfirmDelete, modeConfirmTruncate:
		return []chrome.Shortcut{{Key: "Enter", Label: "Confirm"}, {Key: "Esc", Label: "Cancel"}}
	default:
		return []chrome.Shortcut{
			{Key: "Ctrl+N", Label: "Create"},
			{Key: "Ctrl+O", Label: "Configs"},
			{Key: "Ctrl+P", Label: "Partitions"},
			{Key: "F2", Label: "Delete"},
			{Key: "Ctrl+T", Label: "Truncate"},
			{Key: "/", Label: "Filter"},
		}
	}
}

// Update handles messages and keys.
func (s *topicsSection) Update(message tea.Msg) tea.Cmd {
	switch typed := message.(type) {
	case msg.TopicsLoaded:
		s.loading = false
		if typed.Err != nil {
			s.err = typed.Err
			return nil
		}
		s.topics = typed.Topics
		s.rebuild()
		return nil

	case msg.SizesLoaded:
		if typed.Err == nil {
			s.sizes = typed.Topics
			s.rebuild()
		}
		return nil

	case topicChangedMsg:
		return s.onTopicChanged(typed)

	case configsLoadedMsg:
		if s.configs != nil {
			s.configs.Loaded(typed)
		}
		return nil

	case tea.KeyMsg:
		return s.handleKey(typed)
	}
	return nil
}

func (s *topicsSection) onTopicChanged(message topicChangedMsg) tea.Cmd {
	if message.Err != nil {
		return notify(kafka.Explain(message.Err), msg.NotifyError)
	}
	// The list is reloaded rather than patched: a create or a partition change
	// alters more than the one row.
	return tea.Batch(
		notify(message.Summary, msg.NotifySuccess),
		s.Init(),
	)
}

func (s *topicsSection) handleKey(key tea.KeyMsg) tea.Cmd {
	switch s.mode {
	case modeCreate:
		return s.updateCreate(key)
	case modeAddPartitions:
		return s.updatePartitions(key)
	case modeConfig:
		return s.updateConfig(key)
	case modeConfirmDelete, modeConfirmTruncate:
		return s.updateConfirm(key)
	}
	return s.updateList(key)
}

func (s *topicsSection) updateList(key tea.KeyMsg) tea.Cmd {
	switch key.String() {
	case "ctrl+n":
		s.create = newCreateForm(s.topicNames())
		s.mode = modeCreate
		return nil

	case "ctrl+o":
		topic := s.selectedName()
		if topic == "" {
			return nil
		}
		s.configs = newConfigView(topic)
		s.mode = modeConfig
		return s.configs.Load(s.client)

	case "ctrl+p":
		topic := s.selected()
		if topic == nil {
			return nil
		}
		s.pending = topic.Name
		s.partition = newPartitionForm(topic.Name, topic.Partitions)
		s.mode = modeAddPartitions
		return nil

	case "f2":
		topic := s.selected()
		if topic == nil {
			return nil
		}
		if topic.Internal {
			return notify("internal topics cannot be deleted", msg.NotifyWarning)
		}
		s.pending = topic.Name
		// Typing the name is the standard guard against deleting the wrong topic
		// from muscle memory.
		s.confirm = modal.NewDangerConfirm(
			"Delete topic",
			fmt.Sprintf("Delete %s?", topic.Name),
			fmt.Sprintf("%d partitions · %s on disk · this cannot be undone",
				topic.Partitions, humanBytes(s.sizes[topic.Name])),
			topic.Name)
		s.mode = modeConfirmDelete
		return nil

	case "ctrl+t":
		topic := s.selected()
		if topic == nil {
			return nil
		}
		s.pending = topic.Name
		s.confirm = modal.NewDangerConfirm(
			"Delete all records",
			fmt.Sprintf("Discard every record in %s?", topic.Name),
			"The topic and its partitions remain; only the data is removed.",
			topic.Name)
		s.mode = modeConfirmTruncate
		return nil

	case "/":
		// The table owns filtering.
	}
	return s.table.Update(key)
}

func (s *topicsSection) updateCreate(key tea.KeyMsg) tea.Cmd {
	cmd := s.create.Update(key)

	switch {
	case s.create.Cancelled():
		s.mode, s.create = modeList, nil
		return nil

	case s.create.Submitted():
		spec, err := s.create.Spec()
		if err != nil {
			s.create.setError(err)
			return nil
		}
		s.mode, s.create = modeList, nil
		return s.createTopic(spec)
	}
	return cmd
}

func (s *topicsSection) updatePartitions(key tea.KeyMsg) tea.Cmd {
	cmd := s.partition.Update(key)

	switch {
	case s.partition.Cancelled():
		s.mode, s.partition = modeList, nil
		return nil

	case s.partition.Submitted():
		total, err := s.partition.Total()
		if err != nil {
			s.partition.setError(err)
			return nil
		}
		topic := s.partition.topic
		s.mode, s.partition = modeList, nil

		client := s.client
		return func() tea.Msg {
			err := client.AddPartitions(context.Background(), topic, total)
			return topicChangedMsg{
				Summary: fmt.Sprintf("%s now has %d partitions", topic, total),
				Err:     err,
			}
		}
	}
	return cmd
}

func (s *topicsSection) updateConfig(key tea.KeyMsg) tea.Cmd {
	if key.String() == "esc" && !s.configs.editing() {
		s.mode, s.configs = modeList, nil
		return nil
	}
	return s.configs.Update(key, s.client)
}

func (s *topicsSection) updateConfirm(key tea.KeyMsg) tea.Cmd {
	s.confirm.Update(key)

	switch {
	case s.confirm.Cancelled():
		s.mode, s.confirm = modeList, nil
		return nil

	case s.confirm.Confirmed():
		topic, truncating := s.pending, s.mode == modeConfirmTruncate
		s.mode, s.confirm, s.pending = modeList, nil, ""

		client := s.client
		if truncating {
			return func() tea.Msg {
				err := client.TruncateTopic(context.Background(), topic)
				return topicChangedMsg{Summary: "discarded every record in " + topic, Err: err}
			}
		}
		return func() tea.Msg {
			err := client.DeleteTopic(context.Background(), topic)
			return topicChangedMsg{Summary: "deleted " + topic, Err: err}
		}
	}
	return nil
}

// createTopic performs the creation and reloads the list.
func (s *topicsSection) createTopic(spec kafka.TopicSpec) tea.Cmd {
	client := s.client
	return func() tea.Msg {
		err := client.CreateTopic(context.Background(), spec)
		return topicChangedMsg{
			Summary: fmt.Sprintf("created %s with %d partitions", spec.Name, spec.Partitions),
			Err:     err,
		}
	}
}

// ------------------------------------------------------------------- rows ---

func (s *topicsSection) rebuild() {
	rows := make([]table.Row, 0, len(s.topics))
	for _, topic := range s.topics {
		size := "—"
		if bytes, ok := s.sizes[topic.Name]; ok {
			size = humanBytes(bytes)
		}

		name := topic.Name
		if topic.Internal {
			name += " *"
		}

		row := table.Row{
			Cells: []string{
				name,
				itoa(topic.Partitions),
				itoa(topic.ReplicationFactor),
				fmt.Sprintf("%d/%d", topic.InSync, topic.TotalReplicas),
				size,
			},
			Data: topic,
		}
		// An under-replicated topic is the one thing on this screen worth
		// spotting without reading the numbers.
		if !topic.Healthy() {
			style := warningStyle
			row.Style = &style
		}
		rows = append(rows, row)
	}
	s.table.SetRows(rows)
}

func (s *topicsSection) selected() *kafka.Topic {
	row := s.table.SelectedRow()
	if row == nil {
		return nil
	}
	topic, ok := row.Data.(kafka.Topic)
	if !ok {
		return nil
	}
	return &topic
}

func (s *topicsSection) selectedName() string {
	if topic := s.selected(); topic != nil {
		return topic.Name
	}
	return ""
}

func (s *topicsSection) topicNames() []string {
	names := make([]string, len(s.topics))
	for i, topic := range s.topics {
		names[i] = topic.Name
	}
	return names
}

// ------------------------------------------------------------------- view ---

// View renders the section.
func (s *topicsSection) View(k *ktx.Context, width, height int, focused bool) string {
	th := k.Theme

	switch s.mode {
	case modeCreate:
		return panel.Panel{Title: "Create topic", Focused: true, Width: width, Height: height}.
			Render(th, s.create.View(k, width-4))
	case modeAddPartitions:
		return panel.Panel{Title: "Add partitions", Focused: true, Width: width, Height: height}.
			Render(th, s.partition.View(k, width-4))
	case modeConfig:
		return s.configs.View(k, width, height)
	}

	content := s.table.View(th, width-2, height-2)
	switch {
	case s.loading && len(s.topics) == 0:
		content = th.Dim.Render("  loading topics…")
	case s.err != nil:
		content = th.Critical.Render("  " + kafka.Explain(s.err))
	}

	body := panel.Panel{
		Title:    "Topics",
		TopRight: s.counter(),
		Focused:  focused,
		Width:    width,
		Height:   height,
	}.Render(th, content)

	if s.mode == modeConfirmDelete || s.mode == modeConfirmTruncate {
		return modal.Overlay(body, s.confirm.View(th, min(width-6, 70)))
	}
	return body
}

func (s *topicsSection) counter() string {
	if s.table.Filter() != "" {
		return fmt.Sprintf("%d / %d", s.table.Len(), s.table.TotalLen())
	}
	return itoa(len(s.topics))
}

// ---------------------------------------------------------------- helpers ---

// topicChangedMsg reports the outcome of an operation that alters a topic.
type topicChangedMsg struct {
	Summary string
	Err     error
}

func notify(text string, level msg.NotifyLevel) tea.Cmd {
	return func() tea.Msg { return msg.Notify{Text: text, Level: level} }
}

var (
	warningStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("#FFAF00"))
	criticalStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#FF5F5F"))
)

func itoa(n int) string { return strconv.Itoa(n) }

func humanBytes(bytes int64) string {
	const unit = 1024
	if bytes <= 0 {
		return "—"
	}
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
