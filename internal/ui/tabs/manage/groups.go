package manage

import (
	"context"
	"fmt"
	"sort"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/Cyber-Watcher/kafman/internal/kafka"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/chrome"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/modal"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/panel"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/table"
	"github.com/Cyber-Watcher/kafman/internal/ui/ktx"
	"github.com/Cyber-Watcher/kafman/internal/ui/msg"
	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

// groupMode is what the Consumer groups section is showing.
type groupMode int

const (
	groupModeList groupMode = iota
	groupModeMembers
	groupModeReset
	groupModePreview
	groupModeConfirmDelete
	groupModeConfirmClearOffsets
	groupModeConfirmReset
)

// confirming reports whether a confirmation dialog is open.
func (m groupMode) confirming() bool {
	return m == groupModeConfirmDelete || m == groupModeConfirmClearOffsets ||
		m == groupModeConfirmReset
}

// groupsSection is the Consumer groups section of the Manage tab.
//
// Two panels: the groups on the left, the per-partition offsets of the selected
// group on the right. The detail is loaded for the selected group only — a
// cluster can hold thousands of groups and each detail costs an offset request
// per partition.
type groupsSection struct {
	client kafka.Facade

	table   *table.Model
	groups  []kafka.ConsumerGroup
	loading bool
	err     error

	// offsets holds the detail of the group named by detailName. Responses for
	// any other group are dropped: the cursor moves faster than the cluster
	// answers.
	offsets       *table.Model
	detail        kafka.ConsumerGroup
	detailOffsets []kafka.GroupOffset
	detailName    string
	detailLoading bool
	detailErr     error

	// offsetsFocused moves the keyboard to the right-hand panel, which is what
	// selects the topic that Ctrl+D clears.
	offsetsFocused bool

	mode    groupMode
	members *table.Model
	reset   *resetForm
	preview *previewView
	confirm *modal.Confirm

	// pending is the group a confirmation applies to, pendingTopic the topic.
	pending      string
	pendingTopic string

	// plain mirrors the theme's plain-fonts setting. Rows are built in Update,
	// where the theme is out of reach, so View picks the flag up and rebuilds
	// when it changes.
	plain bool

	// selectOnLoad is a group another tab asked for before the list existed.
	selectOnLoad string
}

func newGroupsSection() *groupsSection {
	s := &groupsSection{}

	// Group names are distinctive at the front, unlike topic names, so they are
	// cut from the end when they do not fit.
	s.table = table.New(
		table.Column{Title: "NAME", Width: 0},
		table.Column{Title: "STATE", Width: 13},
		table.Column{Title: "MEM", Width: 3, Align: table.Right},
		table.Column{Title: "LAG", Width: 8, Align: table.Right},
	)
	s.table.Focus()

	// The topic column is flexible and truncated from the front; the host gets
	// a width a full "/192.168.100.200" fits into — an address cut short is
	// useless, a topic cut short is still recognisable by its tail.
	s.offsets = table.New(
		table.Column{Title: "TOPIC", Width: 0, TruncateFront: true},
		table.Column{Title: "P", Width: 3, Align: table.Right},
		table.Column{Title: "CURRENT", Width: 9, Align: table.Right},
		table.Column{Title: "END", Width: 9, Align: table.Right},
		table.Column{Title: "LAG", Width: 7, Align: table.Right},
		table.Column{Title: "HOST", Width: 16},
	)

	s.members = table.New(
		table.Column{Title: "CLIENT", Width: 18},
		table.Column{Title: "HOST", Width: 16},
		table.Column{Title: "MEMBER ID", Width: 0, TruncateFront: true},
		table.Column{Title: "PARTS", Width: 5, Align: table.Right},
	)
	return s
}

// SetClient hands over the live client.
func (s *groupsSection) SetClient(client kafka.Facade) {
	s.client = client
	if client == nil {
		s.groups, s.detailName = nil, ""
		s.detailOffsets, s.detail = nil, kafka.ConsumerGroup{}
		s.table.SetRows(nil)
		s.offsets.SetRows(nil)
	}
}

// Init loads the group list with its lag.
func (s *groupsSection) Init() tea.Cmd {
	if s.client == nil {
		return nil
	}
	s.loading, s.err = true, nil

	client := s.client
	return func() tea.Msg {
		groups, err := client.ConsumerGroupsWithLag(context.Background())
		return groupsLoadedMsg{Groups: groups, Err: err}
	}
}

// Capturing reports whether the section owns the keyboard, including Tab.
func (s *groupsSection) Capturing() bool { return s.mode != groupModeList }

// AcceptsText reports whether typed characters go to a field.
func (s *groupsSection) AcceptsText() bool {
	return s.mode == groupModeReset || s.mode == groupModeConfirmDelete ||
		s.mode == groupModeConfirmClearOffsets
}

// Breadcrumb names what is open inside the section.
func (s *groupsSection) Breadcrumb() []string {
	switch s.mode {
	case groupModeMembers:
		return []string{s.detailName, "Members"}
	case groupModeReset:
		return []string{s.reset.group, "Reset offsets"}
	case groupModePreview:
		return []string{s.preview.group, "Reset offsets", "Preview"}
	default:
		return nil
	}
}

// Shortcuts implements ui.ShortcutProvider.
func (s *groupsSection) Shortcuts() []chrome.Shortcut {
	switch s.mode {
	case groupModeMembers:
		return []chrome.Shortcut{{Key: "Esc", Label: "Back"}}
	case groupModeReset:
		return []chrome.Shortcut{
			{Key: "Tab", Label: "Next field"},
			{Key: "Enter", Label: "Preview"},
			{Key: "Esc", Label: "Cancel"},
		}
	case groupModePreview:
		return []chrome.Shortcut{
			{Key: "Enter", Label: "Apply"},
			{Key: "Esc", Label: "Back"},
		}
	case groupModeConfirmDelete, groupModeConfirmClearOffsets, groupModeConfirmReset:
		return []chrome.Shortcut{{Key: "Enter", Label: "Confirm"}, {Key: "Esc", Label: "Cancel"}}
	}

	if s.offsetsFocused {
		return []chrome.Shortcut{
			{Key: "←", Label: "Groups"},
			{Key: "Ctrl+D", Label: "Clear offsets"},
			{Key: "Ctrl+R", Label: "Reset offsets"},
		}
	}
	return []chrome.Shortcut{
		{Key: "Ctrl+R", Label: "Reset offsets"},
		{Key: "F2", Label: "Delete group"},
		{Key: "Enter", Label: "Members"},
		{Key: "→", Label: "Offsets"},
		{Key: "/", Label: "Filter"},
	}
}

// Update handles messages and keys.
func (s *groupsSection) Update(message tea.Msg) tea.Cmd {
	switch typed := message.(type) {
	case groupsLoadedMsg:
		s.loading = false
		if typed.Err != nil {
			s.err = typed.Err
			return nil
		}
		s.groups = typed.Groups
		s.rebuild()

		if name := s.selectOnLoad; name != "" {
			s.selectOnLoad = ""
			if cmd := s.Select(name); cmd != nil {
				return cmd
			}
		}
		// A reload of the list is also a reload of the detail: the numbers on the
		// right are exactly the ones an operation just moved.
		return s.loadDetail(true)

	case groupDetailMsg:
		return s.onDetail(typed)

	case previewLoadedMsg:
		return s.onPreview(typed)

	case groupChangedMsg:
		return s.onChanged(typed)

	case tea.KeyMsg:
		return s.handleKey(typed)
	}
	return nil
}

func (s *groupsSection) onDetail(message groupDetailMsg) tea.Cmd {
	// The selection moved on while this was in flight.
	if message.Name != s.selectedName() {
		return nil
	}
	s.detailLoading = false

	if message.Err != nil {
		s.detailErr, s.detailOffsets = message.Err, nil
		s.offsets.SetRows(nil)
		return nil
	}

	s.detailErr = nil
	s.detail, s.detailOffsets = message.Group, message.Offsets
	s.rebuildOffsets()
	s.rebuildMembers()
	return nil
}

func (s *groupsSection) onPreview(message previewLoadedMsg) tea.Cmd {
	if s.mode != groupModeReset && s.mode != groupModePreview {
		return nil
	}
	if message.Err != nil {
		s.mode = groupModeReset
		s.reset.setError(message.Err)
		return nil
	}

	s.preview = newPreviewView(message.Group, message.Spec, message.Changes)
	s.mode = groupModePreview
	return nil
}

func (s *groupsSection) onChanged(message groupChangedMsg) tea.Cmd {
	if message.Err != nil {
		return notify(kafka.Explain(message.Err), msg.NotifyError)
	}
	// The detail is invalidated along with the list: every operation here moves
	// offsets that the right-hand panel is showing.
	s.detailName = ""
	return tea.Batch(
		notify(message.Summary, msg.NotifySuccess),
		s.Init(),
	)
}

func (s *groupsSection) handleKey(key tea.KeyMsg) tea.Cmd {
	switch s.mode {
	case groupModeMembers:
		if key.String() == "esc" {
			s.mode = groupModeList
			return nil
		}
		return s.members.Update(key)

	case groupModeReset:
		return s.updateReset(key)

	case groupModePreview:
		return s.updatePreview(key)

	case groupModeConfirmDelete, groupModeConfirmClearOffsets, groupModeConfirmReset:
		return s.updateConfirm(key)
	}
	return s.updateList(key)
}

func (s *groupsSection) updateList(key tea.KeyMsg) tea.Cmd {
	switch key.String() {
	case "right", "l":
		if len(s.detailOffsets) > 0 {
			s.offsetsFocused = true
			s.table.Blur()
			s.offsets.Focus()
		}
		return nil

	case "left", "h":
		s.offsetsFocused = false
		s.offsets.Blur()
		s.table.Focus()
		return nil

	case "enter":
		if s.offsetsFocused || s.selectedName() == "" {
			return nil
		}
		if len(s.detail.MemberList) == 0 {
			return notify(s.selectedName()+" has no members", msg.NotifyInfo)
		}
		s.mode = groupModeMembers
		s.members.Focus()
		return nil

	case "ctrl+r":
		group := s.selectedName()
		if group == "" {
			return nil
		}
		s.reset = newResetForm(group, s.detailTopics())
		s.mode = groupModeReset
		return nil

	case "ctrl+d":
		return s.clearOffsets()

	case "f2":
		group := s.selected()
		if group == nil {
			return nil
		}
		s.pending = group.Name
		s.confirm = modal.NewConfirm("Delete consumer group",
			fmt.Sprintf("Delete %s?\n\nIts committed offsets go with it. Kafka refuses\nwhile the group still has members.", group.Name))
		s.mode = groupModeConfirmDelete
		return nil
	}

	if s.offsetsFocused {
		return s.offsets.Update(key)
	}

	before := s.selectedName()
	cmd := s.table.Update(key)
	if s.selectedName() != before {
		return tea.Batch(cmd, s.loadDetail(false))
	}
	return cmd
}

// clearOffsets asks to remove the group's offsets for one topic, which is the
// selected row's topic when the offsets panel has the keyboard and the group's
// only topic otherwise.
func (s *groupsSection) clearOffsets() tea.Cmd {
	group := s.selectedName()
	if group == "" {
		return nil
	}

	topic := ""
	if s.offsetsFocused {
		if offset := s.selectedOffset(); offset != nil {
			topic = offset.Topic
		}
	} else if topics := s.detailTopics(); len(topics) == 1 {
		topic = topics[0]
	}
	if topic == "" {
		return notify("select a partition on the right to choose the topic", msg.NotifyWarning)
	}

	s.pending, s.pendingTopic = group, topic
	s.confirm = modal.NewConfirm("Delete committed offsets",
		fmt.Sprintf("Forget where %s is in %s?\n\nOn its next start the group falls back to\nauto.offset.reset.", group, topic))
	s.mode = groupModeConfirmClearOffsets
	return nil
}

// previewReset asks the cluster to compute a reset and opens the preview, which
// is what the form does once it is filled in.
func (s *groupsSection) previewReset(spec kafka.ResetSpec) tea.Cmd {
	if s.client == nil {
		return nil
	}
	s.reset = newResetForm(spec.Group, s.detailTopics())
	s.mode = groupModeReset

	client := s.client
	return func() tea.Msg {
		changes, err := client.PreviewOffsetReset(context.Background(), spec)
		return previewLoadedMsg{Group: spec.Group, Spec: spec, Changes: changes, Err: err}
	}
}

func (s *groupsSection) updateReset(key tea.KeyMsg) tea.Cmd {
	cmd := s.reset.Update(key)

	switch {
	case s.reset.Cancelled():
		s.mode, s.reset = groupModeList, nil
		return nil

	case s.reset.Submitted():
		spec, err := s.reset.Spec()
		if err != nil {
			s.reset.setError(err)
			return nil
		}

		client := s.client
		return func() tea.Msg {
			changes, err := client.PreviewOffsetReset(context.Background(), spec)
			return previewLoadedMsg{Group: spec.Group, Spec: spec, Changes: changes, Err: err}
		}
	}
	return cmd
}

func (s *groupsSection) updatePreview(key tea.KeyMsg) tea.Cmd {
	switch key.String() {
	case "esc":
		// Back to the form, not out of the reset: the usual reason to leave a
		// preview is that the numbers were not what was meant.
		s.mode, s.preview = groupModeReset, nil
		return nil

	case "enter":
		moving := len(s.preview.movingChanges())
		if moving == 0 {
			return notify("every partition is already there", msg.NotifyInfo)
		}

		// The preview shows what would happen; this asks whether to do it. The
		// two are separate because reading a table is not the same act as
		// agreeing to it.
		s.confirm = modal.NewConfirm("Apply the reset",
			fmt.Sprintf("Move %d partitions of %s %s?\n\nConsumers of the group will read from the new\npositions.",
				moving, s.preview.group, s.preview.spec.Kind))
		s.mode = groupModeConfirmReset
		return nil
	}
	return s.preview.Update(key)
}

func (s *groupsSection) updateConfirm(key tea.KeyMsg) tea.Cmd {
	s.confirm.Update(key)

	switch {
	case s.confirm.Cancelled():
		// Declining the reset goes back to the preview it was asked from, not
		// out of the reset altogether.
		if s.mode == groupModeConfirmReset {
			s.mode, s.confirm = groupModePreview, nil
			return nil
		}
		s.mode, s.confirm = groupModeList, nil
		return nil

	case s.confirm.Confirmed() && s.mode == groupModeConfirmReset:
		group, changes := s.preview.group, s.preview.movingChanges()
		summary := s.preview.summary()
		s.mode, s.confirm, s.preview, s.reset = groupModeList, nil, nil, nil

		client := s.client
		return func() tea.Msg {
			err := client.ApplyOffsetReset(context.Background(), group, changes)
			return groupChangedMsg{Summary: summary, Err: err}
		}

	case s.confirm.Confirmed():
		group, topic := s.pending, s.pendingTopic
		clearing := s.mode == groupModeConfirmClearOffsets
		s.mode, s.confirm = groupModeList, nil
		s.pending, s.pendingTopic = "", ""

		client := s.client
		if clearing {
			return func() tea.Msg {
				err := client.DeleteGroupOffsets(context.Background(), group, topic)
				return groupChangedMsg{
					Summary: fmt.Sprintf("cleared the offsets of %s in %s", group, topic),
					Err:     err,
				}
			}
		}
		return func() tea.Msg {
			err := client.DeleteConsumerGroup(context.Background(), group)
			return groupChangedMsg{Summary: "deleted group " + group, Err: err}
		}
	}
	return nil
}

// loadDetail fetches the offsets of the selected group. Without force it does
// nothing when they are already on screen, which is what keeps cursor movement
// from refetching the same group.
func (s *groupsSection) loadDetail(force bool) tea.Cmd {
	name := s.selectedName()
	if s.client == nil || name == "" {
		return nil
	}
	if name == s.detailName && !force {
		return nil
	}

	if name != s.detailName {
		// A different group: what is on screen belongs to the old one.
		s.detailOffsets, s.detail = nil, kafka.ConsumerGroup{}
		s.offsets.SetRows(nil)
		s.offsetsFocused = false
		s.offsets.Blur()
		s.table.Focus()
	}
	s.detailName, s.detailLoading, s.detailErr = name, true, nil

	client := s.client
	return func() tea.Msg {
		group, offsets, err := client.DescribeConsumerGroup(context.Background(), name)
		return groupDetailMsg{Name: name, Group: group, Offsets: offsets, Err: err}
	}
}

// Select moves the cursor onto a named group, which is how a jump from another
// tab lands.
func (s *groupsSection) Select(name string) tea.Cmd {
	for i, row := range s.table.Rows() {
		if len(row.Cells) > 0 && row.Cells[0] == name {
			s.table.MoveCursor(i - s.table.Cursor())
			return s.loadDetail(false)
		}
	}
	return nil
}

// ------------------------------------------------------------------- rows ---

func (s *groupsSection) rebuild() {
	rows := make([]table.Row, 0, len(s.groups))
	for _, group := range s.groups {
		lag := "—"
		if group.LagKnown {
			lag = humanCount(group.TotalLag)
		}

		row := table.Row{
			Cells: []string{group.Name, stateLabel(group.State, s.plain), itoa(group.Members), lag},
			Data:  group,
		}
		if style := stateStyle(group.State); style != nil {
			row.Style = style
		}
		rows = append(rows, row)
	}
	s.table.SetRows(rows)
}

func (s *groupsSection) rebuildOffsets() {
	rows := make([]table.Row, 0, len(s.detailOffsets))
	for _, offset := range s.detailOffsets {
		current, lag := "—", "—"
		if offset.Current >= 0 {
			current = humanCount(offset.Current)
		}
		if offset.Lag >= 0 {
			lag = humanCount(offset.Lag)
		}

		host := offset.Host
		if host == "" {
			// No member owns the partition; the group is not reading it now.
			host = "―"
		}

		row := table.Row{
			Cells: []string{
				offset.Topic, itoa(int(offset.Partition)), current,
				humanCount(offset.NewestOffset), lag, host,
			},
			Data: offset,
		}
		if offset.Lag > 0 {
			style := warningStyle
			row.Style = &style
		}
		rows = append(rows, row)
	}
	s.offsets.SetRows(rows)
}

func (s *groupsSection) rebuildMembers() {
	rows := make([]table.Row, 0, len(s.detail.MemberList))
	for _, member := range s.detail.MemberList {
		rows = append(rows, table.Row{
			Cells: []string{
				member.ClientID, member.Host, member.MemberID,
				itoa(len(member.Assignment)),
			},
			Data: member,
		})
	}
	s.members.SetRows(rows)
}

func (s *groupsSection) selected() *kafka.ConsumerGroup {
	row := s.table.SelectedRow()
	if row == nil {
		return nil
	}
	group, ok := row.Data.(kafka.ConsumerGroup)
	if !ok {
		return nil
	}
	return &group
}

func (s *groupsSection) selectedName() string {
	if group := s.selected(); group != nil {
		return group.Name
	}
	return ""
}

func (s *groupsSection) selectedOffset() *kafka.GroupOffset {
	row := s.offsets.SelectedRow()
	if row == nil {
		return nil
	}
	offset, ok := row.Data.(kafka.GroupOffset)
	if !ok {
		return nil
	}
	return &offset
}

// detailTopics lists the topics the selected group has offsets for.
func (s *groupsSection) detailTopics() []string {
	seen := make(map[string]bool)
	topics := make([]string, 0, 4)
	for _, offset := range s.detailOffsets {
		if !seen[offset.Topic] {
			seen[offset.Topic] = true
			topics = append(topics, offset.Topic)
		}
	}
	return topics
}

// ------------------------------------------------------------------- view ---

// View renders the section.
func (s *groupsSection) View(k *ktx.Context, width, height int, focused bool) string {
	th := k.Theme

	if s.plain != th.PlainFonts {
		s.plain = th.PlainFonts
		s.rebuild()
	}

	switch s.mode {
	case groupModeReset:
		return panel.Panel{Title: "Reset offsets · " + s.reset.group, Focused: true,
			Width: width, Height: height}.Render(th, s.reset.View(k, width-4))

	case groupModePreview:
		return s.preview.View(k, width, height)

	case groupModeConfirmReset:
		// The preview stays behind the dialog: the numbers being agreed to must
		// remain in sight while agreeing to them.
		return modal.Overlay(s.preview.View(k, width, height),
			s.confirm.View(th, min(width-6, 70)))

	case groupModeMembers:
		return s.membersView(k, width, height)
	}

	// Side by side only when both panels can hold their columns: the group list
	// needs room for a name, the offsets for six numbers. The window's own
	// "narrow" flag is not the measure here — the section list has already taken
	// its share of the width.
	const bothFit = 92

	var body string
	if width < bothFit {
		// One at a time; → / ← already switch between them.
		if s.offsetsFocused {
			body = s.offsetsView(k, width, height, focused)
		} else {
			body = s.listView(k, width, height, focused)
		}
	} else {
		listWidth := min(max(width*2/5, 34), 48)
		body = lipgloss.JoinHorizontal(lipgloss.Top,
			s.listView(k, listWidth, height, focused && !s.offsetsFocused),
			s.offsetsView(k, width-listWidth, height, focused && s.offsetsFocused),
		)
	}

	if s.mode.confirming() {
		return modal.Overlay(body, s.confirm.View(th, min(width-6, 70)))
	}
	return body
}

func (s *groupsSection) listView(k *ktx.Context, width, height int, focused bool) string {
	th := k.Theme

	content := s.table.View(th, width-2, height-2)
	switch {
	case s.loading && len(s.groups) == 0:
		content = th.Dim.Render("  loading groups…")
	case s.err != nil:
		content = th.Critical.Render("  " + kafka.Explain(s.err))
	case len(s.groups) == 0:
		content = th.Dim.Render("  no consumer groups")
	}

	counter := itoa(len(s.groups))
	if s.table.Filter() != "" {
		counter = fmt.Sprintf("%d / %d", s.table.Len(), s.table.TotalLen())
	}

	return panel.Panel{
		Title: "Groups", TopRight: counter, Focused: focused,
		Width: width, Height: height,
	}.Render(th, content)
}

func (s *groupsSection) offsetsView(k *ktx.Context, width, height int, focused bool) string {
	th := k.Theme

	title := s.detailName
	if title == "" {
		title = "Offsets"
	}

	// The hosts line takes one row from the table — a live group's consumers
	// are worth it.
	hosts := s.hostsLine(th, width-4)
	tableHeight := height - 2
	if hosts != "" {
		tableHeight--
	}

	content := s.offsets.View(th, width-2, tableHeight)
	switch {
	case s.detailName == "":
		content = th.Dim.Render("  select a group")
	case s.detailLoading:
		content = th.Dim.Render("  loading offsets…")
	case s.detailErr != nil:
		content = th.Critical.Render("  " + kafka.Explain(s.detailErr))
	case len(s.detailOffsets) == 0:
		content = th.Dim.Render("  the group has no committed offsets")
	}
	if hosts != "" {
		content = lipgloss.JoinVertical(lipgloss.Left, content, hosts)
	}

	return panel.Panel{
		Title: title, TopRight: s.detailCounter(), Focused: focused,
		Width: width, Height: height,
	}.Render(th, content)
}

// hostsLine lists every host the selected group's members are running on, or
// "" for a group with no members. The full membership is one Enter away; the
// hosts are on this screen because "who is consuming this right now" is the
// question the offsets panel usually gets opened for.
func (s *groupsSection) hostsLine(th *theme.Theme, width int) string {
	if s.detailLoading || s.detailErr != nil || len(s.detail.MemberList) == 0 {
		return ""
	}

	seen := make(map[string]bool)
	hosts := make([]string, 0, len(s.detail.MemberList))
	for _, member := range s.detail.MemberList {
		if member.Host != "" && !seen[member.Host] {
			seen[member.Host] = true
			hosts = append(hosts, member.Host)
		}
	}
	if len(hosts) == 0 {
		return ""
	}
	sort.Strings(hosts)

	label := fmt.Sprintf("  hosts (%d): ", len(hosts))
	text := strings.Join(hosts, ", ")
	if room := width - lipgloss.Width(label); room > 3 && lipgloss.Width(text) > room {
		text = string([]rune(text)[:room-1]) + "…"
	}
	return th.Label.Render(label) + th.Value.Render(text)
}

func (s *groupsSection) detailCounter() string {
	if s.detailName == "" || s.detailLoading || s.detailErr != nil {
		return ""
	}
	parts := []string{"total lag: " + humanCount(s.detail.TotalLag)}
	if s.detail.Coordinator > 0 {
		parts = append(parts, fmt.Sprintf("coordinator %d", s.detail.Coordinator))
	}
	return strings.Join(parts, " · ")
}

func (s *groupsSection) membersView(k *ktx.Context, width, height int) string {
	th := k.Theme

	lines := []string{s.members.View(th, width-2, height-5)}

	// The assignment of the selected member, which is the reason to open this
	// screen at all.
	if row := s.members.SelectedRow(); row != nil {
		if member, ok := row.Data.(kafka.GroupMember); ok {
			lines = append(lines,
				th.Label.Render("  assignment: ")+th.Value.Render(assignmentText(member, width-16)))
		}
	}

	return panel.Panel{
		Title:    s.detailName + " · members",
		TopRight: s.detail.Protocol,
		Focused:  true,
		Width:    width,
		Height:   height,
	}.Render(th, lipgloss.JoinVertical(lipgloss.Left, lines...))
}

// assignmentText renders a member's partitions as "topic p0,p1,p2".
func assignmentText(member kafka.GroupMember, width int) string {
	if len(member.Assignment) == 0 {
		return "none"
	}

	var parts []string
	var current string
	var partitions []string

	flush := func() {
		if current != "" {
			parts = append(parts, current+" p"+strings.Join(partitions, ",p"))
		}
	}
	for _, ref := range member.Assignment {
		if ref.Topic != current {
			flush()
			current, partitions = ref.Topic, nil
		}
		partitions = append(partitions, itoa(int(ref.Partition)))
	}
	flush()

	text := strings.Join(parts, "   ")
	if width > 3 && len(text) > width {
		text = text[:width-1] + "…"
	}
	return text
}

// ---------------------------------------------------------------- messages ---

// groupsLoadedMsg carries the group list with lag.
type groupsLoadedMsg struct {
	Groups []kafka.ConsumerGroup
	Err    error
}

// groupDetailMsg carries one group's members and per-partition offsets.
type groupDetailMsg struct {
	Name    string
	Group   kafka.ConsumerGroup
	Offsets []kafka.GroupOffset
	Err     error
}

// previewLoadedMsg carries a computed reset, before anything is committed.
type previewLoadedMsg struct {
	Group   string
	Spec    kafka.ResetSpec
	Changes []kafka.OffsetChange
	Err     error
}

// groupChangedMsg reports the outcome of an operation that alters a group.
type groupChangedMsg struct {
	Summary string
	Err     error
}

// ---------------------------------------------------------------- helpers ---

// stateLabel prefixes the state with the marker yozefu uses, so the shape of the
// group is visible before the word is read.
func stateLabel(state string, plain bool) string {
	marker := func(nerd, ascii string) string {
		if plain {
			return ascii
		}
		return nerd
	}

	switch state {
	case kafka.GroupStable:
		return marker("⏺", "*") + " Stable"
	case kafka.GroupPreparingRebalance, kafka.GroupCompletingRebalance:
		return marker("⦿", "~") + " Rebalancing"
	case kafka.GroupEmpty:
		return marker("◯", "o") + " Empty"
	case kafka.GroupDead:
		return marker("⊗", "x") + " Dead"
	default:
		return state
	}
}

func stateStyle(state string) *lipgloss.Style {
	switch state {
	case kafka.GroupPreparingRebalance, kafka.GroupCompletingRebalance:
		style := warningStyle
		return &style
	case kafka.GroupDead:
		style := criticalStyle
		return &style
	}
	return nil
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
