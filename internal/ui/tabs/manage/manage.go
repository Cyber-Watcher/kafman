// Package manage implements the Manage tab: every operation that changes the
// cluster, grouped by the resource it acts on — topics, consumer groups, SCRAM
// users and ACLs.
package manage

import (
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/Cyber-Watcher/kafman/internal/kafka"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/chrome"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/panel"
	"github.com/Cyber-Watcher/kafman/internal/ui/ktx"
	"github.com/Cyber-Watcher/kafman/internal/ui/msg"
)

// section is one entry in the left-hand list.
type section int

const (
	sectionTopics section = iota
	sectionGroups
	sectionUsers
	sectionACLs
	sectionCount
)

func (s section) String() string {
	switch s {
	case sectionTopics:
		return "Topics"
	case sectionGroups:
		return "Consumer groups"
	case sectionUsers:
		return "Users (SCRAM)"
	default:
		return "ACLs"
	}
}

// sectionView is what every section implements, so the tab can route to the
// current one without knowing which one it is.
type sectionView interface {
	SetClient(client kafka.Facade)
	Init() tea.Cmd

	// Capturing reports that the section owns the keyboard including Tab, which
	// otherwise moves focus to the section list.
	Capturing() bool
	AcceptsText() bool

	Breadcrumb() []string
	Shortcuts() []chrome.Shortcut
	Update(message tea.Msg) tea.Cmd
	View(k *ktx.Context, width, height int, focused bool) string
}

// Model is the Manage tab.
type Model struct {
	client kafka.Facade

	section section

	// sectionFocused is true while the keyboard drives the section list rather
	// than the content on the right.
	sectionFocused bool

	topics *topicsSection
	groups *groupsSection
	users  *usersSection
	acls   *aclsSection
}

// New builds the tab.
func New() *Model {
	return &Model{
		topics: newTopicsSection(),
		groups: newGroupsSection(),
		users:  newUsersSection(),
		acls:   newACLsSection(),
	}
}

// current is the section on screen.
func (m *Model) current() sectionView {
	switch m.section {
	case sectionTopics:
		return m.topics
	case sectionGroups:
		return m.groups
	case sectionUsers:
		return m.users
	default:
		return m.acls
	}
}

// Title implements ui.Tab.
func (m *Model) Title() string { return "Manage" }

// SetClient implements ui.ClientAware.
func (m *Model) SetClient(client kafka.Facade) {
	m.client = client
	m.topics.SetClient(client)
	m.groups.SetClient(client)
	m.users.SetClient(client)
	m.acls.SetClient(client)
}

// Init implements ui.Tab.
func (m *Model) Init() tea.Cmd {
	if m.client == nil {
		return nil
	}
	return m.sectionInit()
}

// AcceptsText reports whether a form or filter is capturing keystrokes.
func (m *Model) AcceptsText() bool { return m.current().AcceptsText() }

// Breadcrumb lets the root show where we are inside the tab.
func (m *Model) Breadcrumb() []string {
	return append([]string{m.section.String()}, m.current().Breadcrumb()...)
}

// Shortcuts implements ui.ShortcutProvider.
func (m *Model) Shortcuts() []chrome.Shortcut {
	if m.sectionFocused {
		return []chrome.Shortcut{
			{Key: "↑/↓", Label: "Section"},
			{Key: "Tab", Label: "Content"},
		}
	}
	return m.current().Shortcuts()
}

// Update implements ui.View.
func (m *Model) Update(message tea.Msg) tea.Cmd {
	if key, ok := message.(tea.KeyMsg); ok {
		return m.handleKey(key)
	}

	// Domain messages go to the section that asked for them. Each section uses
	// its own message types, so delivering to all of them is harmless and saves
	// the tab from tracking which request is in flight where.
	return tea.Batch(
		m.topics.Update(message),
		m.groups.Update(message),
		m.users.Update(message),
		m.acls.Update(message),
	)
}

func (m *Model) handleKey(key tea.KeyMsg) tea.Cmd {
	current := m.current()

	// A form on the right takes precedence: Tab moves between its fields.
	if current.Capturing() {
		return current.Update(key)
	}

	switch key.String() {
	case "tab", "shift+tab":
		m.sectionFocused = !m.sectionFocused
		return nil
	}

	if m.sectionFocused {
		switch key.String() {
		case "up", "k":
			m.section = (m.section - 1 + sectionCount) % sectionCount
			return m.sectionInit()
		case "down", "j":
			m.section = (m.section + 1) % sectionCount
			return m.sectionInit()
		}
		return nil
	}

	return current.Update(key)
}

func (m *Model) sectionInit() tea.Cmd { return m.current().Init() }

// CreateTopic creates a topic and reloads the list, as the create form does.
func (m *Model) CreateTopic(spec kafka.TopicSpec) tea.Cmd {
	return m.topics.createTopic(spec)
}

// OpenSection selects a section by the title shown in the list.
func (m *Model) OpenSection(title string) {
	for s := section(0); s < sectionCount; s++ {
		if s.String() == title {
			m.section, m.sectionFocused = s, false
			return
		}
	}
}

// UpsertScramUser writes a credential and reloads the list, as the user form
// does.
func (m *Model) UpsertScramUser(spec kafka.ScramUserSpec) tea.Cmd {
	m.section = sectionUsers
	return m.users.upsert(spec, true)
}

// CreateACL adds a rule and reloads the list, as the ACL form does.
func (m *Model) CreateACL(acl kafka.ACL) tea.Cmd {
	m.section = sectionACLs
	return m.acls.create(acl)
}

// PreviewOffsetReset opens the reset preview for a spec, as pressing Preview on
// the reset form does. Nothing is committed: the preview still has to be
// confirmed.
func (m *Model) PreviewOffsetReset(spec kafka.ResetSpec) tea.Cmd {
	m.section, m.sectionFocused = sectionGroups, false
	return m.groups.previewReset(spec)
}

// FocusGroup handles a jump from another tab: Enter on a group in Overview
// lands on that group here, not merely on the section.
func (m *Model) FocusGroup(message msg.FocusGroup) tea.Cmd {
	m.section, m.sectionFocused = sectionGroups, false

	// The list may not be loaded yet, in which case the selection is applied
	// once it arrives.
	m.groups.selectOnLoad = message.Group
	return tea.Batch(m.groups.Init(), m.groups.Select(message.Group))
}

// View implements ui.View.
func (m *Model) View(k *ktx.Context) string {
	th := k.Theme
	width, height := k.WindowWidth, k.AvailableHeight

	if m.client == nil {
		return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center,
			th.Dim.Render("not connected"))
	}

	const sectionsWidth = 22
	contentWidth := width - sectionsWidth

	sections := panel.Panel{
		Title:   "Sections",
		Focused: m.sectionFocused,
		Width:   sectionsWidth,
		Height:  height,
	}.Render(th, m.sectionList(k))

	content := m.current().View(k, contentWidth, height, !m.sectionFocused)

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
		case s == m.section && m.sectionFocused:
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
