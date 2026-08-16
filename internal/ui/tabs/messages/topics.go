package messages

import (
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/Cyber-Watcher/kafman/internal/kafka"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/panel"
	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

// topicList is the left-hand panel: every topic with a checkbox, an incremental
// filter and the selected ones floated to the top.
//
// Floating the selection is what makes the panel usable on a cluster with
// hundreds of topics: without it, the two topics being read scroll away the
// moment the list is filtered or refreshed. Idea from yozefu.
type topicList struct {
	topics   []kafka.Topic
	selected map[string]bool

	filter        string
	filtering     bool
	showInternal  bool
	cursor        int
	offset        int
	pageSize      int
	focused       bool
	visibleTopics []kafka.Topic
}

func newTopicList() *topicList {
	return &topicList{selected: make(map[string]bool)}
}

// SetTopics replaces the list, keeping the cursor on the same topic where it
// still exists.
func (t *topicList) SetTopics(topics []kafka.Topic) {
	previous := t.currentName()
	t.topics = topics
	t.rebuild()

	if previous != "" {
		for i, topic := range t.visibleTopics {
			if topic.Name == previous {
				t.cursor = i
				break
			}
		}
	}
	t.clamp()
}

// Selected returns the checked topics, in list order.
func (t *topicList) Selected() []string {
	var out []string
	for _, topic := range t.topics {
		if t.selected[topic.Name] {
			out = append(out, topic.Name)
		}
	}
	return out
}

// SelectOnly checks exactly one topic, used when another tab asks for it.
func (t *topicList) SelectOnly(name string) {
	t.selected = map[string]bool{name: true}
	t.rebuild()
	for i, topic := range t.visibleTopics {
		if topic.Name == name {
			t.cursor = i
			break
		}
	}
	t.clamp()
}

// ClearSelection unchecks everything.
func (t *topicList) ClearSelection() {
	t.selected = make(map[string]bool)
	t.rebuild()
}

// Filtering reports whether the panel is capturing typed characters.
func (t *topicList) Filtering() bool { return t.filtering && t.focused }

func (t *topicList) Focus() { t.focused = true }
func (t *topicList) Blur()  { t.focused = false }

// LongestName is what the tab uses to size the panel to its content.
func (t *topicList) LongestName() int {
	longest := 0
	for _, topic := range t.topics {
		longest = max(longest, len(topic.Name))
	}
	return longest
}

func (t *topicList) currentName() string {
	if t.cursor < 0 || t.cursor >= len(t.visibleTopics) {
		return ""
	}
	return t.visibleTopics[t.cursor].Name
}

// rebuild applies the filter and floats the selected topics to the top.
func (t *topicList) rebuild() {
	needle := strings.ToLower(t.filter)

	var chosen, rest []kafka.Topic
	for _, topic := range t.topics {
		if topic.Internal && !t.showInternal {
			continue
		}
		if needle != "" && !strings.Contains(strings.ToLower(topic.Name), needle) {
			continue
		}
		if t.selected[topic.Name] {
			chosen = append(chosen, topic)
			continue
		}
		rest = append(rest, topic)
	}
	t.visibleTopics = append(chosen, rest...)
}

func (t *topicList) clamp() {
	if t.cursor >= len(t.visibleTopics) {
		t.cursor = len(t.visibleTopics) - 1
	}
	if t.cursor < 0 {
		t.cursor = 0
	}
	if t.pageSize > 0 {
		switch {
		case t.cursor < t.offset:
			t.offset = t.cursor
		case t.cursor >= t.offset+t.pageSize:
			t.offset = t.cursor - t.pageSize + 1
		}
	}
	if t.offset < 0 {
		t.offset = 0
	}
}

// Update handles keys. Typing any printable character starts filtering, without
// a mode to enter first.
func (t *topicList) Update(msg tea.Msg) tea.Cmd {
	key, ok := msg.(tea.KeyMsg)
	if !ok || !t.focused {
		return nil
	}

	switch key.Type {
	case tea.KeyUp:
		t.cursor--
		t.clamp()
		return nil
	case tea.KeyDown:
		t.cursor++
		t.clamp()
		return nil
	case tea.KeyPgUp:
		t.cursor -= t.pageSize
		t.clamp()
		return nil
	case tea.KeyPgDown:
		t.cursor += t.pageSize
		t.clamp()
		return nil
	case tea.KeyHome:
		t.cursor = 0
		t.clamp()
		return nil
	case tea.KeyEnd:
		t.cursor = len(t.visibleTopics) - 1
		t.clamp()
		return nil
	case tea.KeyBackspace:
		if t.filter != "" {
			t.filter = t.filter[:len(t.filter)-1]
			t.filtering = t.filter != ""
			t.rebuild()
			t.clamp()
		}
		return nil
	case tea.KeyEsc:
		if t.filter != "" {
			t.filter, t.filtering = "", false
			t.rebuild()
			t.clamp()
		}
		return nil
	case tea.KeySpace:
		t.toggle()
		return nil
	}

	switch key.String() {
	case "ctrl+u":
		t.ClearSelection()
		return nil
	case "f4":
		t.showInternal = !t.showInternal
		t.rebuild()
		t.clamp()
		return nil
	}

	// Printable characters go to the filter. j and k are sacrificed here: in a
	// panel whose whole purpose is finding a topic by name, typing must win.
	if key.Type == tea.KeyRunes && len(key.Runes) > 0 {
		t.filter += string(key.Runes)
		t.filtering = true
		t.rebuild()
		t.clamp()
	}
	return nil
}

func (t *topicList) toggle() {
	name := t.currentName()
	if name == "" {
		return
	}
	if t.selected[name] {
		delete(t.selected, name)
	} else {
		t.selected[name] = true
	}
	// Deliberately not rebuilt here: reordering under the cursor as the user
	// ticks boxes makes multi-select infuriating. The float happens on the next
	// refresh or filter change.
}

// View renders the panel body.
func (t *topicList) View(th *theme.Theme, width, height int) string {
	// The filter box, when active, takes the bottom two lines.
	listHeight := height
	if t.filtering {
		listHeight -= 2
	}
	t.pageSize = listHeight
	t.clamp()

	if len(t.visibleTopics) == 0 {
		empty := "no topics"
		if t.filter != "" {
			empty = "nothing matches " + t.filter
		}
		return th.Dim.Render("  " + empty)
	}

	lines := make([]string, 0, listHeight)
	end := min(t.offset+listHeight, len(t.visibleTopics))

	for i := t.offset; i < end; i++ {
		topic := t.visibleTopics[i]

		box := "[ ]"
		if t.selected[topic.Name] {
			box = "[x]"
		}

		name := topic.Name
		if !topic.Healthy() {
			name += " " + th.Glyph("▲", "!")
		}

		line := " " + box + " " + panel.Truncate(name, max(width-6, 4))
		switch {
		case i == t.cursor && t.focused:
			line = th.TableRowSelected.Render(pad(line, width))
		case i == t.cursor:
			line = th.TableRowUnfocused.Render(pad(line, width))
		case t.selected[topic.Name]:
			line = th.Success.Render(line)
		case !topic.Healthy():
			line = th.Warning.Render(line)
		default:
			line = th.Value.Render(line)
		}
		lines = append(lines, line)
	}

	for len(lines) < listHeight {
		lines = append(lines, "")
	}

	if t.filtering {
		lines = append(lines,
			th.Dim.Render(" "+strings.Repeat("─", max(width-2, 1))),
			th.Highlight.Render(" /"+t.filter)+th.Dim.Render("▏"),
		)
	}
	return lipgloss.JoinVertical(lipgloss.Left, lines...)
}

// Counter is the "[ 2 / 142 ]" text for the panel border.
func (t *topicList) Counter() string {
	selected := len(t.Selected())
	if selected == 0 {
		return itoa(len(t.visibleTopics))
	}
	return itoa(selected) + " / " + itoa(len(t.visibleTopics))
}

func pad(s string, width int) string {
	if w := lipgloss.Width(s); w < width {
		return s + strings.Repeat(" ", width-w)
	}
	return s
}
