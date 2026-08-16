// Package chrome renders the frame around every tab: the tab bar, the context
// line with the cluster chip and breadcrumbs, and the contextual status bar.
package chrome

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

// TabBar draws the row of tab titles with the active one highlighted.
func TabBar(th *theme.Theme, titles []string, active int, width int) string {
	rendered := make([]string, len(titles))
	for i, title := range titles {
		if i == active {
			rendered[i] = th.TabActive.Render(title)
			continue
		}
		rendered[i] = th.TabInactive.Render(title)
	}

	left := strings.Join(rendered, th.Dim.Render("│"))
	right := th.Dim.Render("«F1» help ")

	gap := width - lipgloss.Width(left) - lipgloss.Width(right)
	if gap < 1 {
		// Too narrow for the help hint: the tabs matter more.
		return truncate(left, width)
	}
	return left + strings.Repeat(" ", gap) + right
}

// ConnState is how the connection to the current cluster is doing.
type ConnState int

const (
	Connecting ConnState = iota
	Connected
	Degraded
	Disconnected
)

// ContextLine draws the cluster chip, the breadcrumb trail and the connection
// state with a clock.
//
// The cluster chip carries the colour from the config: making production
// visually alarming is the cheapest safety feature in the whole tool.
func ContextLine(th *theme.Theme, cluster, colour string, crumbs []string, state ConnState, brokers int, width int) string {
	chip := th.ClusterChip(cluster, colour)

	separator := th.Glyph(" ▸ ", " > ")

	trail := ""
	if len(crumbs) > 0 {
		parts := make([]string, len(crumbs))
		for i, c := range crumbs {
			if i == len(crumbs)-1 {
				parts[i] = th.Highlight.Render(c)
				continue
			}
			parts[i] = th.Dim.Render(c)
		}
		trail = "  " + strings.Join(parts, th.Dim.Render(separator))
	}

	status := connStatus(th, state, brokers)
	clock := th.Dim.Render(time.Now().Format("15:04:05"))
	right := status + "  " + clock

	left := " " + chip + trail
	gap := width - lipgloss.Width(left) - lipgloss.Width(right) - 1
	if gap < 1 {
		return truncate(left, width)
	}
	return left + strings.Repeat(" ", gap) + right + " "
}

func connStatus(th *theme.Theme, state ConnState, brokers int) string {
	// Glyph falls back to ASCII when plain-fonts is set, so the status stays
	// legible in a terminal without a symbol font.
	switch state {
	case Connected:
		return th.Success.Render(th.Glyph("●", "*")+" connected") +
			th.Dim.Render(fmt.Sprintf("  %d brokers", brokers))
	case Degraded:
		return th.Warning.Render(th.Glyph("⚠", "!")+" degraded") +
			th.Dim.Render(fmt.Sprintf("  %d brokers", brokers))
	case Disconnected:
		return th.Critical.Render(th.Glyph("✖", "x") + " disconnected")
	default:
		return th.Warning.Render(th.Glyph("◌", "o") + " connecting")
	}
}

// Shortcut is one key binding shown in the status bar.
type Shortcut struct {
	Key   string
	Label string
}

// StatusBar renders the shortcuts of whatever currently has focus, dropping
// entries from the right when they do not fit.
//
// The list is rebuilt every frame from the focused view, so the footer can
// never advertise a key that does nothing here.
func StatusBar(th *theme.Theme, shortcuts []Shortcut, width int) string {
	var (
		b     strings.Builder
		first = true
		seen  = make(map[string]bool, len(shortcuts))
	)
	for _, s := range shortcuts {
		// A view's own shortcuts are followed by the global ones, and an overlay
		// that binds Esc would otherwise advertise it twice.
		if seen[s.Key] {
			continue
		}
		seen[s.Key] = true

		entry := th.ShortcutKey.Render("["+s.Key+"]") + th.ShortcutLabel.Render(" "+s.Label)
		sep := "  "
		if first {
			sep = " "
		}
		if lipgloss.Width(b.String())+lipgloss.Width(entry)+len(sep) > width {
			break
		}
		b.WriteString(sep + entry)
		first = false
	}
	return b.String()
}

// Notification levels.
type Level int

const (
	Info Level = iota
	Success
	Warning
	Error
)

// Notification is a transient message shown at the right of the context line.
type Notification struct {
	Text    string
	Level   Level
	Expires time.Time
}

// Expired reports whether the notification should no longer be drawn.
func (n Notification) Expired() bool {
	return n.Text == "" || time.Now().After(n.Expires)
}

// Render styles a notification according to its level.
func (n Notification) Render(th *theme.Theme) string {
	if n.Expired() {
		return ""
	}
	switch n.Level {
	case Success:
		return th.Success.Render(th.Glyph("✔", "+") + " " + n.Text)
	case Warning:
		return th.Warning.Render(th.Glyph("▲", "!") + " " + n.Text)
	case Error:
		return th.Critical.Render(th.Glyph("✖", "x") + " " + n.Text)
	default:
		return th.Dim.Render(n.Text)
	}
}

// Notify builds a notification that lasts for the given duration.
func Notify(text string, level Level, ttl time.Duration) Notification {
	return Notification{Text: text, Level: level, Expires: time.Now().Add(ttl)}
}

func truncate(s string, width int) string {
	if lipgloss.Width(s) <= width {
		return s
	}
	// Cheap cut: chrome strings are short and this only happens on a very narrow
	// terminal, where the layout has already degraded.
	runes := []rune(s)
	for len(runes) > 0 && lipgloss.Width(string(runes)) > width {
		runes = runes[:len(runes)-1]
	}
	return string(runes)
}
