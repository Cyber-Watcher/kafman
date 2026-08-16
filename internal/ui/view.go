package ui

import (
	tea "github.com/charmbracelet/bubbletea"

	"github.com/Cyber-Watcher/kafman/internal/kafka"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/chrome"
	"github.com/Cyber-Watcher/kafman/internal/ui/ktx"
)

// View is what every screen, tab and panel implements.
//
// Update returns only a tea.Cmd, not a new model: nested views are pointers and
// mutate themselves. That removes the `m.child, cmd = m.child.Update(msg)`
// reassignment that makes deep Bubble Tea trees tedious. Only the root Model
// implements tea.Model proper. Pattern taken from ktea's ui.View.
type View interface {
	Update(msg tea.Msg) tea.Cmd
	View(k *ktx.Context) string
}

// Shortcut is one entry in the status bar. It is chrome's type so there is a
// single definition: chrome renders it, everything else produces it.
type Shortcut = chrome.Shortcut

// ShortcutProvider is implemented by whatever currently has focus. The status
// bar reads it on every frame, so the footer can never claim a key that does
// nothing on the current screen.
type ShortcutProvider interface {
	Shortcuts() []Shortcut
}

// ClientAware is implemented by tabs that talk to Kafka. The root hands the
// live client over on every successful connection, and nil when the connection
// is lost, so a tab never holds a client belonging to a cluster we have left.
type ClientAware interface {
	SetClient(client kafka.Facade)
}

// Focusable is implemented by panels that take part in the Tab cycle.
type Focusable interface {
	Focus()
	Blur()
	Focused() bool
}

// Tab is a top-level screen behind one entry in the tab bar.
type Tab interface {
	View
	ShortcutProvider

	// Title is the label in the tab bar.
	Title() string

	// Init is called when the tab is first shown, and again after a cluster
	// switch, so it can start loading.
	Init() tea.Cmd
}

// Overlay is a view pushed on top of a tab: record details, a modal, help.
// Esc pops it.
type Overlay interface {
	View
	ShortcutProvider

	// Name appears in the breadcrumb trail.
	Name() string
}
