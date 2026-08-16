package ui

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/ansi"

	"github.com/Cyber-Watcher/kafman/internal/config"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/chrome"
	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

// newTestModel builds a root model sized to a typical terminal.
func newTestModel(t *testing.T, width, height int) *Model {
	t.Helper()

	cfg, err := config.LoadFrom([]byte(
		"current-cluster: local\n"+
			"clusters:\n"+
			"  - name: local\n    color: \"#00D787\"\n    brokers: [localhost:9092]\n"+
			"  - name: staging\n    brokers: [stg:9092]\n"), t.TempDir())
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	cluster, err := cfg.Current()
	if err != nil {
		t.Fatal(err)
	}

	m := New(Options{Config: cfg, Cluster: cluster, LogPath: "/tmp/kafman.log"})
	m.Update(tea.WindowSizeMsg{Width: width, Height: height})
	return m
}

// plain strips styling so assertions match on text, not escape codes.
func plain(s string) string { return ansi.Strip(s) }

func TestFrameLayout(t *testing.T) {
	m := newTestModel(t, 100, 30)
	view := plain(m.View())
	lines := strings.Split(view, "\n")

	if len(lines) != 30 {
		t.Errorf("frame is %d lines, want exactly the window height of 30", len(lines))
	}
	for i, line := range lines {
		if w := ansi.StringWidth(line); w > 100 {
			t.Errorf("line %d is %d cells wide, wider than the 100-cell window: %q", i, w, line)
		}
	}

	// Tab bar, context line and status bar must all be present.
	if !strings.Contains(lines[0], "Overview") || !strings.Contains(lines[0], "Clusters") {
		t.Errorf("first line should be the tab bar, got %q", lines[0])
	}
	if !strings.Contains(lines[1], "local") {
		t.Errorf("second line should carry the cluster chip, got %q", lines[1])
	}
	last := lines[len(lines)-1]
	if !strings.Contains(last, "F1") || !strings.Contains(last, "Help") {
		t.Errorf("last line should be the status bar, got %q", last)
	}
}

func TestTabNavigation(t *testing.T) {
	m := newTestModel(t, 100, 30)

	if got := m.tabs[m.active].Title(); got != "Overview" {
		t.Errorf("initial tab = %q, want Overview", got)
	}

	m.Update(tea.KeyMsg{Type: tea.KeyCtrlL})
	if got := m.tabs[m.active].Title(); got != "Messages" {
		t.Errorf("after ctrl+l, tab = %q, want Messages", got)
	}

	m.Update(tea.KeyMsg{Type: tea.KeyCtrlH})
	if got := m.tabs[m.active].Title(); got != "Overview" {
		t.Errorf("after ctrl+h, tab = %q, want Overview", got)
	}

	// Moving left from the first tab must stay put rather than wrap or crash.
	m.Update(tea.KeyMsg{Type: tea.KeyCtrlH})
	if m.active != 0 {
		t.Errorf("active = %d, want it clamped at 0", m.active)
	}

	// Number keys jump directly.
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("5")})
	if got := m.tabs[m.active].Title(); got != "Clusters" {
		t.Errorf("after 5, tab = %q, want Clusters", got)
	}
}

func TestOverlayStack(t *testing.T) {
	m := newTestModel(t, 100, 30)

	if len(m.overlays) != 0 {
		t.Fatal("no overlay should be open initially")
	}

	m.Update(tea.KeyMsg{Type: tea.KeyF1})
	if len(m.overlays) != 1 {
		t.Fatalf("F1 should open help, got %d overlays", len(m.overlays))
	}

	// The breadcrumb trail must show where we are.
	if got := plain(m.contextLine()); !strings.Contains(got, "Help") {
		t.Errorf("context line should show the Help crumb, got %q", got)
	}

	// Help content must be visible over the tab.
	if view := plain(m.View()); !strings.Contains(view, "keys and diagnostics") {
		t.Error("the help overlay is not visible in the frame")
	}

	m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	if len(m.overlays) != 0 {
		t.Errorf("Esc should pop the overlay, %d left", len(m.overlays))
	}
}

func TestEscapeDoesNotQuit(t *testing.T) {
	m := newTestModel(t, 100, 30)

	// With nothing to close, Esc must be inert: quitting is q or Ctrl+C.
	_, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	if cmd != nil {
		if msg := cmd(); msg != nil {
			if _, isQuit := msg.(tea.QuitMsg); isQuit {
				t.Error("Esc must not quit the application")
			}
		}
	}
}

func TestQuitKeys(t *testing.T) {
	for _, key := range []tea.KeyMsg{
		{Type: tea.KeyRunes, Runes: []rune("q")},
		{Type: tea.KeyCtrlC},
	} {
		t.Run(key.String(), func(t *testing.T) {
			m := newTestModel(t, 100, 30)
			_, cmd := m.Update(key)
			if cmd == nil {
				t.Fatal("expected a quit command")
			}
			if _, isQuit := cmd().(tea.QuitMsg); !isQuit {
				t.Error("expected tea.QuitMsg")
			}
		})
	}
}

func TestConnectionStateIsShown(t *testing.T) {
	m := newTestModel(t, 100, 30)

	if got := plain(m.contextLine()); !strings.Contains(got, "connecting") {
		t.Errorf("context line should start as connecting, got %q", got)
	}

	m.conn = chrome.Connected
	m.notification = chrome.Notification{} // clear so the state itself is visible
	if got := plain(m.contextLine()); !strings.Contains(got, "connected") {
		t.Errorf("context line should show connected, got %q", got)
	}

	m.conn = chrome.Disconnected
	if got := plain(m.contextLine()); !strings.Contains(got, "disconnected") {
		t.Errorf("context line should show disconnected, got %q", got)
	}
}

func TestTooSmallTerminal(t *testing.T) {
	m := newTestModel(t, 40, 10)
	view := plain(m.View())

	if !strings.Contains(view, "terminal too small") {
		t.Errorf("a tiny terminal should show a notice, got:\n%s", view)
	}
	// The notice must state what is needed, not just complain.
	if !strings.Contains(view, "60") {
		t.Error("the notice should name the minimum size")
	}
}

func TestResizeIsHandled(t *testing.T) {
	m := newTestModel(t, 100, 30)

	for _, size := range []struct{ w, h int }{{200, 60}, {80, 24}, {61, 16}} {
		m.Update(tea.WindowSizeMsg{Width: size.w, Height: size.h})
		view := plain(m.View())
		lines := strings.Split(view, "\n")

		if len(lines) != size.h {
			t.Errorf("at %dx%d the frame is %d lines, want %d", size.w, size.h, len(lines), size.h)
		}
		for i, line := range lines {
			if w := ansi.StringWidth(line); w > size.w {
				t.Errorf("at %dx%d line %d is %d cells wide: %q", size.w, size.h, i, w, line)
			}
		}
	}
}

func TestStatusBarFollowsFocus(t *testing.T) {
	m := newTestModel(t, 100, 30)

	base := plain(chrome.StatusBar(m.theme, m.shortcuts(), 100))
	if !strings.Contains(base, "Quit") {
		t.Errorf("the tab status bar should offer Quit, got %q", base)
	}

	m.Update(tea.KeyMsg{Type: tea.KeyF1})
	withHelp := plain(chrome.StatusBar(m.theme, m.shortcuts(), 100))
	if !strings.Contains(withHelp, "Close") {
		t.Errorf("with an overlay open the status bar should offer Close, got %q", withHelp)
	}
	if strings.Contains(withHelp, "Quit") {
		t.Errorf("with an overlay open Esc closes rather than quits, got %q", withHelp)
	}
}

// TestRenderFrame prints a frame, so `go test -run TestRenderFrame -v ./internal/ui/`
// shows what the interface actually looks like — the alt screen cannot be
// captured from a script.
func TestRenderFrame(t *testing.T) {
	m := newTestModel(t, 100, 24)
	m.conn = chrome.Connected
	t.Logf("\n%s", m.View())
}

// TestRenderLightTheme prints a frame in the light theme with plain fonts, the
// combination least likely to be exercised by accident.
func TestRenderLightTheme(t *testing.T) {
	m := newTestModel(t, 100, 16)
	m.opts.Config.UI.Theme = "light"
	m.opts.Config.UI.PlainFonts = true
	m.theme = theme.ByName("light", true)
	m.conn = chrome.Degraded
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("5")})
	t.Logf("\n%s", m.View())
}
