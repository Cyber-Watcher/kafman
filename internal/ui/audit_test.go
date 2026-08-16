package ui

import (
	"fmt"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/ansi"

	"github.com/Cyber-Watcher/kafman/internal/config"
	"github.com/Cyber-Watcher/kafman/internal/kafka"
	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

// TestEveryTabFitsEveryTerminal renders all five tabs at four sizes, in both
// themes and with plain fonts, and checks the two invariants that break
// silently: a frame taller or wider than the terminal, and a Nerd Font glyph
// surviving plain-fonts mode. Both defects were found this way.
func TestEveryTabFitsEveryTerminal(t *testing.T) {
	sizes := [][2]int{{61, 16}, {80, 24}, {130, 34}, {200, 60}}
	themes := []struct {
		name  string
		kind  theme.Palette
		plain bool
	}{
		{"dark", theme.Dark, false},
		{"light", theme.Light, false},
		{"dark-plain", theme.Dark, true},
	}

	for _, th := range themes {
		for _, size := range sizes {
			for tab := range 5 {
				m := newAuditModel(t, th.kind, th.plain)
				m.Update(tea.WindowSizeMsg{Width: size[0], Height: size[1]})
				drainCmds(t, m, m.connectCurrent())
				m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(fmt.Sprint(tab + 1))})
				drainCmds(t, m, m.tabs[m.active].Init())

				view := m.View()
				label := fmt.Sprintf("%s %dx%d tab=%s", th.name, size[0], size[1], m.tabs[m.active].Title())
				m.monitoringTab.Close()

				lines := strings.Split(view, "\n")
				if len(lines) != size[1] {
					t.Errorf("%s: %d lines, want %d", label, len(lines), size[1])
				}
				for i, line := range lines {
					if w := ansi.StringWidth(line); w > size[0] {
						t.Errorf("%s: line %d is %d wide: %q", label, i, w, ansi.Strip(line))
					}
				}
				if th.plain {
					for _, glyph := range []string{"▸", "●", "◆", "⏺", "⦿", "◯", "⊗", "▲", "✖", "█"} {
						if strings.Contains(ansi.Strip(view), glyph) {
							t.Errorf("%s: plain fonts still render %q", label, glyph)
						}
					}
				}
			}
		}
	}
}

// drainCmds runs a command chain to completion, bounded.
func drainCmds(t *testing.T, m *Model, cmd tea.Cmd) {
	t.Helper()
	for range 8 {
		if cmd == nil {
			return
		}
		message := cmd()
		if message == nil {
			return
		}
		if batch, ok := message.(tea.BatchMsg); ok {
			for _, c := range batch {
				drainCmds(t, m, c)
			}
			return
		}
		_, cmd = m.Update(message)
	}
}

// newAuditModel builds a root model wired to the mock cluster in the given
// theme.
func newAuditModel(t *testing.T, palette theme.Palette, plainFonts bool) *Model {
	t.Helper()

	// A short refresh interval keeps the Monitoring tab's collector from making
	// this test wait five seconds per case for its second poll.
	cfg, err := config.LoadFrom([]byte(
		"current-cluster: local\n"+
			"ui:\n  refresh-interval: 20ms\n"+
			"clusters:\n"+
			"  - name: local\n    color: \"#00D787\"\n    brokers: [localhost:9092]\n"), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := cfg.Current()
	if err != nil {
		t.Fatal(err)
	}

	mock := kafka.NewMock(cluster)
	m := New(Options{
		Config: cfg, Cluster: cluster, LogPath: "/tmp/kafman.log",
		Instantiator: kafka.MockInstantiator(mock),
	})
	m.theme = theme.New(palette, plainFonts)
	return m
}
