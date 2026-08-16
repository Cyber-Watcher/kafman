package monitoring

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/ansi"

	"github.com/Cyber-Watcher/kafman/internal/config"
	"github.com/Cyber-Watcher/kafman/internal/ui/ktx"
	"github.com/Cyber-Watcher/kafman/internal/ui/msg"
	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

func TestGroupsSectionShowsReadRate(t *testing.T) {
	m := newTab(t, growingMock(600), 3)

	openSection(m, sectionGroups)
	view := render(t, m)

	for _, want := range []string{
		"Produce vs consume", "produce", "consume", // the chart and its legend
		"READ/S", "MEMBERS", "NO COMMIT", "STATE", // the table header
		"billing-worker", "Stable",
		"total lag", // the panel counter
	} {
		if !strings.Contains(view, want) {
			t.Errorf("the Groups section should show %q:\n%s", want, view)
		}
	}
}

// One poll: a read rate has nothing to compare against, and the column shows a
// dash rather than a zero.
func TestReadRateColumnNeedsTwoPolls(t *testing.T) {
	m := newTab(t, growingMock(600), 1)

	openSection(m, sectionGroups)
	if view := render(t, m); !strings.Contains(view, "—") {
		t.Errorf("a read rate without a second poll should read as a dash:\n%s", view)
	}
}

// A group that never committed on some assigned partitions is distinguishable
// from one that is merely caught up.
func TestNoCommitIsDistinguishedFromZeroLag(t *testing.T) {
	m := newTab(t, growingMock(600), 3)

	openSection(m, sectionGroups)
	view := render(t, m)

	// idle-consumer has lag 0 AND two never-committed partitions; the count
	// is on its row.
	line := ""
	for _, l := range strings.Split(view, "\n") {
		if strings.Contains(l, "idle-consumer") {
			line = l
			break
		}
	}
	if line == "" {
		t.Fatalf("idle-consumer should be listed:\n%s", view)
	}
	if !strings.Contains(line, "2") {
		t.Errorf("the never-committed count should be on the row: %q", line)
	}
}

func TestGroupsChartTogglesPerGroup(t *testing.T) {
	m := newTab(t, growingMock(600), 3)

	openSection(m, sectionGroups)
	m.Update(key("t"))
	if view := render(t, m); !strings.Contains(view, "Consume by group") {
		t.Errorf("t should switch the chart to per-group lines:\n%s", view)
	}
	m.Update(key("t"))
	if view := render(t, m); !strings.Contains(view, "Produce vs consume") {
		t.Errorf("t again should switch back to the totals:\n%s", view)
	}
}

func TestGroupsSectionTabRing(t *testing.T) {
	m := newTab(t, growingMock(600), 2)

	openSection(m, sectionGroups)
	if m.selectorFocused || !m.sectionGroupTable.Focused() {
		t.Fatal("entering the section should focus its table")
	}
	m.Update(tea.KeyMsg{Type: tea.KeyTab})
	if !m.selectorFocused || m.sectionGroupTable.Focused() {
		t.Error("Tab should move to the section list")
	}
	m.Update(tea.KeyMsg{Type: tea.KeyTab})
	if m.selectorFocused || !m.sectionGroupTable.Focused() {
		t.Error("Tab should come back to the table")
	}
}

func TestEnterOnGroupsSectionJumpsToManage(t *testing.T) {
	m := newTab(t, growingMock(600), 3)

	openSection(m, sectionGroups)
	cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	if cmd == nil {
		t.Fatal("enter on a group should produce a jump")
	}
	if focus, ok := cmd().(msg.FocusGroup); !ok || focus.Group == "" {
		t.Fatalf("expected a FocusGroup message, got %T", cmd())
	}
}

// The groups chart and table are partly hand-drawn; nothing may overflow the
// frame at any width or height.
func TestGroupsSectionFitsTheFrame(t *testing.T) {
	m := newTab(t, growingMock(600), 3)

	cfg, err := config.LoadFrom([]byte("clusters:\n  - name: c\n    brokers: [h:9092]\n"), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	openSection(m, sectionGroups)
	for _, size := range [][2]int{{61, 16}, {80, 24}, {130, 30}, {200, 60}} {
		k := ktx.New(cfg, theme.New(theme.Dark, false), size[0], size[1])
		for i, line := range strings.Split(m.View(k), "\n") {
			if got := ansi.StringWidth(line); got > size[0] {
				t.Errorf("at %dx%d line %d is %d wide: %q",
					size[0], size[1], i, got, ansi.Strip(line))
			}
		}
	}
}

// The splitter hands out exactly what it was given.
func TestGroupsHeightsAlwaysFit(t *testing.T) {
	for available := 1; available <= 60; available++ {
		for groups := 0; groups <= 30; groups++ {
			chart, table := groupsHeights(available, groups)
			if chart < 0 || table < 0 {
				t.Fatalf("groupsHeights(%d, %d) went negative: %d/%d", available, groups, chart, table)
			}
			if chart+table != max(available, 0) {
				t.Errorf("groupsHeights(%d, %d) uses %d rows", available, groups, chart+table)
			}
		}
	}
}
