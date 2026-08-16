// Package clusters implements the Clusters tab: the list of configured
// clusters, their connection state, and the forms to add or edit them.
//
// Switching between clusters is a core requirement, so it is deliberately one
// keystroke from anywhere (Ctrl+K opens the same list as a modal).
package clusters

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/Cyber-Watcher/kafman/internal/config"
	"github.com/Cyber-Watcher/kafman/internal/kafka"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/chrome"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/modal"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/panel"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/table"
	"github.com/Cyber-Watcher/kafman/internal/ui/ktx"
	"github.com/Cyber-Watcher/kafman/internal/ui/msg"
	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

// testState is what is known about one cluster's reachability.
type testState struct {
	tested  bool
	testing bool
	result  kafka.ConnCheck
}

// mode is what the tab is currently showing.
type mode int

const (
	modeList mode = iota
	modeForm
	modeConfirmDelete
)

// Model is the Clusters tab.
type Model struct {
	cfg   *config.Config
	table *table.Model

	states map[string]*testState

	mode    mode
	form    *clusterForm
	confirm *modal.Confirm

	// currentName is the cluster the application is connected to; it gets the
	// live connection state rather than a stored test result.
	currentName string
	currentConn chrome.ConnState

	// plain mirrors the theme's plain-fonts setting. The rows are built without
	// the theme in reach, so View picks the flag up and rebuilds when it changes.
	plain bool
}

// New builds the tab.
func New(cfg *config.Config, current string) *Model {
	m := &Model{
		cfg:         cfg,
		states:      make(map[string]*testState),
		currentName: current,
		currentConn: chrome.Connecting,
	}

	m.table = table.New(
		table.Column{Title: "NAME", Width: 22},
		table.Column{Title: "BROKERS", Width: 0},
		table.Column{Title: "SECURITY", Width: 28},
		table.Column{Title: "JMX", Width: 4},
		table.Column{Title: "STATUS", Width: 16},
	)
	m.table.Focus()
	m.rebuildRows()
	return m
}

// Title implements ui.Tab.
func (m *Model) Title() string { return "Clusters" }

// Init implements ui.Tab.
func (m *Model) Init() tea.Cmd {
	m.rebuildRows()
	return nil
}

// SetResult records the outcome of connecting to a cluster, so the details
// panel can show the whole error. The context line only has room for a
// truncated version.
func (m *Model) SetResult(name string, result kafka.ConnCheck) {
	m.states[name] = &testState{tested: true, result: result}
	m.rebuildRows()
}

// SetCurrent records which cluster is connected and in what state.
func (m *Model) SetCurrent(name string, state chrome.ConnState) {
	m.currentName, m.currentConn = name, state
	m.rebuildRows()
}

func (m *Model) rebuildRows() {
	rows := make([]table.Row, 0, len(m.cfg.Clusters))
	for i := range m.cfg.Clusters {
		cl := &m.cfg.Clusters[i]

		jmx := "no"
		if cl.JMX.Enabled {
			jmx = "yes"
		}

		rows = append(rows, table.Row{
			Cells: []string{
				m.namePrefix(cl.Name) + cl.Name,
				summariseBrokers(cl.Brokers),
				cl.SecurityProfile(),
				jmx,
				m.statusText(cl.Name),
			},
			Data: cl,
		})
	}
	m.table.SetRows(rows)
}

// marker returns the Nerd Font glyph, or its ASCII stand-in when the theme runs
// with plain fonts.
func (m *Model) marker(nerd, ascii string) string {
	if m.plain {
		return ascii
	}
	return nerd
}

// namePrefix marks the cluster the application is currently connected to.
func (m *Model) namePrefix(name string) string {
	if name == m.currentName {
		return m.marker("▸", ">") + " "
	}
	return "  "
}

func (m *Model) statusText(name string) string {
	if name == m.currentName {
		switch m.currentConn {
		case chrome.Connected:
			return m.marker("●", "*") + " connected"
		case chrome.Degraded:
			return m.marker("⚠", "!") + " degraded"
		case chrome.Disconnected:
			return m.marker("✖", "x") + " failed"
		default:
			return m.marker("◌", ".") + " connecting"
		}
	}

	state, ok := m.states[name]
	switch {
	case !ok:
		return m.marker("○", "o") + " not tested"
	case state.testing:
		return m.marker("◌", ".") + " testing"
	case state.result.OK():
		return m.marker("●", "*") + " reachable"
	default:
		return m.marker("✖", "x") + " failed"
	}
}

// SelectedCluster returns the highlighted cluster, or nil.
func (m *Model) SelectedCluster() *config.Cluster {
	row := m.table.SelectedRow()
	if row == nil {
		return nil
	}
	cl, _ := row.Data.(*config.Cluster)
	return cl
}

// Shortcuts implements ui.ShortcutProvider.
func (m *Model) Shortcuts() []chrome.Shortcut {
	switch m.mode {
	case modeForm:
		return []chrome.Shortcut{
			{Key: "Tab", Label: "Next field"},
			{Key: "Enter", Label: "Confirm"},
			{Key: "Esc", Label: "Cancel"},
		}
	case modeConfirmDelete:
		return []chrome.Shortcut{{Key: "Enter", Label: "Delete"}, {Key: "Esc", Label: "Cancel"}}
	default:
		return []chrome.Shortcut{
			{Key: "Enter", Label: "Switch"},
			{Key: "t", Label: "Test"},
			{Key: "Ctrl+N", Label: "Add"},
			{Key: "Ctrl+E", Label: "Edit"},
			{Key: "F2", Label: "Remove"},
		}
	}
}

// Update implements ui.View.
func (m *Model) Update(message tea.Msg) tea.Cmd {
	switch typed := message.(type) {
	case msg.ClusterTested:
		// Recorded unconditionally: a test can also be started from the edit
		// form, where no row was marked as testing beforehand.
		m.states[typed.Name] = &testState{tested: true, result: typed.Result}
		m.rebuildRows()
		return nil

	case msg.ConfigChanged:
		m.rebuildRows()
		return nil

	case tea.KeyMsg:
		switch m.mode {
		case modeForm:
			return m.updateForm(typed)
		case modeConfirmDelete:
			return m.updateConfirm(typed)
		default:
			return m.updateList(typed)
		}
	}
	return nil
}

func (m *Model) updateList(key tea.KeyMsg) tea.Cmd {
	switch key.String() {
	case "enter":
		cl := m.SelectedCluster()
		if cl == nil || cl.Name == m.currentName {
			return nil
		}
		return func() tea.Msg { return msg.SwitchCluster{Name: cl.Name} }

	case "t":
		cl := m.SelectedCluster()
		if cl == nil {
			return nil
		}
		m.states[cl.Name] = &testState{testing: true}
		m.rebuildRows()
		return func() tea.Msg { return msg.TestCluster{Name: cl.Name} }

	case "ctrl+n":
		m.form = newClusterForm(nil, m.cfg)
		m.mode = modeForm
		return nil

	case "ctrl+e":
		cl := m.SelectedCluster()
		if cl == nil {
			return nil
		}
		m.form = newClusterForm(cl, m.cfg)
		m.mode = modeForm
		return nil

	case "f2":
		cl := m.SelectedCluster()
		if cl == nil {
			return nil
		}
		if len(m.cfg.Clusters) == 1 {
			return notify("the last cluster cannot be removed", msg.NotifyWarning)
		}
		m.confirm = modal.NewConfirm("Remove cluster",
			fmt.Sprintf("Remove %q from the configuration?", cl.Name))
		m.confirm.Details = "The cluster itself is untouched; only this entry is deleted."
		m.mode = modeConfirmDelete
		return nil
	}

	return m.table.Update(key)
}

func (m *Model) updateForm(key tea.KeyMsg) tea.Cmd {
	cmd := m.form.Update(key)

	switch {
	case m.form.Cancelled():
		m.mode, m.form = modeList, nil
		return nil

	case m.form.SubmittedTest():
		cluster, err := m.form.Cluster()
		if err != nil {
			return notify(err.Error(), msg.NotifyError)
		}
		m.form.markTesting()
		return func() tea.Msg { return msg.TestCluster{Name: cluster.Name, Cluster: cluster} }

	case m.form.SubmittedSave():
		cluster, err := m.form.Cluster()
		if err != nil {
			return notify(err.Error(), msg.NotifyError)
		}
		if err := m.save(cluster); err != nil {
			return notify(err.Error(), msg.NotifyError)
		}
		m.mode, m.form = modeList, nil
		m.rebuildRows()
		return tea.Batch(
			notify(fmt.Sprintf("cluster %q saved", cluster.Name), msg.NotifySuccess),
			func() tea.Msg { return msg.ConfigChanged{} },
		)
	}
	return cmd
}

func (m *Model) save(cluster config.Cluster) error {
	if m.form.editing {
		return m.cfg.UpdateCluster(cluster)
	}
	return m.cfg.AddCluster(cluster)
}

func (m *Model) updateConfirm(key tea.KeyMsg) tea.Cmd {
	m.confirm.Update(key)

	switch {
	case m.confirm.Cancelled():
		m.mode, m.confirm = modeList, nil
		return nil

	case m.confirm.Confirmed():
		cl := m.SelectedCluster()
		m.mode, m.confirm = modeList, nil
		if cl == nil {
			return nil
		}
		name := cl.Name
		if err := m.cfg.RemoveCluster(name); err != nil {
			return notify(err.Error(), msg.NotifyError)
		}
		delete(m.states, name)
		m.rebuildRows()
		return tea.Batch(
			notify(fmt.Sprintf("cluster %q removed", name), msg.NotifySuccess),
			func() tea.Msg { return msg.ConfigChanged{} },
		)
	}
	return nil
}

func notify(text string, level msg.NotifyLevel) tea.Cmd {
	return func() tea.Msg { return msg.Notify{Text: text, Level: level} }
}

// View implements ui.View.
func (m *Model) View(k *ktx.Context) string {
	if m.mode == modeForm {
		return m.form.View(k)
	}

	th := k.Theme
	width, height := k.WindowWidth, k.AvailableHeight

	if m.plain != th.PlainFonts {
		m.plain = th.PlainFonts
		m.rebuildRows()
	}

	// The details panel grows to fit its content — a failed connection needs the
	// room for its diagnosis and full error — but never takes more than half the
	// screen, so the list stays usable.
	details := m.detailsBody(th, width-2)
	detailsHeight := min(strings.Count(details, "\n")+3, height/2)
	detailsHeight = max(detailsHeight, 5)
	listHeight := height - detailsHeight

	list := panel.Panel{
		Title:    "Clusters",
		TopRight: fmt.Sprintf("config: %s", shortenPath(k.Config.Path())),
		Focused:  m.mode == modeList,
		Width:    width,
		Height:   listHeight,
	}.Render(th, m.table.View(th, width-2, listHeight-2))

	detailsPanel := panel.Panel{
		Title:  m.detailsTitle(),
		Width:  width,
		Height: detailsHeight,
	}.Render(th, details)

	body := lipgloss.JoinVertical(lipgloss.Left, list, detailsPanel)

	if m.mode == modeConfirmDelete {
		return modal.Overlay(body, m.confirm.View(th, min(width-10, 70)))
	}
	return body
}

func (m *Model) detailsTitle() string {
	if cl := m.SelectedCluster(); cl != nil {
		return cl.Name
	}
	return "Details"
}

// detailsBody describes the selected cluster. Passwords and passphrases are
// never rendered: this panel is on screen while people share their terminal.
func (m *Model) detailsBody(th *theme.Theme, width int) string {
	cl := m.SelectedCluster()
	if cl == nil {
		return th.Dim.Render("  no cluster selected")
	}

	label := func(s string) string { return th.Label.Render(padRight(s, 16)) }
	lines := []string{
		"  " + label("Bootstrap") + th.Value.Render(strings.Join(cl.Brokers, ", ")),
		"  " + label("Client") + th.Value.Render(fmt.Sprintf(
			"version %s   client-id %s   timeout %s",
			orDefault(cl.KafkaVersion, "auto"), cl.ClientID, cl.RequestTimeout)),
	}

	if cl.TLS.Enabled {
		tls := "enabled"
		if cl.TLS.CA != "" {
			tls += "   CA " + cl.TLS.CA
		}
		if cl.TLS.Cert != "" {
			tls += "   client cert " + cl.TLS.Cert
		}
		if cl.TLS.Insecure {
			tls += "   " + th.Warning.Render("verification disabled")
		}
		lines = append(lines, "  "+label("TLS")+th.Value.Render(tls))
	}

	if cl.SASL.Enabled {
		lines = append(lines, "  "+label("SASL")+th.Value.Render(fmt.Sprintf(
			"%s   user %s   password %s",
			cl.SASL.Mechanism, cl.SASL.Username, th.Dim.Render("(hidden)"))))
	}

	if cl.JMX.Enabled {
		lines = append(lines, "  "+label("JMX")+th.Value.Render(
			fmt.Sprintf("%d target(s)", len(cl.JMX.Targets))))
	}

	if cl.NodeExporter.Enabled {
		lines = append(lines, "  "+label("Node exp")+th.Value.Render(
			fmt.Sprintf("%d target(s)", len(cl.NodeExporter.Targets))))
	}

	// A failed test is the most useful thing this panel can show, so it gets the
	// full error rather than a truncated one.
	if state, ok := m.states[cl.Name]; ok && state.tested && !state.result.OK() {
		lines = append(lines, "")
		if d := state.result.Diagnosis; d != "" {
			lines = append(lines, "  "+th.Warning.Render(wrapAt(d, width-4, "  ")))
		}
		lines = append(lines, "  "+th.Dim.Render(wrapAt(state.result.Err.Error(), width-4, "  ")))
	}

	return lipgloss.JoinVertical(lipgloss.Left, lines...)
}

// ------------------------------------------------------------------ helpers ---

func summariseBrokers(brokers []string) string {
	switch len(brokers) {
	case 0:
		return ""
	case 1:
		return brokers[0]
	default:
		return fmt.Sprintf("%s +%d", brokers[0], len(brokers)-1)
	}
}

func shortenPath(path string) string {
	if len(path) <= 40 {
		return path
	}
	return "…" + path[len(path)-39:]
}

func orDefault(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

func padRight(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}

// wrapAt breaks text on word boundaries, indenting continuation lines.
func wrapAt(s string, width int, indent string) string {
	words := strings.Fields(s)
	if len(words) == 0 || width <= 0 {
		return ""
	}

	var (
		b       strings.Builder
		lineLen int
	)
	for i, w := range words {
		switch {
		case i == 0:
			b.WriteString(w)
			lineLen = len(w)
		case lineLen+1+len(w) > width:
			b.WriteString("\n" + indent + w)
			lineLen = len(w)
		default:
			b.WriteString(" " + w)
			lineLen += 1 + len(w)
		}
	}
	return b.String()
}

// AcceptsText reports whether a form is capturing keystrokes, so the root model
// knows not to treat letters and digits as global shortcuts.
func (m *Model) AcceptsText() bool { return m.mode == modeForm }
