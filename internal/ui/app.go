// Package ui holds the terminal user interface.
//
// The root Model owns the frame — tab bar, context line, status bar — plus the
// Kafka client and the config. Tabs never open connections or write the config
// themselves: they ask through internal/ui/msg, and the root performs it. That
// keeps a cluster switch, which has to tear down and rebuild everything, in one
// place.
package ui

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"

	"github.com/Cyber-Watcher/kafman/internal/config"
	"github.com/Cyber-Watcher/kafman/internal/kafka"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/chrome"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/help"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/modal"
	"github.com/Cyber-Watcher/kafman/internal/ui/ktx"
	"github.com/Cyber-Watcher/kafman/internal/ui/msg"
	"github.com/Cyber-Watcher/kafman/internal/ui/tabs/clusters"
	"github.com/Cyber-Watcher/kafman/internal/ui/tabs/manage"
	"github.com/Cyber-Watcher/kafman/internal/ui/tabs/messages"
	"github.com/Cyber-Watcher/kafman/internal/ui/tabs/monitoring"
	"github.com/Cyber-Watcher/kafman/internal/ui/tabs/overview"
	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

// Options carries what main() resolved from flags, the environment and the
// configuration file.
type Options struct {
	Config  *config.Config
	Cluster *config.Cluster
	LogPath string

	// Instantiator creates Kafka clients. Tests inject a mock through it.
	Instantiator kafka.Instantiator
}

// Chrome heights, subtracted from the window to size the tab body.
const (
	tabBarHeight      = 1
	contextLineHeight = 1
	statusBarHeight   = 1
	chromeHeight      = tabBarHeight + contextLineHeight + statusBarHeight
)

// notificationTTL is how long a message stays on the context line.
const notificationTTL = 5 * time.Second

// Tab indices used for cross-tab jumps.
const (
	messagesTabIndex = 1
	manageTabIndex   = 2
	clustersTabIndex = 4
)

// Model is the root Bubble Tea model.
type Model struct {
	opts  Options
	theme *theme.Theme

	width  int
	height int
	ready  bool

	tabs          []Tab
	clustersTab   *clusters.Model
	overviewTab   *overview.Model
	messagesTab   *messages.Model
	manageTab     *manage.Model
	monitoringTab *monitoring.Model
	active        int

	// overlays is a stack: Esc pops the top one, and only the top receives keys.
	overlays []Overlay

	client  kafka.Facade
	conn    chrome.ConnState
	info    kafka.ClusterInfo
	spinner spinner.Model

	// switching suppresses tab input while clients are being rebuilt.
	switching bool

	notification chrome.Notification
	cancelConn   context.CancelFunc
}

var _ tea.Model = (*Model)(nil)

// New builds the root model.
func New(opts Options) *Model {
	if opts.Instantiator == nil {
		opts.Instantiator = kafka.NewFacade
	}

	th := theme.ByName(opts.Config.UI.Theme, opts.Config.UI.PlainFonts)

	sp := spinner.New()
	sp.Spinner = spinner.Dot
	sp.Style = lipgloss.NewStyle().Foreground(th.Warn)

	m := &Model{
		opts:    opts,
		theme:   th,
		spinner: sp,
		conn:    chrome.Connecting,
	}
	m.clustersTab = clusters.New(opts.Config, opts.Cluster.Name)
	m.overviewTab = overview.New()
	m.messagesTab = messages.New(opts.Config.UI.RecordBufferSize, opts.Config.UI.QueryHistory)
	m.messagesTab.SetContext(opts.Cluster.Name, opts.Config.UI.ExportDir)
	m.manageTab = manage.New()
	m.monitoringTab = monitoring.New(time.Duration(opts.Config.UI.RefreshInterval))
	m.tabs = m.buildTabs()
	return m
}

// buildTabs creates the five tabs.
func (m *Model) buildTabs() []Tab {
	return []Tab{
		m.overviewTab,
		m.messagesTab,
		m.manageTab,
		m.monitoringTab,
		m.clustersTab,
	}
}

// Init starts the connection and the spinner.
func (m *Model) Init() tea.Cmd {
	return tea.Batch(m.spinner.Tick, m.connectCurrent(), m.tabs[m.active].Init())
}

func (m *Model) connectCurrent() tea.Cmd {
	if m.cancelConn != nil {
		m.cancelConn()
	}
	ctx, cancel := context.WithTimeout(context.Background(), kafka.DefaultConnectTimeout)
	m.cancelConn = cancel

	m.conn = chrome.Connecting
	m.clustersTab.SetCurrent(m.opts.Cluster.Name, m.conn)
	return connect(ctx, m.opts.Cluster, m.opts.Instantiator)
}

func (m *Model) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	switch typed := message.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height, m.ready = typed.Width, typed.Height, true
		return m, nil

	case msg.Connected:
		return m, m.onConnected(typed)

	case msg.ConnectFailed:
		return m, m.onConnectFailed(typed)

	case msg.SwitchCluster:
		return m, m.switchCluster(typed.Name)

	case msg.TestCluster:
		return m, m.testCluster(typed)

	case msg.FocusTopic:
		// Overview is a dashboard: Enter on a row leads to the screen that acts
		// on it, with the topic already selected.
		cmds := []tea.Cmd{m.switchTab(messagesTabIndex), m.messagesTab.Update(typed)}
		return m, tea.Batch(cmds...)

	case msg.FocusGroup:
		cmds := []tea.Cmd{m.switchTab(manageTabIndex), m.manageTab.FocusGroup(typed)}
		return m, tea.Batch(cmds...)

	case msg.Notify:
		m.notification = chrome.Notify(typed.Text, chrome.Level(typed.Level), notificationTTL)
		return m, nil

	case spinner.TickMsg:
		if m.conn != chrome.Connecting {
			return m, nil
		}
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(typed)
		return m, cmd

	case tea.KeyMsg:
		return m, m.handleKey(typed)
	}

	return m, m.forward(message)
}

func (m *Model) onConnected(message msg.Connected) tea.Cmd {
	// A stale reply from a cluster we have already switched away from.
	if m.client != nil {
		if previous := m.client; previous != message.Client {
			m.client = nil
			defer func() { _ = previous.Close() }()
		}
	}

	m.client = message.Client
	m.info = message.Info
	m.conn = chrome.Connected
	m.switching = false
	m.publishClient(message.Client)
	m.clustersTab.SetCurrent(m.opts.Cluster.Name, m.conn)
	m.clustersTab.SetResult(m.opts.Cluster.Name, kafka.ConnCheck{
		Cluster: m.opts.Cluster, Info: message.Info, Latency: message.Latency,
	})

	slog.Info("cluster reachable",
		"cluster", m.opts.Cluster.Name,
		"brokers", len(message.Info.Brokers),
		"latency", message.Latency)

	m.notification = chrome.Notify(
		fmt.Sprintf("connected to %s in %v",
			m.opts.Cluster.Name, message.Latency.Round(time.Millisecond)),
		chrome.Success, notificationTTL)

	// The tab could not load anything before the client existed.
	return tea.Batch(m.forward(message), m.tabs[m.active].Init())
}

func (m *Model) onConnectFailed(message msg.ConnectFailed) tea.Cmd {
	m.conn = chrome.Disconnected
	m.switching = false
	m.info = kafka.ClusterInfo{}
	m.publishClient(nil)
	m.clustersTab.SetCurrent(m.opts.Cluster.Name, m.conn)
	m.clustersTab.SetResult(m.opts.Cluster.Name, kafka.ConnCheck{
		Cluster: m.opts.Cluster, Err: message.Err, Diagnosis: message.Diagnosis,
	})

	slog.Error("cluster unreachable", "cluster", m.opts.Cluster.Name, "error", message.Err)

	text := message.Diagnosis
	if text == "" {
		text = message.Err.Error()
	}
	m.notification = chrome.Notify(text, chrome.Error, notificationTTL)
	return m.forward(message)
}

// switchCluster tears down the current client, points the config at the new
// cluster, resets every tab and reconnects.
// publishClient hands the client to every tab that implements ClientAware, and
// nil when the connection is gone, so no tab keeps a client for a cluster we
// have left.
func (m *Model) publishClient(client kafka.Facade) {
	for _, tab := range m.tabs {
		if aware, ok := tab.(ClientAware); ok {
			aware.SetClient(client)
		}
	}
}

func (m *Model) switchCluster(name string) tea.Cmd {
	cluster, err := m.opts.Config.Cluster(name)
	if err != nil {
		return m.notify(err.Error(), chrome.Error)
	}
	if name == m.opts.Cluster.Name && m.conn == chrome.Connected {
		return nil
	}

	slog.Info("switching cluster", "from", m.opts.Cluster.Name, "to", name)

	// Persist the choice so the next run starts here.
	if err := m.opts.Config.SwitchCluster(name); err != nil {
		return m.notify(fmt.Sprintf("could not save the cluster choice: %v", err), chrome.Warning)
	}

	closing := closeClient(m.client)
	m.publishClient(nil)
	m.client = nil
	m.info = kafka.ClusterInfo{}
	m.opts.Cluster = cluster
	m.switching = true

	// Rebuilding the tabs discards any state tied to the old cluster.
	m.clustersTab = clusters.New(m.opts.Config, cluster.Name)
	m.overviewTab = overview.New()
	m.messagesTab = messages.New(m.opts.Config.UI.RecordBufferSize, m.opts.Config.UI.QueryHistory)
	m.messagesTab.SetContext(cluster.Name, m.opts.Config.UI.ExportDir)
	m.manageTab = manage.New()
	// The old collector polls the cluster we are leaving; it goes with the tab.
	m.monitoringTab.Close()
	m.monitoringTab = monitoring.New(time.Duration(m.opts.Config.UI.RefreshInterval))
	m.tabs = m.buildTabs()

	cmds := []tea.Cmd{closing, m.connectCurrent(), m.tabs[m.active].Init()}
	cmds = append(cmds, func() tea.Msg { return msg.ClusterSwitched{Cluster: cluster} })
	return tea.Batch(cmds...)
}

// testCluster checks a cluster other than the current one, or an unsaved
// definition coming from the edit form.
func (m *Model) testCluster(message msg.TestCluster) tea.Cmd {
	cluster := &message.Cluster
	if cluster.Name == "" {
		found, err := m.opts.Config.Cluster(message.Name)
		if err != nil {
			return m.notify(err.Error(), chrome.Error)
		}
		cluster = found
	}

	return testCluster(message.Name, cluster)
}

func (m *Model) notify(text string, level chrome.Level) tea.Cmd {
	m.notification = chrome.Notify(text, level, notificationTTL)
	return nil
}

// handleKey routes a key press: overlays first, then global bindings, then the
// active tab.
func (m *Model) handleKey(key tea.KeyMsg) tea.Cmd {
	if len(m.overlays) > 0 {
		top := m.overlays[len(m.overlays)-1]
		switch key.String() {
		case "esc":
			m.popOverlay()
			return nil
		case "ctrl+c":
			return m.quit()
		}
		return top.Update(key)
	}

	switch key.String() {
	case "ctrl+c":
		return m.quit()

	case "q":
		// While a form has focus, q is a letter, not a command.
		if m.tabAcceptsText() {
			break
		}
		return m.quit()

	case "f1":
		m.pushOverlay(help.New(m.opts.LogPath))
		return nil

	case "f5":
		return tea.Batch(m.connectCurrent(), m.tabs[m.active].Init())

	case "ctrl+k":
		return m.switchTab(clustersTabIndex)

	case "ctrl+right", "ctrl+l":
		return m.switchTab(m.active + 1)
	case "ctrl+left", "ctrl+h":
		return m.switchTab(m.active - 1)

	case "1", "2", "3", "4", "5":
		if m.tabAcceptsText() {
			break
		}
		return m.switchTab(int(key.String()[0] - '1'))
	}

	if m.switching {
		// Input during a cluster switch would act on a half-built state.
		return nil
	}
	return m.tabs[m.active].Update(key)
}

// tabAcceptsText reports whether the active tab is editing text, in which case
// plain letters and digits must reach it rather than trigger shortcuts.
func (m *Model) tabAcceptsText() bool {
	editor, ok := m.tabs[m.active].(interface{ AcceptsText() bool })
	return ok && editor.AcceptsText()
}

func (m *Model) quit() tea.Cmd {
	if m.cancelConn != nil {
		m.cancelConn()
	}
	m.saveQueryHistory()
	// The collector holds a goroutine and the client it polls with; stopping it
	// before the client closes keeps the last poll from failing on the way out.
	m.monitoringTab.Close()
	return tea.Sequence(closeClient(m.client), tea.Quit)
}

// saveQueryHistory persists the queries run this session. A failure here is
// logged rather than shown: the user is on their way out, and losing the
// history is not worth a dialog.
func (m *Model) saveQueryHistory() {
	history := m.messagesTab.QueryHistory()
	if len(history) == 0 {
		return
	}

	for _, entry := range history {
		m.opts.Config.AddQuery(entry)
	}
	if err := m.opts.Config.Save(); err != nil {
		slog.Warn("could not save the query history", "error", err)
	}
}

func (m *Model) switchTab(index int) tea.Cmd {
	if index < 0 || index >= len(m.tabs) || index == m.active {
		return nil
	}
	m.active = index
	return m.tabs[index].Init()
}

func (m *Model) pushOverlay(o Overlay) { m.overlays = append(m.overlays, o) }

func (m *Model) popOverlay() {
	if len(m.overlays) > 0 {
		m.overlays = m.overlays[:len(m.overlays)-1]
	}
}

// forward passes a non-key message to the active tab and the top overlay.
//
// A response can arrive after its screen lost focus, so the tab always gets it
// regardless of what is on top.
func (m *Model) forward(message tea.Msg) tea.Cmd {
	cmds := []tea.Cmd{m.tabs[m.active].Update(message)}

	// The Clusters tab tracks test results even while another tab is on screen.
	if m.active != clustersTabIndex {
		cmds = append(cmds, m.clustersTab.Update(message))
	}
	if len(m.overlays) > 0 {
		cmds = append(cmds, m.overlays[len(m.overlays)-1].Update(message))
	}
	return tea.Batch(cmds...)
}

func (m *Model) View() string {
	if !m.ready {
		return "starting…"
	}

	k := ktx.New(m.opts.Config, m.theme, m.width, m.height)
	if k.TooSmall() {
		return m.tooSmall(k)
	}

	titles := make([]string, len(m.tabs))
	for i, t := range m.tabs {
		titles[i] = t.Title()
	}

	tabBar := chrome.TabBar(m.theme, titles, m.active, m.width)
	k.HeightUsed(tabBarHeight)

	contextLine := m.contextLine()
	k.HeightUsed(contextLineHeight)

	statusBar := chrome.StatusBar(m.theme, m.shortcuts(), m.width)
	k.HeightUsed(statusBarHeight)

	body := m.tabs[m.active].View(k.WithHeight(m.height - chromeHeight))

	frame := lipgloss.JoinVertical(lipgloss.Left, tabBar, contextLine, body, statusBar)

	for _, o := range m.overlays {
		frame = modal.Overlay(frame, m.overlayView(o, k))
	}
	return frame
}

func (m *Model) contextLine() string {
	crumbs := []string{m.tabs[m.active].Title()}
	// A tab may be showing a view of its own, such as record details.
	if nested, ok := m.tabs[m.active].(interface{ Breadcrumb() []string }); ok {
		crumbs = append(crumbs, nested.Breadcrumb()...)
	}
	for _, o := range m.overlays {
		crumbs = append(crumbs, o.Name())
	}

	render := func(width int) string {
		return chrome.ContextLine(m.theme,
			m.opts.Cluster.Name, m.opts.Cluster.Color,
			crumbs, m.conn, len(m.info.Brokers), width)
	}

	if m.notification.Expired() {
		return render(m.width)
	}

	// A live notification takes the right-hand side of the line. It is truncated
	// rather than dropped: a diagnosis the user cannot see is worse than a
	// shortened one, and the full text is on the Clusters tab.
	note := m.notification.Render(m.theme)
	available := m.width / 2
	if lipgloss.Width(note) > available {
		note = ansi.Truncate(note, available, "…")
	}
	return render(m.width-lipgloss.Width(note)-2) + " " + note + " "
}

// shortcuts collects what the current screen supports, plus the global keys
// that always apply.
func (m *Model) shortcuts() []chrome.Shortcut {
	var out []chrome.Shortcut

	if len(m.overlays) > 0 {
		out = append(out, m.overlays[len(m.overlays)-1].Shortcuts()...)
		out = append(out,
			chrome.Shortcut{Key: "F1", Label: "Help"},
			chrome.Shortcut{Key: "Esc", Label: "Close"})
		return out
	}

	out = append(out, m.tabs[m.active].Shortcuts()...)
	out = append(out,
		chrome.Shortcut{Key: "Ctrl+K", Label: "Cluster"},
		chrome.Shortcut{Key: "F1", Label: "Help"},
		chrome.Shortcut{Key: "q", Label: "Quit"},
	)
	return out
}

func (m *Model) overlayView(o Overlay, k *ktx.Context) string {
	width := min(m.width-8, 100)
	height := m.height - 6

	inner := o.View(k.WithHeight(height - 4))
	return m.theme.ModalBorder.Width(width).Render(
		lipgloss.JoinVertical(lipgloss.Left, inner))
}

func (m *Model) tooSmall(k *ktx.Context) string {
	message := lipgloss.JoinVertical(lipgloss.Center,
		m.theme.Warning.Render("terminal too small"),
		m.theme.Dim.Render(fmt.Sprintf("kafman needs at least %d×%d, this is %d×%d",
			ktx.MinWidth, ktx.MinHeight, k.WindowWidth, k.WindowHeight)),
	)
	return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, message)
}
